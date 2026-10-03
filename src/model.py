"""容量看板、压力分析与预测。

设计原则（对应领域要求）：
- 可售数量、实时占用、安全余量、服务降级、居民影响是五条互不相混的通道，
  看板拒绝把它们折成一个“游客总量”；
- 每个读数自带统计口径 basis 与可信度 confidence，值班视图据此区分
  “闸门计数”与“警方口头反馈”；
- 预测因子（大型团队 / 散客 / 免签政策）只影响信号与方案，
  不触碰既有预约；
- 数据可能迟到（断网补报），看板由按 occurred_at 重放的事件流构建，
  并保留读数的业务时间窗，用 staleness 表达“此刻是否还新鲜”。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .eventstore import StoredEvent
from .timeutil import minutes_between, parse_ts

# 五条互不相混的观察通道
CHANNELS = {
    "sellable": {"tickets_available", "tickets_sold_booked"},
    "occupancy": {"occupancy_rate", "queue_length_minutes", "wait_time_minutes", "infrastructure_saturation", "forecast_arrivals"},
    "safety": {"safety_headcount_limit", "safety_margin", "staff_on_duty_stations", "staff_idle_ratio"},
    "degradation": {"service_degradation_level"},
    "community": {"resident_road_impact_level"},
}

CONFIDENCE_RANK = {"confirmed": 5, "high": 4, "medium": 3, "low": 2, "stale": 1, "unverified": 0}


def channel_of(metric: str) -> str:
    for channel, metrics in CHANNELS.items():
        if metric in metrics:
            return channel
    return "other"


@dataclass(frozen=True)
class Reading:
    metric: str
    value: float
    unit: str | None
    basis: str | None
    confidence: str
    confidence_reason: str | None
    window_start: str
    window_end: str
    occurred_at: str
    event_id: str
    source_ingest: str

    def is_stale_at(self, moment: str, *, max_age_minutes: float = 20.0) -> bool:
        """读数时间窗终点距 moment 是否已经超出保鲜期。"""
        return minutes_between(self.window_end, moment) > max_age_minutes


@dataclass
class SourceState:
    dimension: str
    source_id: str
    label: str | None
    readings: dict[str, Reading] = field(default_factory=dict)
    history: dict[str, list[Reading]] = field(default_factory=dict)

    def latest(self, metric: str) -> Reading | None:
        return self.readings.get(metric)


class CapacityBoard:
    """CAPACITY_REPORTED 的读模型：每个来源 × 指标保留最新读数与历史序列。"""

    def __init__(self, events: list[StoredEvent] | None = None) -> None:
        self.sources: dict[str, SourceState] = {}
        if events:
            for event in events:
                self.apply(event)

    def apply(self, event: StoredEvent) -> None:
        if event.event_type != "CAPACITY_REPORTED":
            return
        payload = event.record["payload"]
        scope = payload["scope"]
        source_id = scope["source_id"]
        state = self.sources.get(source_id)
        if state is None:
            state = SourceState(scope["dimension"], source_id, scope.get("label"))
            self.sources[source_id] = state
        window = payload["window"]
        ingest = event.record.get("source", {}).get("ingest", "live")
        for raw in payload["readings"]:
            reading = Reading(
                metric=raw["metric"],
                value=float(raw["value"]),
                unit=raw.get("unit"),
                basis=raw.get("basis"),
                confidence=raw.get("confidence", "unverified"),
                confidence_reason=raw.get("confidence_reason"),
                window_start=window["start"],
                window_end=window["end"],
                occurred_at=event.occurred_at,
                event_id=event.event_id,
                source_ingest=ingest,
            )
            current = state.readings.get(reading.metric)
            if current is None or parse_ts(reading.window_end) >= parse_ts(current.window_end):
                state.readings[reading.metric] = reading
            state.history.setdefault(reading.metric, []).append(reading)
            state.history[reading.metric].sort(key=lambda r: parse_ts(r.window_end))

    def latest(self, source_id: str, metric: str) -> Reading | None:
        state = self.sources.get(source_id)
        return state.readings.get(metric) if state else None

    def channel_view(self, channel: str) -> dict[str, Reading]:
        """某条通道下各来源的最新读数，例如只看居民影响。"""
        metrics = CHANNELS[channel]
        view: dict[str, Reading] = {}
        for source_id, state in self.sources.items():
            for metric in metrics:
                reading = state.readings.get(metric)
                if reading is not None:
                    view[f"{source_id}.{metric}"] = reading
        return view


# ---------------------------------------------------------------------------
# 阈值策略与压力分析
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Threshold:
    warning: float
    critical: float
    # high_is_bad=False 时（如 safety_margin、可用车位）数值越低越糟
    high_is_bad: bool = True


DEFAULT_THRESHOLDS: dict[tuple[str, str], Threshold] = {
    ("scenic_area_entry", "occupancy_rate"): Threshold(80, 95),
    ("scenic_area_entry", "safety_margin"): Threshold(15, 5, high_is_bad=False),
    ("transport_shuttle", "queue_length_minutes"): Threshold(25, 40),
    ("transport_shuttle", "occupancy_rate"): Threshold(85, 98),
    ("transport_shuttle", "wait_time_minutes"): Threshold(20, 35),
    ("tax_refund_shop", "infrastructure_saturation"): Threshold(0.75, 0.92),
    ("tax_refund_shop", "wait_time_minutes"): Threshold(15, 30),
    ("medical_point", "wait_time_minutes"): Threshold(10, 20),
    ("medical_point", "staff_idle_ratio"): Threshold(0.4, 0.15, high_is_bad=False),
    ("accommodation", "occupancy_rate"): Threshold(90, 98),
    ("community_road", "resident_road_impact_level"): Threshold(1, 2),
}


@dataclass
class Pressure:
    dimension: str
    source_id: str
    label: str | None
    metric: str
    severity: str
    observed: float
    threshold_warning: float
    threshold_critical: float
    window: dict
    minutes_to_breach: float | None
    confidence: str
    based_on: list[str]
    stale: bool
    message: str

    @property
    def breaching(self) -> bool:
        return self.minutes_to_breach == 0


def _severity_for(value: float, threshold: Threshold) -> str | None:
    if threshold.high_is_bad:
        if value >= threshold.critical:
            return "critical"
        if value >= threshold.warning:
            return "warning"
    else:
        if value <= threshold.critical:
            return "critical"
        if value <= threshold.warning:
            return "warning"
    return None


def _minutes_to_breach(history: list[Reading], threshold: Threshold, moment: str) -> float | None:
    """按最近两个读数线性外推到 warning 阈值的时间；已越过阈值记 0。"""
    series = [r for r in history if parse_ts(r.window_end) <= parse_ts(moment)]
    if len(series) < 2:
        return None
    prev, last = series[-2], series[-1]
    elapsed = minutes_between(prev.window_end, last.window_end)
    if elapsed <= 0:
        return None
    slope = (last.value - prev.value) / elapsed  # 每分钟变化
    value = last.value
    if threshold.high_is_bad:
        if value >= threshold.warning:
            return 0.0
        if slope <= 0:
            return None
        return max(0.0, (threshold.warning - value) / slope)
    if value <= threshold.warning:
        return 0.0
    if slope >= 0:
        return None
    return max(0.0, (value - threshold.warning) / (-slope))


@dataclass
class ForecastFactors:
    """大型团队、散客与免签政策变化；仅用于信号研判。"""

    group_arrivals: float = 0.0
    independent_arrivals: float = 0.0
    visa_policy_notes: str = ""
    confidence: str = "medium"

    def total(self) -> float:
        return self.group_arrivals + self.independent_arrivals


class PressureAnalyzer:
    def __init__(
        self,
        board: CapacityBoard,
        thresholds: dict[tuple[str, str], Threshold] | None = None,
        *,
        approaching_window_minutes: float = 30.0,
    ) -> None:
        self.board = board
        self.thresholds = thresholds or DEFAULT_THRESHOLDS
        self.approaching_window_minutes = approaching_window_minutes

    def analyze(self, moment: str, *, forecast: ForecastFactors | None = None) -> list[Pressure]:
        pressures: list[Pressure] = []
        for state in self.board.sources.values():
            for metric, reading in state.readings.items():
                threshold = self.thresholds.get((state.dimension, metric))
                if threshold is None:
                    continue
                # 读数在 occurred_at 已是现场已知事实即纳入（覆盖窗口可延伸到未来）；
                # 是否陈旧由读数窗口终点与保鲜期另行判断。
                if parse_ts(reading.occurred_at) > parse_ts(moment):
                    continue
                severity = _severity_for(reading.value, threshold)
                mtb = _minutes_to_breach(state.history[metric], threshold, moment)
                # 尚未越限，但趋势显示将在观察窗内越限 → watch（“下一处将要失守”）。
                if severity is None and mtb is not None and mtb <= self.approaching_window_minutes:
                    severity = "watch"
                if severity is None:
                    continue
                # 已越过 warning 阈值即视为失守；历史不足以外推时失守时间记 0。
                if severity in ("warning", "critical") and mtb is None:
                    mtb = 0.0
                stale = reading.is_stale_at(moment)
                confidence = "stale" if stale else reading.confidence
                pressures.append(
                    Pressure(
                        dimension=state.dimension,
                        source_id=state.source_id,
                        label=state.label,
                        metric=metric,
                        severity=severity,
                        observed=reading.value,
                        threshold_warning=threshold.warning,
                        threshold_critical=threshold.critical,
                        window={"start": reading.window_start, "end": reading.window_end},
                        minutes_to_breach=mtb,
                        confidence=confidence,
                        based_on=[reading.event_id],
                        stale=stale,
                        message=self._describe(state, reading, severity),
                    )
                )
        pressures = self._merge_pressure(pressures)
        if forecast is not None and forecast.total() > 0:
            self._annotate_forecast(pressures, forecast)
        return pressures

    def next_to_fail(self, moment: str, *, forecast: ForecastFactors | None = None) -> list[Pressure]:
        """值班视角：下一处将要失守的环节排序。

        已失守(critical)在最前；其余按预计失守分钟数升序；
        无法估计的排到同级别末尾；低可信/陈旧信号打标但不隐藏。
        """
        severity_rank = {"critical": 0, "warning": 1, "watch": 2}
        pressures = self.analyze(moment, forecast=forecast)
        return sorted(
            pressures,
            key=lambda p: (
                severity_rank[p.severity],
                p.minutes_to_breach if p.minutes_to_breach is not None else float("inf"),
                -CONFIDENCE_RANK[p.confidence],
                p.source_id,
            ),
        )

    @staticmethod
    def _describe(state: SourceState, reading: Reading, severity: str) -> str:
        basis = f"（口径：{reading.basis}）" if reading.basis else ""
        return f"{state.label or state.source_id} {reading.metric}={reading.value}{basis} → {severity}"

    @staticmethod
    def _merge_pressure(pressures: list[Pressure]) -> list[Pressure]:
        """同一来源多个指标告警时保留全部（局部瓶颈不可互相掩盖）。"""
        return pressures

    @staticmethod
    def _annotate_forecast(pressures: list[Pressure], forecast: ForecastFactors) -> None:
        if not pressures:
            return
        note = f"预测未来时段团队{forecast.group_arrivals:.0f}+散客{forecast.independent_arrivals:.0f}人次"
        if forecast.visa_policy_notes:
            note += f"；免签政策：{forecast.visa_policy_notes}"
        for p in pressures:
            p.message += f"｜{note}"
