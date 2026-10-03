"""承载监测投影与值班预警。

各机构（景区/交通/住宿/退税商店/医疗点/社区）各自维护自己的 capacity_source，
投影时把五类信号分开呈现：可售数量、实时占用、安全余量、服务降级、居民影响——
不再用‘游客总量’掩盖局部瓶颈。

预警分两种：
- 已失守：安全余量 <= 0 或服务严重降级/居民出行受阻；
- 将失守：对同一来源的历史观测按事件时间做线性外推，估算安全余量归零的分钟数。
  售票页仍有余票时，接驳车/外卡/急救等局部环节也能在这里被提前看到。
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .event_store import EventStore, StoredEvent, parse_dt

# 外推时最多采用的观测点数与默认预警视野
_DEFAULT_HORIZON_MINUTES = 30
_MAX_SAMPLES = 8
# 超过粒度的多少倍没有新数据即视为陈旧
_STALE_FACTOR = 3


@dataclass
class CapacityState:
    aggregate_id: str
    owner: str
    location_id: str | None
    metric_code: str
    unit: str
    basis: str
    window: dict
    sellable_quantity: float | None = None
    realtime_occupancy: float | None = None
    safety_headroom: float | None = None
    service_level: str = "normal"
    service_detail: str | None = None
    community_level: str = "none"
    community_detail: str | None = None
    confidence_level: str = "medium"
    driver_factors: dict | None = None
    observed_at: datetime | None = None
    # 同一来源的数值时间序列：(观测时刻, metric_code, {字段: 值})，供外推与恢复核对
    series: list[tuple[datetime, str, dict]] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.owner}/{self.location_id or self.aggregate_id}·{self.metric_code}"

    def field_series(self, field_name: str, metric_code: str | None = None) -> list[tuple[datetime, float]]:
        out = []
        for ts, code, values in self.series:
            if metric_code is not None and code != metric_code:
                continue
            value = values.get(field_name)
            if isinstance(value, (int, float)):
                out.append((ts, float(value)))
        return out

    def latest(self, field_name: str, metric_code: str | None = None) -> tuple[datetime, float] | None:
        points = self.field_series(field_name, metric_code)
        return points[-1] if points else None


@dataclass
class ServiceStatus:
    state: CapacityState
    severity: str                      # critical / breach / watch / projected_watch / ok / stale
    eta_minutes: float | None          # 预计多少分钟后安全余量归零
    reasons: list[str]
    projected: bool = False

    @property
    def label(self) -> str:
        return self.state.label


def _as_float(value) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


class Projection:
    def __init__(self) -> None:
        self.capacities: dict[str, CapacityState] = {}
        self.pressures: list[dict] = []
        self.plans: list[dict] = []
        self.decisions: list[dict] = []

    @classmethod
    def build(cls, store: EventStore, as_of: datetime | None = None,
              known_at: datetime | None = None) -> "Projection":
        proj = cls()
        for stored in store.replay(as_of=as_of, known_at=known_at):
            proj.apply(stored)
        return proj

    def apply(self, stored: StoredEvent) -> None:
        event = stored.event
        etype = event["event_type"]
        if etype == "CAPACITY_REPORTED":
            self._apply_capacity(stored)
        elif etype == "PRESSURE_DETECTED":
            self.pressures.append(event)
        elif etype == "DIVERSION_PROPOSED":
            self.plans.append(event)
        elif etype in ("ACTION_CONFIRMED", "NORMAL_SERVICE_RESTORED"):
            self.decisions.append(event)

    def _apply_capacity(self, stored: StoredEvent) -> None:
        event = stored.event
        p = event["payload"]
        agg = event["aggregate_id"]
        metric = p["metric"]
        state = self.capacities.get(agg)
        if state is None:
            state = CapacityState(
                aggregate_id=agg,
                owner=p["owner"],
                location_id=p.get("location_id"),
                metric_code=metric["code"],
                unit=metric["unit"],
                basis=metric["basis"],
                window=p["window"],
            )
            self.capacities[agg] = state
        # 同一来源口径可能演进，以最新版本为准
        state.basis = metric["basis"]
        state.unit = metric["unit"]
        state.window = p["window"]
        state.sellable_quantity = _as_float(p.get("sellable_quantity"))
        state.realtime_occupancy = _as_float(p.get("realtime_occupancy"))
        state.safety_headroom = _as_float(p.get("safety_headroom"))
        deg = p.get("service_degradation") or {}
        state.service_level = deg.get("level", "normal")
        state.service_detail = deg.get("detail")
        comm = p.get("community_impact") or {}
        state.community_level = comm.get("level", "none")
        state.community_detail = comm.get("detail")
        conf = p.get("confidence") or {}
        state.confidence_level = conf.get("level", "medium")
        state.driver_factors = p.get("driver_factors")
        coll = p.get("collection") or {}
        state.observed_at = parse_dt(coll["observed_at"]) if coll.get("observed_at") else stored.occurred_at
        values = {
            name: p.get(name)
            for name in ("sellable_quantity", "realtime_occupancy", "safety_headroom")
            if isinstance(p.get(name), (int, float))
        }
        state.series.append((state.observed_at, metric["code"], values))
        state.series = state.series[-_MAX_SAMPLES:]


def _extrapolate_minutes_to_zero(series: list[tuple[datetime, float]]) -> float | None:
    """对 (时间, 安全余量) 做最小二乘线性外推，返回余量归零的分钟数。

    余量未呈下降趋势或样本不足时返回 None。
    """
    points = [(t, v) for t, v in series if v is not None]
    if len(points) < 2:
        return None
    t0 = points[0][0]
    xs = [(t - t0).total_seconds() / 60.0 for t, _ in points]
    ys = [v for _, v in points]
    n = len(points)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    if denom == 0:
        return None
    slope = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n)) / denom
    if slope >= 0:
        return None  # 没有恶化趋势
    intercept = mean_y - slope * mean_x
    eta = -intercept / slope - xs[-1]
    return eta if eta > 0 else 0.0


class DutyBoard:
    """值班看板：按‘最可能先失守’排序的局部服务环节清单。"""

    def __init__(self, projection: Projection, now: datetime, horizon_minutes: int = _DEFAULT_HORIZON_MINUTES):
        self.projection = projection
        self.now = now
        self.horizon_minutes = horizon_minutes

    def _staleness_minutes(self, state: CapacityState) -> float | None:
        gran = state.window.get("granularity_minutes")
        if not gran or state.observed_at is None:
            return None
        return (self.now - state.observed_at).total_seconds() / 60.0

    def evaluate(self, state: CapacityState) -> ServiceStatus:
        reasons: list[str] = []
        severity = "ok"

        if state.community_level == "disrupted":
            severity = "critical"
            reasons.append(f"居民出行受阻：{state.community_detail or '社区道路受影响'}")
        elif state.community_level == "noticeable" and severity == "ok":
            severity = "breach"
            reasons.append("社区已感到明显压力")

        if state.service_level == "severely_degraded":
            severity = "critical"
            reasons.append(f"服务严重降级：{state.service_detail or ''}".rstrip("："))
        elif state.service_level == "degraded" and severity in ("ok",):
            severity = "breach"
            reasons.append(f"服务降级：{state.service_detail or ''}".rstrip("："))

        if state.safety_headroom is not None and state.safety_headroom <= 0:
            severity = "critical"
            reasons.append(f"安全余量已耗尽（当前 {state.safety_headroom:g} {state.unit}）")

        # 趋势外推：余量仍为正，但正在快速下滑
        eta: float | None = None
        numeric_series = state.field_series("safety_headroom", metric_code=state.metric_code)
        if len(numeric_series) >= 2:
            eta = _extrapolate_minutes_to_zero(numeric_series)
        if (
            eta is not None
            and eta <= self.horizon_minutes
            and state.safety_headroom is not None
            and state.safety_headroom > 0
            and severity == "ok"
        ):
            severity = "projected_watch"
            reasons.append(
                f"趋势外推约 {eta:.0f} 分钟后安全余量归零（当前余量 {state.safety_headroom:g} {state.unit}，"
                f"口径可信度 {state.confidence_level}）"
            )

        # 陈旧数据：口径声称实时但已断更（典型为断网传感器尚未补传）
        stale = self._staleness_minutes(state)
        if stale is not None and stale > state.window.get("granularity_minutes", 1) * _STALE_FACTOR:
            if severity == "ok":
                severity = "stale"
            reasons.append(f"数据已 {stale:.0f} 分钟未更新，判断需谨慎")

        return ServiceStatus(state=state, severity=severity, eta_minutes=eta, reasons=reasons,
                             projected=severity == "projected_watch")

    def service_statuses(self) -> list[ServiceStatus]:
        statuses = [self.evaluate(s) for s in self.projection.capacities.values()]
        rank = {"critical": 0, "breach": 1, "projected_watch": 2, "stale": 3, "watch": 4, "ok": 5}
        return sorted(
            statuses,
            key=lambda s: (
                rank.get(s.severity, 9),
                s.eta_minutes if s.eta_minutes is not None else float("inf"),
                s.label,
            ),
        )

    def next_to_fail(self) -> ServiceStatus | None:
        """下一处将要失守的服务环节（已失守的 critical 排在其前）。"""
        for status in self.service_statuses():
            if status.severity in ("critical", "breach", "projected_watch"):
                return status
        return None

    def render_text(self) -> str:
        icons = {
            "critical": "🔴",
            "breach": "🟠",
            "projected_watch": "🟡",
            "stale": "⚪",
            "ok": "🟢",
        }
        lines = [f"值班承载看板 @ {self.now.isoformat()}", "=" * 46]
        for s in self.service_statuses():
            head = f"{icons.get(s.severity, '·')} [{s.severity}] {s.label}"
            lines.append(head)
            st = s.state
            lines.append(
                f"   可售={_fmt(st.sellable_quantity)} 占用={_fmt(st.realtime_occupancy)} "
                f"安全余量={_fmt(st.safety_headroom)} {st.unit}（{st.basis}/{st.confidence_level}）"
            )
            if st.service_level != "normal":
                lines.append(f"   服务降级({st.service_level}): {st.service_detail or ''}")
            if st.community_level != "none":
                lines.append(f"   居民影响({st.community_level}): {st.community_detail or ''}")
            for reason in s.reasons:
                lines.append(f"   ⚑ {reason}")
        return "\n".join(lines)


def _fmt(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:g}"
