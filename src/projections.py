"""读侧投影。

两个视图，同一事件事实，裁剪不同：
- public_view：面向游客与社区的公众页。只呈现“现在能不能去/要等多久/
  对社区有什么影响”，不含任何个人行程、预约明细、值班部署与可信度原始数据；
- duty_view：面向值班人员。按“下一处将要失守”排序，给出指标、口径、
  可信度、预计失守时间、对应协同链路与当前处置状态。
"""

from __future__ import annotations

from .model import CapacityBoard, Pressure, PressureAnalyzer, ForecastFactors

DIMENSION_LABELS = {
    "scenic_area_entry": "景区入口",
    "transport_shuttle": "接驳车",
    "accommodation": "住宿",
    "tax_refund_shop": "退税/外卡受理",
    "medical_point": "医疗点",
    "community_road": "社区道路",
}

# 公众页只显示分档文案，不显示精确余量与内部阈值
def _queue_band(minutes: float | None) -> str:
    if minutes is None:
        return "暂无数据"
    if minutes >= 40:
        return "排队 40 分钟以上"
    if minutes >= 25:
        return "排队约 25–40 分钟"
    if minutes >= 10:
        return "排队约 10–25 分钟"
    return "等待时间较短"


def _occupancy_band(rate: float | None) -> str:
    if rate is None:
        return "暂无数据"
    if rate >= 95:
        return "即将满员"
    if rate >= 80:
        return "较为拥挤"
    if rate >= 40:
        return "顺畅"
    return "宽松"


class PublicProjection:
    def __init__(self, board: CapacityBoard) -> None:
        self.board = board

    def build(self, moment: str, active_traces: list[dict] | None = None) -> dict:
        accessibility = []
        for state in self.board.sources.values():
            entry: dict = {
                "dimension": state.dimension,
                "name": state.label or state.source_id,
                "status": "open",
                "notes": [],
            }
            queue = state.readings.get("queue_length_minutes") or state.readings.get("wait_time_minutes")
            occ = state.readings.get("occupancy_rate")
            tickets = state.readings.get("tickets_available")
            sat = state.readings.get("infrastructure_saturation")
            impact = state.readings.get("resident_road_impact_level")

            if state.dimension != "community_road":
                if queue is not None:
                    entry["notes"].append(_queue_band(queue.value))
                if occ is not None:
                    entry["notes"].append(_occupancy_band(occ.value))
                if tickets is not None:
                    # 公众页只给有无，不给精确余票，避免与实际到场体验脱节
                    entry["tickets"] = "sold_out" if tickets.value <= 0 else "available"
                if sat is not None and state.dimension == "tax_refund_shop":
                    entry["notes"].append(
                        "外卡受理繁忙，建议预留时间" if sat.value >= 0.75 else "外卡受理正常"
                    )
            accessibility.append(entry)

        # 已确认、生效中的动作改变公众可见状态（提案不改变状态）
        for trace in active_traces or []:
            for action in trace.get("active_actions", []):
                kind = action["action"]
                for entry in accessibility:
                    if any(scope in (entry["name"],) or scope == self._scope_of(entry) for scope in action["scope_ids"]):
                        if kind in ("hold_entry", "limit_ticketing"):
                            entry["status"] = "entry_held"
                            entry["notes"].append("当前时段采取限流，已预约游客照常入园，建议改约稍后时段")
                        elif kind == "add_shuttle_capacity":
                            entry["notes"].append("已加开接驳车")
                        elif kind == "divert_traffic_control":
                            if entry["dimension"] == "community_road":
                                entry["notes"].append("周边道路临时分流，居民通道保持通行，社会车辆请按引导绕行")
                            else:
                                entry["notes"].append("周边车流正在分流，请听从现场引导")
                        elif kind == "communicate_advisory":
                            entry["notes"].append("请听从现场引导")

        community = self._community_view(moment)
        return {
            "as_of": moment,
            "headline": self._headline(accessibility, community),
            "accessibility": accessibility,
            "community_pressure": community,
            "privacy_note": "本页仅展示分档状态，不含任何个人行程信息",
        }

    def _scope_of(self, entry: dict) -> str:
        for state in self.board.sources.values():
            if (state.label or state.source_id) == entry["name"]:
                return state.source_id
        return entry["name"]

    def _community_view(self, moment: str) -> list[dict]:
        items = []
        for state in self.board.sources.values():
            impact = state.readings.get("resident_road_impact_level")
            if impact is None:
                continue
            level = int(impact.value)
            items.append(
                {
                    "source_id": state.source_id,
                    "name": state.label or state.source_id,
                    "impact_level": level,
                    "impact_text": ["暂无影响", "轻度影响", "明显影响，建议绕行", "严重影响，已启动居民通道保障"][level],
                }
            )
        return items

    @staticmethod
    def _headline(accessibility: list[dict], community: list[dict]) -> str:
        held = [e["name"] for e in accessibility if e["status"] == "entry_held"]
        worst_community = max((i["impact_level"] for i in community), default=0)
        parts = []
        if held:
            parts.append(f"{'、'.join(held)} 正在限流，已预约不受影响")
        if worst_community >= 2:
            parts.append("部分周边道路对居民出行影响明显，社会车辆请按引导绕行")
        if not parts:
            parts.append("各环节总体有序，请留意现场排队提示")
        return "；".join(parts)


class DutyProjection:
    def __init__(self, board: CapacityBoard, store=None) -> None:
        self.board = board
        self.store = store

    def build(
        self,
        moment: str,
        *,
        forecast: ForecastFactors | None = None,
        correlation_index: dict[str, str] | None = None,
    ) -> dict:
        """correlation_index: source_id:metric -> correlation_id，标注该瓶颈是否已在处置链路上。"""
        analyzer = PressureAnalyzer(self.board)
        pressures = analyzer.next_to_fail(moment, forecast=forecast)
        watchlist = [self._item(p, moment, correlation_index or {}) for p in pressures]
        return {
            "as_of": moment,
            "watchlist": watchlist,
            "forecast": None
            if forecast is None
            else {
                "group_arrivals": forecast.group_arrivals,
                "independent_arrivals": forecast.independent_arrivals,
                "visa_free": forecast.visa_policy_notes,
                "confidence": forecast.confidence,
            },
            "summary": self._summary(watchlist),
        }

    def _item(self, p: Pressure, moment: str, correlation_index: dict) -> dict:
        reading = self.board.latest(p.source_id, p.metric)
        key = f"{p.source_id}:{p.metric}"
        item = {
            "dimension": DIMENSION_LABELS.get(p.dimension, p.dimension),
            "source_id": p.source_id,
            "name": p.label or p.source_id,
            "metric": p.metric,
            "severity": p.severity,
            "observed": p.observed,
            "warning_at": p.threshold_warning,
            "critical_at": p.threshold_critical,
            "minutes_to_breach": p.minutes_to_breach,
            "confidence": p.confidence,
            "basis": reading.basis if reading else None,
            "window": p.window,
            "stale": p.stale,
            "coordination_correlation_id": correlation_index.get(key),
            "in_treatment": key in correlation_index,
        }
        if p.minutes_to_breach == 0:
            item["alert"] = "已经失守/越限"
        elif p.minutes_to_breach is not None:
            item["alert"] = f"预计 {p.minutes_to_breach:.0f} 分钟后越限"
        else:
            item["alert"] = "趋势数据不足，需人工核对"
        if p.confidence in ("low", "unverified"):
            item["alert"] += "（信号可信度低，先核源）"
        elif p.confidence == "stale":
            item["alert"] += "（读数陈旧，等待补报/现场确认）"
        return item

    @staticmethod
    def _summary(watchlist: list[dict]) -> str:
        breached = [w for w in watchlist if w["minutes_to_breach"] == 0]
        upcoming = [w for w in watchlist if w["minutes_to_breach"] not in (0, None)]
        parts = []
        if breached:
            parts.append("已越限：" + "、".join(f"{w['name']}({w['metric']})" for w in breached))
        if upcoming:
            soonest = upcoming[0]
            parts.append(
                f"下一处可能失守：{soonest['name']} {soonest['metric']}，约 {soonest['minutes_to_breach']:.0f} 分钟"
            )
        untreated = [w for w in watchlist if not w["in_treatment"] and w["severity"] in ("warning", "critical")]
        if untreated:
            parts.append(f"尚有 {len(untreated)} 个告警环节未进入处置链路")
        return "；".join(parts) or "各环节均在阈值内"
