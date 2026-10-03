"""隐私与最小共享。

约束：
- 匿名位置只允许是分段网格（grid cell），语言偏好只服务当次保障；
  两者只存在于内存中的当次会话，到期清除，不进入事件日志；
- 事件负载中出现个人标识、证件、联系方式、精确坐标、个人行程一律视为违规；
- 合作机构只能拿到完成调整所需的字段：调度公司拿线路/时段，
  医疗点拿增援请求，社区拿交通影响信息，互不可见对方部分。
"""

from __future__ import annotations

from dataclasses import dataclass

from .timeutil import minutes_between

# 事件负载任何层级都不得出现的个人数据键
FORBIDDEN_KEYS = {
    "name",
    "full_name",
    "passport_no",
    "id_number",
    "phone",
    "mobile",
    "email",
    "itinerary",
    "personal_itinerary",
    "booking_ref",
    "ticket_holder",
    "lat",
    "lon",
    "latitude",
    "longitude",
    "exact_location",
}

# 允许出现的位置类数据只有“匿名网格”这一个类别名
ALLOWED_LOCATION_CATEGORY = "anonymous_grid_location"

# 人数低于该阈值的语言/网格分组不对外输出，避免小样本反识别
K_ANONYMITY = 5


class PrivacyViolation(ValueError):
    pass


def scan_event_for_personal_data(record: dict, *, _path: str = "event") -> list[str]:
    """递归检查事件，返回所有疑似个人数据路径。"""
    violations: list[str] = []
    if isinstance(record, dict):
        for key, value in record.items():
            path = f"{_path}.{key}"
            if key in FORBIDDEN_KEYS:
                violations.append(f"{path} 包含个人数据键 {key}")
            violations.extend(scan_event_for_personal_data(value, _path=path))
    elif isinstance(record, list):
        for i, value in enumerate(record):
            violations.extend(scan_event_for_personal_data(value, _path=f"{_path}[{i}]"))
    elif isinstance(record, str):
        # 精确坐标经纬度对
        if _path.endswith(".lat") or _path.endswith(".lon"):
            violations.append(f"{_path} 疑似精确坐标")
    return violations


def assert_clean_event(record: dict) -> None:
    violations = scan_event_for_personal_data(record)
    if violations:
        raise PrivacyViolation("；".join(violations))


@dataclass(frozen=True)
class _Signal:
    grid: str
    language: str
    at: str


class EphemeralVisitorSignals:
    """匿名网格位置 + 语言偏好的当次会话缓存。

    与事件日志严格隔离：协调结束（或超过 ttl_minutes）即清除，
    下游只能取到聚合后的分组占比，且分组人数低于 K_ANONYMITY 不输出。
    """

    def __init__(self, *, ttl_minutes: float = 120.0) -> None:
        self.ttl_minutes = ttl_minutes
        self._signals: list[_Signal] = []
        self.purpose = "当次承载保障"

    def add(self, *, grid: str, language: str, at: str) -> None:
        if not grid.startswith("grid:"):
            raise ValueError("位置必须是分段网格标识（grid: 前缀），拒绝精确位置")
        self._signals.append(_Signal(grid=grid, language=language, at=at))

    def purge_expired(self, moment: str) -> int:
        before = len(self._signals)
        self._signals = [
            s for s in self._signals
            if minutes_between(s.at, moment) <= self.ttl_minutes
        ]
        return before - len(self._signals)

    def language_needs(self, moment: str) -> dict[str, float]:
        """各语种服务需求占比（百分比）；小样本分组并入 other 不单独显示。"""
        self.purge_expired(moment)
        counts: dict[str, int] = {}
        for signal in self._signals:
            counts[signal.language] = counts.get(signal.language, 0) + 1
        total = len(self._signals)
        if total == 0:
            return {}
        result: dict[str, float] = {}
        pooled = 0
        for language, count in counts.items():
            if count < K_ANONYMITY:
                pooled += count
            else:
                result[language] = round(count * 100.0 / total, 1)
        if pooled >= K_ANONYMITY:
            result["other"] = round(pooled * 100.0 / total, 1)
        return result

    def grid_pressure(self, moment: str) -> dict[str, int]:
        """各网格的匿名人数；低样本网格不输出。"""
        self.purge_expired(moment)
        counts: dict[str, int] = {}
        for signal in self._signals:
            counts[signal.grid] = counts.get(signal.grid, 0) + 1
        return {grid: count for grid, count in counts.items() if count >= K_ANONYMITY}

    def clear(self) -> None:
        self._signals.clear()


# ---------------------------------------------------------------------------
# 合作机构最小信息包
# ---------------------------------------------------------------------------

# 每类合作机构完成其调整所需的字段白名单（其余一律不下发）
PARTNER_FIELD_ACCESS = {
    "shuttle_operator": {
        "needs": ["bottleneck", "time_slots", "routes", "resources"],
        "dimensions": ["transport_shuttle"],
        "never": ["language_needs", "reservation_detail", "medical", "community_detail"],
    },
    "scenic_area": {
        "needs": ["bottleneck", "time_slots", "reservation_policy"],
        "dimensions": ["scenic_area_entry"],
        "never": ["language_needs", "medical"],
    },
    "tax_refund_shop": {
        "needs": ["bottleneck", "time_slots", "resources"],
        "dimensions": ["tax_refund_shop"],
        "never": ["language_needs_raw", "reservation_detail"],
    },
    "medical_point": {
        "needs": ["bottleneck", "resources", "language_needs_band"],
        "dimensions": ["medical_point"],
        "never": ["reservation_detail", "itinerary"],
    },
    "community_office": {
        "needs": ["community_pressure", "routes", "action_window"],
        "dimensions": ["community_road"],
        "never": ["visitor_counts_by_language", "reservation_detail", "ticketing"],
    },
    "traffic_police": {
        "needs": ["community_pressure", "routes", "action_window", "bottleneck"],
        "dimensions": ["community_road", "transport_shuttle"],
        "never": ["language_needs", "reservation_detail"],
    },
}


def build_partner_packet(
    partner_role: str,
    *,
    trace_view: dict,
    public_community: list[dict],
    language_needs: dict[str, float] | None = None,
) -> dict:
    """从全链路视图中为某合作机构裁剪最小信息包。"""
    access = PARTNER_FIELD_ACCESS.get(partner_role)
    if access is None:
        raise ValueError(f"未知合作机构类型：{partner_role}")

    allowed_dimensions = set(access["dimensions"])
    signals_all = trace_view["stages"]["trigger_signals"]
    signals = [s for s in signals_all if s["dimension"] in allowed_dimensions]
    proposals = trace_view["stages"]["proposals"]
    alternatives_all = proposals[-1]["alternatives"] if proposals else {"time_slots": [], "routes": [], "resources": []}
    # 只下发与该机构维度相关的替代项
    def _relevant(items: list[dict]) -> list[dict]:
        return [
            a for a in items
            if allowed_dimensions & set(a.get("applies_to_dimensions", []))
        ]
    alternatives = {
        "time_slots": _relevant(alternatives_all.get("time_slots", [])),
        "routes": _relevant(alternatives_all.get("routes", [])),
        "resources": _relevant(alternatives_all.get("resources", [])),
    }
    active = trace_view.get("active_actions", [])

    packet: dict = {"partner_role": partner_role, "correlation_id": trace_view["correlation_id"]}
    needs = access["needs"]

    if "bottleneck" in needs:
        packet["bottlenecks"] = [
            {
                "dimension": s["dimension"],
                "source_id": s["source_id"],
                "severity": s["severity"],
                "observed_band": _band(s["metric"], s["observed_value"]),
            }
            for s in signals
        ]
    if "time_slots" in needs:
        packet["suggested_time_slots"] = [a["window"] for a in alternatives["time_slots"]]
    if "routes" in needs:
        packet["suggested_routes"] = [
            {"route_id": a["route_id"], "label": a.get("label", "")}
            for a in alternatives["routes"]
        ]
    if "resources" in needs:
        packet["resource_requests"] = [
            {"kind": a["resource"]["kind"], "target_id": a["resource"]["target_id"], "label": a["resource"].get("label")}
            for a in alternatives["resources"]
        ]
    if "reservation_policy" in needs:
        # 只有数量与处理原则，没有任何预约明细
        packet["reservation_policy"] = proposals[-1].get("reservation_policy", []) if proposals else []
    if "language_needs_band" in needs:
        # 医疗点只需知道“需要哪些语种多语急救”，给占比档位而非原始数据
        needs_map = language_needs or {}
        packet["language_service_bands"] = {
            lang: ("high" if pct >= 25 else "medium" if pct >= 10 else "low")
            for lang, pct in needs_map.items()
        }
    if "community_pressure" in needs:
        packet["community_pressure"] = [
            {"source_id": item["source_id"], "level": item["impact_level"], "label": item.get("label")}
            for item in public_community
        ]
    if "action_window" in needs and active:
        relevant_scopes = {s["source_id"] for s in signals}
        relevant_action = next(
            (a for a in active if set(a["scope_ids"]) & relevant_scopes),
            active[0],
        )
        packet["action_window"] = relevant_action["window"]
    return packet


def _band(metric: str, value: float | None) -> str:
    if value is None:
        return "unknown"
    if metric.endswith("rate") or metric.endswith("saturation"):
        return "≥95%" if value >= 95 or (value <= 1.5 and value >= 0.9) else "80-95%" if value >= 80 or value >= 0.75 else "<80%"
    if metric in ("queue_length_minutes", "wait_time_minutes"):
        return "≥30分钟" if value >= 30 else "15-30分钟" if value >= 15 else "<15分钟"
    return str(value)
