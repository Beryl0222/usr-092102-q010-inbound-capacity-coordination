"""生成 data/scenario_zhangjiajie.json：2026-10-03 张家界西入口接驳限流全链条。

事件按‘现场发生时刻’书写；其中外卡受理终端在 12:05-12:39 断网，12:20 与 12:33
两条观测于 12:40 才补传到库（collection.transmission=backfill，ingested_at 为
补传时刻，occurred_at/observed_at 仍为观测时刻）。文件内事件顺序也刻意保留
补传错位，重放正确性由测试保证。

恢复事件不在本文件内：它必须由恢复条件驱动生成（见 src/coordination.RecoveryTracker
与 src/demo.py）。
"""

import json
from pathlib import Path

T = "2026-10-03T"
TZ = "+08:00"
CORR = "INC-20261003-WESTGATE-01"


def ts(h: str) -> str:
    return f"{T}{h}:00{TZ}"


def shift(tm: str, minutes: int) -> str:
    h, m = int(tm[:2]), int(tm[3:5])
    total = h * 60 + m + minutes
    return f"{total // 60:02d}:{total % 60:02d}"


def window(end_tm: str, granularity: int) -> dict:
    return {"starts_at": ts(shift(end_tm, -granularity)), "ends_at": ts(end_tm),
            "granularity_minutes": granularity}


def metric(code: str, unit: str, basis: str, rule: str = "") -> dict:
    m = {"code": code, "unit": unit, "basis": basis}
    if rule:
        m["counting_rule"] = rule
    return m


def coll(observed: str, *, transmission: str = "live", ingested: str | None = None,
         sensor: str | None = None) -> dict:
    c = {"observed_at": ts(observed), "transmission": transmission,
         "ingested_at": ts(ingested or observed)}
    if sensor:
        c["sensor_id"] = sensor
    return c


def sharing(visibility: str, partners: list[str] | None = None) -> dict:
    s = {"visibility": visibility, "purpose": "current_safeguard_only"}
    if partners:
        s["allowed_partners"] = partners
    return s


events: list[dict] = []


def add(**kw) -> dict:
    events.append(kw)
    return kw


# ---------- 景区：售票仍有余票，但核心区实时占用攀升（两个口径分开报） ----------
add(
    event_id="zjj-cap-ticket-01", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-tickets-main",
    occurred_at=ts("12:00"), version=1, summary="景区主门票分时可售数量上报",
    data_sharing=sharing("public"),
    payload={
        "owner": "scenic_area", "location_id": "gate-main",
        "window": window("12:30", 30),
        "metric": metric("tickets_sellable", "张", "measured", "当前分时剩余可售票，不含已预约未入园"),
        "sellable_quantity": 1200, "realtime_occupancy": None, "safety_headroom": None,
        "confidence": {"level": "high"},
        "driver_factors": {
            "large_groups": 3,
            "independent_travelers_estimate": 1400,
            "visa_policy_change": {"policy_id": "VISA-FREE-240H-EXT", "expected_delta_pct": 18,
                                   "effective_from": ts("00:00")},
        },
        "collection": coll("12:00", sensor="ticketing-cloud"),
    },
)
add(
    event_id="zjj-cap-onsite-01", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-onsite-core",
    occurred_at=ts("12:00"), version=1, summary="核心景区实时占用上报",
    data_sharing=sharing("partners", ["transport", "community"]),
    payload={
        "owner": "scenic_area", "location_id": "core-zone",
        "window": window("12:15", 15),
        "metric": metric("persons_on_site", "人", "measured", "闸机计数，含免签随团旅客，不含工作人员"),
        "sellable_quantity": None, "realtime_occupancy": 6100, "safety_headroom": 1900,
        "confidence": {"level": "high"}, "collection": coll("12:00", sensor="gate-counters"),
    },
)
add(
    event_id="zjj-cap-onsite-02", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-onsite-core",
    occurred_at=ts("12:30"), version=2, summary="核心景区实时占用继续上升",
    data_sharing=sharing("partners", ["transport", "community"]),
    payload={
        "owner": "scenic_area", "location_id": "core-zone",
        "window": window("12:30", 15),
        "metric": metric("persons_on_site", "人", "measured"),
        "sellable_quantity": None, "realtime_occupancy": 7300, "safety_headroom": 700,
        "confidence": {"level": "high"}, "collection": coll("12:30", sensor="gate-counters"),
    },
)
add(
    event_id="zjj-cap-ticket-02", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-tickets-main",
    occurred_at=ts("12:40"), version=2, summary="售票页仍显示大量余票",
    data_sharing=sharing("public"),
    payload={
        "owner": "scenic_area", "location_id": "gate-main",
        "window": window("13:00", 30),
        "metric": metric("tickets_sellable", "张", "measured"),
        "sellable_quantity": 860, "realtime_occupancy": None, "safety_headroom": None,
        "confidence": {"level": "high"}, "collection": coll("12:40", sensor="ticketing-cloud"),
    },
)

# ---------- 交通：接驳站长队（内部细粒度，partners） ----------
shuttle_series = [
    ("12:00", 1, 480, 220, "none", None),
    ("12:15", 2, 580, 120, "none", None),
    ("12:30", 3, 655, 45, "noticeable", "排队开始外溢至西入口社区路段"),
    ("12:40", 4, 708, -8, "disrupted", "接驳站排队外溢，影响附近居民道路出行"),
    ("12:50", 5, 640, 60, "disrupted", "限流启动后队伍仍在消化"),
    ("13:05", 6, 605, 95, "noticeable", "外溢缓解"),
    ("13:20", 7, 590, 110, "noticeable", "接近恢复阈值"),
    ("13:35", 8, 565, 135, "none", None),
    ("13:50", 9, 558, 142, "none", None),
]
for tm, ver, occ, head, clevel, cdetail in shuttle_series:
    p = {
        "owner": "transport", "location_id": "shuttle-west-gate",
        "window": window(tm, 10),
        "metric": metric("queue_slots_headroom", "人", "measured",
                         "接驳站排队容量余量=设计容量-在队人数"),
        "sellable_quantity": None, "realtime_occupancy": occ, "safety_headroom": head,
        "confidence": {"level": "high"},
        "collection": coll(tm, sensor="shuttle-cam-west"),
    }
    if clevel != "none":
        p["community_impact"] = {"level": clevel, "detail": cdetail}
    add(
        event_id=f"zjj-cap-shuttle-{ver:02d}", event_type="CAPACITY_REPORTED",
        aggregate_type="capacity_source", aggregate_id="cap-shuttle-west",
        occurred_at=ts(tm), version=ver,
        summary=f"西入口接驳站占用与安全余量 {tm}",
        data_sharing=sharing("partners", ["scenic_area", "community"]), payload=p,
    )

# 面向公众的粗粒度发布（不含精确数值，只给降级/恢复档位与文案）
public_shuttle_notes = [
    ("12:30", 1, "degraded", "西入口接驳排队较长，建议改走东门或延后前往"),
    ("12:50", 2, "severely_degraded", "西入口接驳已限流，新到游客请按引导前往东门线路B，已预约班次照发"),
    ("13:35", 3, "normal", "西入口接驳秩序恢复，可正常前往"),
]
for tm, ver, level, detail in public_shuttle_notes:
    pub = {
        "level": level, "detail": detail,
    } if level != "normal" else {"level": "normal"}
    add(
        event_id=f"zjj-cap-shuttle-pub-{ver:02d}", event_type="CAPACITY_REPORTED",
        aggregate_type="capacity_source", aggregate_id="cap-shuttle-west-pub",
        occurred_at=ts(tm), version=ver, summary=detail,
        data_sharing=sharing("public"),
        payload={
            "owner": "transport", "location_id": "shuttle-west-gate",
            "window": window(tm, 10),
            "metric": metric("queue_status_band", "档", "estimated"),
            "sellable_quantity": None, "realtime_occupancy": None, "safety_headroom": None,
            "service_degradation": pub,
            "confidence": {"level": "medium"},
            "collection": coll(tm, sensor="ops-publisher"),
        },
    )

# ---------- 退税商店：外卡受理终端断网，补传两条观测 ----------
add(
    event_id="zjj-cap-card-01", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-card-terminal-07",
    occurred_at=ts("12:00"), version=1, summary="外卡受理终端占用 60%",
    payload={
        "owner": "tax_refund_shop", "location_id": "tfs-07",
        "window": window("12:05", 5),
        "metric": metric("terminal_saturation_pct", "%", "measured"),
        "sellable_quantity": None, "realtime_occupancy": 60, "safety_headroom": 40,
        "confidence": {"level": "high"},
        "collection": coll("12:00", sensor="pos-07"),
    },
)
add(
    event_id="zjj-cap-card-02", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-card-terminal-07",
    occurred_at=ts("12:20"), version=2, summary="【断网补传】终端占用 88%",
    payload={
        "owner": "tax_refund_shop", "location_id": "tfs-07",
        "window": window("12:20", 5),
        "metric": metric("terminal_saturation_pct", "%", "measured"),
        "sellable_quantity": None, "realtime_occupancy": 88, "safety_headroom": 12,
        "service_degradation": {"level": "degraded", "detail": "外卡受理点接近饱和"},
        "confidence": {"level": "medium", "note": "终端本地缓存，补传数据"},
        "collection": coll("12:20", transmission="backfill", ingested="12:40", sensor="pos-07"),
    },
)
add(
    event_id="zjj-cap-card-03", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-card-terminal-07",
    occurred_at=ts("12:33"), version=3, summary="【断网补传】终端占用 97%",
    payload={
        "owner": "tax_refund_shop", "location_id": "tfs-07",
        "window": window("12:35", 5),
        "metric": metric("terminal_saturation_pct", "%", "measured"),
        "sellable_quantity": None, "realtime_occupancy": 97, "safety_headroom": 3,
        "service_degradation": {"level": "severely_degraded", "detail": "外卡受理点接近饱和，等待明显拉长"},
        "confidence": {"level": "medium", "note": "终端本地缓存，补传数据"},
        "collection": coll("12:33", transmission="backfill", ingested="12:40", sensor="pos-07"),
    },
)

# ---------- 医疗点：多语急救仅一组值班 ----------
add(
    event_id="zjj-cap-firstaid-01", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-firstaid-i18n",
    occurred_at=ts("12:10"), version=1, summary="多语急救两组值班中",
    data_sharing=sharing("partners", ["scenic_area"]),
    payload={
        "owner": "medical_post", "location_id": "first-aid-center",
        "window": window("12:30", 30),
        "metric": metric("multilingual_responder_headroom", "组", "measured"),
        "sellable_quantity": None, "realtime_occupancy": 1, "safety_headroom": 3,
        "confidence": {"level": "high"}, "collection": coll("12:10", sensor="roster"),
    },
)
add(
    event_id="zjj-cap-firstaid-02", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-firstaid-i18n",
    occurred_at=ts("12:25"), version=2, summary="多语急救只剩一组值班",
    data_sharing=sharing("partners", ["scenic_area"]),
    payload={
        "owner": "medical_post", "location_id": "first-aid-center",
        "window": window("12:40", 30),
        "metric": metric("multilingual_responder_headroom", "组", "measured"),
        "sellable_quantity": None, "realtime_occupancy": 3, "safety_headroom": 1,
        "service_degradation": {"level": "severely_degraded", "detail": "多语急救只有一组值班人员，第二组在山下"},
        "confidence": {"level": "high"}, "collection": coll("12:25", sensor="roster"),
    },
)

# ---------- 社区：居民道路受影响（公开发布） ----------
community_series = [
    ("12:10", 1, "noticeable", "西入口周边车流量升高，居民出行略有延迟"),
    ("12:50", 2, "disrupted", "游客排队与接驳车辆外溢，社区道路拥堵，建议社会车辆绕行外环"),
    ("13:20", 3, "noticeable", "分流后社区路段回落"),
    ("13:35", 4, "none", "社区道路恢复正常"),
]
for tm, ver, level, detail in community_series:
    p = {
        "owner": "community", "location_id": "west-access-road",
        "window": window(tm, 10),
        "metric": metric("road_impact_index", "指数", "estimated"),
        "sellable_quantity": None,
        "realtime_occupancy": {"noticeable": 65, "disrupted": 88, "none": 30}[level],
        "safety_headroom": {"noticeable": 35, "disrupted": 12, "none": 70}[level],
        "confidence": {"level": "medium"},
        "collection": coll(tm, sensor="community-traffic-w"),
    }
    if level != "none":
        p["community_impact"] = {"level": level, "detail": detail}
    add(
        event_id=f"zjj-cap-community-{ver:02d}", event_type="CAPACITY_REPORTED",
        aggregate_type="capacity_source", aggregate_id="cap-community-road-west",
        occurred_at=ts(tm), version=ver, summary=f"社区道路压力 {tm}：{level}",
        data_sharing=sharing("public"), payload=p,
    )

# ---------- 住宿：远端吸纳能力（无共享声明，默认 restricted） ----------
add(
    event_id="zjj-cap-lodging-01", event_type="CAPACITY_REPORTED",
    aggregate_type="capacity_source", aggregate_id="cap-lodging-west",
    occurred_at=ts("12:05"), version=1, summary="片区住宿吸纳能力上报",
    payload={
        "owner": "lodging", "location_id": "west-cluster",
        "window": window("13:00", 60),
        "metric": metric("rooms_available", "间", "measured"),
        "sellable_quantity": 210, "realtime_occupancy": 1820, "safety_headroom": 210,
        "confidence": {"level": "high"}, "collection": coll("12:05", sensor="pms-aggregate"),
    },
)

# ---------- 触发信号 → 系统建议 → 责任方决定 ----------
add(
    event_id="zjj-pressure-01", event_type="PRESSURE_DETECTED",
    aggregate_type="pressure_signal", aggregate_id="sig-shuttle-west-20261003-01",
    occurred_at=ts("12:40"), version=1, correlation_id=CORR,
    summary="西入口接驳站安全余量耗尽并外溢社区道路（售票页仍有余票）",
    caused_by="zjj-cap-shuttle-04",
    data_sharing=sharing("partners", ["scenic_area", "community"]),
    payload={
        "owner": "transport", "location_id": "shuttle-west-gate",
        "pressure_kind": "shuttle_queue", "severity": "critical",
        "source_capacity_id": "cap-shuttle-west",
        "observed_value": -8, "threshold_value": 0, "metric_code": "queue_slots_headroom",
        "window": window("12:40", 10),
    },
)
add(
    event_id="zjj-plan-01", event_type="DIVERSION_PROPOSED",
    aggregate_type="diversion_plan", aggregate_id="plan-sig-shuttle-west-20261003-01",
    occurred_at=ts("12:42"), version=1, correlation_id=CORR, caused_by="zjj-pressure-01",
    summary="协调系统提出错峰、东线绕行与增车建议；既有预约照常履约",
    payload={
        "proposed_by": "coordination-system",
        "pressure_event_id": "zjj-pressure-01",
        "options": [
            {"option_type": "alternative_time_window",
             "description": "向未到达游客建议改约 60 分钟后接驳班次（改签须本人同意）",
             "estimated_relief_pct": 25},
            {"option_type": "alternative_route",
             "description": "引导至东门备用接驳线路 B，绕开饱和站点", "estimated_relief_pct": 30},
            {"option_type": "alternative_resource",
             "description": "临时增开 2 台接驳车，需交通值班方确认调派", "estimated_relief_pct": 35},
        ],
        "protected_commitments": [
            {"commitment_ref": "BK-G7781-20261003", "holder_kind": "large_group", "handling": "honor"},
            {"commitment_ref": "FIT-EN-9917", "holder_kind": "independent_traveler",
             "handling": "offer_reschedule_with_consent"},
        ],
    },
)
add(
    event_id="zjj-decision-01", event_type="ACTION_CONFIRMED",
    aggregate_type="operating_decision", aggregate_id="throttle-sig-shuttle-west-20261003-01",
    occurred_at=ts("12:45"), version=1, correlation_id=CORR, caused_by="zjj-plan-01",
    summary="交通值班经理确认西入口接驳限流并启用东线绕行，附明确恢复条件",
    data_sharing=sharing("public"),
    payload={
        "decision": "throttle_entry",
        "confirmed_by": {"owner": "transport", "role": "接驳运营值班经理", "party": "transport-duty-w03"},
        "plan_event_id": "zjj-plan-01",
        "pressure_event_id": "zjj-pressure-01",
        "scope": "西入口接驳站只出不进，新到游客引导东门线路B；团队 BK-G7781 已约班次照发",
        "recovery_condition": {
            "metric_code": "queue_slots_headroom",
            "source_capacity_id": "cap-shuttle-west",
            "observed_field": "safety_headroom",
            "comparator": "above",
            "restore_below": 120,
            "hold_minutes": 15,
            "also_require": ["community_impact:none"],
        },
    },
)


def ordered_for_file() -> list[dict]:
    """刻意保留断网补传错位：两条 backfill 在文件中也位于决策事件之后到达。"""
    backfill_ids = {"zjj-cap-card-02", "zjj-cap-card-03"}
    backfill = [e for e in events if e["event_id"] in backfill_ids]
    primary = [e for e in events if e["event_id"] not in backfill_ids]
    return primary + backfill


if __name__ == "__main__":
    out = Path(__file__).parents[1] / "data" / "scenario_zhangjiajie.json"
    ordered = ordered_for_file()
    out.write_text(json.dumps(ordered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(ordered)} events -> {out}")
