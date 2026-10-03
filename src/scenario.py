"""2026-10-03 张家界入境游承载协调场景。

时间线（UTC+8）：
- 08:00–09:30 各来源常规/基线读数；
- 09:35 接驳车排队传感器断网，09:50/10:05 两个读数滞留现场，
  10:15 恢复连接后补报（occurred_at 仍是读数当时）；
- 售票页仍有余票，但接驳排队、外卡受理、多语急救、社区道路相继告急；
- 10:25 引擎检测到多源局部压力，10:27 提出替代时段/线路/资源方案；
- 10:35 景区与交警分别确认限流/绕行（各自责任边界），并设定恢复条件；
- 11:30 首次恢复评估未通过（持续时间不足），11:40 证据齐备后解除。

本模块只构造事实与调用公开 API，不绕过任何治理规则。
"""

from __future__ import annotations

from .coordination import (
    CoordinationTrace,
    DiversionEngine,
    RecoveryEvaluator,
    ResourceOption,
    capacity_event,
    confirm_action,
    reset_id_sequence,
)
from .eventstore import EventStore
from .model import CapacityBoard, ForecastFactors
from .privacy import EphemeralVisitorSignals
from .projections import DutyProjection, PublicProjection

CORRELATION = "C-20261003-zjj-01"
TZ = "+08:00"


def _ts(hour_minute: str) -> str:
    return f"2026-10-03T{hour_minute}:00{TZ}"


def _window(start: str, end: str) -> dict:
    return {"start": _ts(start), "end": _ts(end)}


def _build_raw_events(store: EventStore) -> None:
    # --- 常规读数 ---
    store.append(
        _capacity(
            store, "cap-zjj-ticket", "zjj-scenic-bureau", _ts("08:00"),
            _window("08:00", "08:15"),
            {"dimension": "scenic_area_entry", "source_id": "zjj-ticket", "label": "张家界森林公园票务"},
            [
                {"metric": "tickets_available", "value": 42000, "unit": "person", "basis": "票务系统拉单", "confidence": "confirmed"},
                {"metric": "occupancy_rate", "value": 34, "unit": "pct", "basis": "闸门计数去重", "confidence": "high"},
            ],
            data_categories=["ticket_system_pull", "gate_count_dedup"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-shuttle-q", "zjj-transport-bureau", _ts("09:20"),
            _window("09:20", "09:30"),
            {"dimension": "transport_shuttle", "source_id": "zjj-shuttle-queue", "label": "门票站接驳车排队点"},
            [
                {"metric": "queue_length_minutes", "value": 22, "unit": "minute", "basis": "排队登记折半", "confidence": "medium"},
            ],
            sensor_id="shuttle-q-gate-07",
            data_categories=["queue_registry_half"],
        )
    )

    # --- 断网期间滞留、10:15 恢复后补报的两个读数（发生时间仍是 09:50 / 10:05）---
    outage = {"lost_at": _ts("09:35"), "recovered_at": _ts("10:15")}
    store.append(
        _capacity(
            store, "cap-zjj-shuttle-q", "zjj-transport-bureau", _ts("09:50"),
            _window("09:50", "10:00"),
            {"dimension": "transport_shuttle", "source_id": "zjj-shuttle-queue", "label": "门票站接驳车排队点"},
            [
                {"metric": "queue_length_minutes", "value": 31, "unit": "minute", "basis": "排队登记折半", "confidence": "medium", "confidence_reason": "断网补报，人工核过登记台账"},
            ],
            sensor_id="shuttle-q-gate-07", ingest="backfill_after_outage", outage=outage,
            received_at=_ts("10:15"), data_categories=["queue_registry_half", "manual_report"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-shuttle-q", "zjj-transport-bureau", _ts("10:05"),
            _window("10:05", "10:15"),
            {"dimension": "transport_shuttle", "source_id": "zjj-shuttle-queue", "label": "门票站接驳车排队点"},
            [
                {"metric": "queue_length_minutes", "value": 38, "unit": "minute", "basis": "排队登记折半", "confidence": "medium"},
            ],
            sensor_id="shuttle-q-gate-07", ingest="backfill_after_outage", outage=outage,
            received_at=_ts("10:15"), data_categories=["queue_registry_half"],
        )
    )

    # --- 09:30 各来源基线读数（与 10:1x 读数构成趋势，支撑“下一处失守”外推）---
    store.append(
        _capacity(
            store, "cap-zjj-taxrefund", "zjj-taxrefund-mall", _ts("09:30"),
            _window("09:30", "09:40"),
            {"dimension": "tax_refund_shop", "source_id": "zjj-taxrefund", "label": "标志门退税商店外卡受理点"},
            [
                {"metric": "infrastructure_saturation", "value": 0.62, "basis": "POS 受理笔数/窗口能力", "confidence": "high"},
                {"metric": "wait_time_minutes", "value": 7, "unit": "minute", "basis": "叫号系统", "confidence": "medium"},
            ],
            data_categories=["ticket_system_pull"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-medical", "zjj-health-point", _ts("09:30"),
            _window("09:30", "09:40"),
            {"dimension": "medical_point", "source_id": "zjj-medical", "label": "门票站多语急救点"},
            [
                {"metric": "wait_time_minutes", "value": 5, "unit": "minute", "basis": "急救点登记", "confidence": "medium"},
                {"metric": "staff_on_duty_stations", "value": 1, "unit": "station", "basis": "值班排班表", "confidence": "confirmed"},
                {"metric": "staff_idle_ratio", "value": 0.6, "basis": "值班排班表", "confidence": "confirmed"},
            ],
            data_categories=["staff_roster_duty"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-community", "zjj-community-office", _ts("09:30"),
            _window("09:30", "09:45"),
            {"dimension": "community_road", "source_id": "zjj-community-road", "label": "锣鼓塔社区出入道路"},
            [
                {"metric": "resident_road_impact_level", "value": 0, "unit": "level", "basis": "社区网格员巡查", "confidence": "high"},
            ],
            data_categories=["community_report"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-lodging", "zjj-lodging-association", _ts("09:30"),
            _window("09:30", "10:00"),
            {"dimension": "accommodation", "source_id": "zjj-lodging", "label": "标志门片区住宿业"},
            [
                {"metric": "occupancy_rate", "value": 82, "unit": "pct", "basis": "住宿业协会抽样报送", "confidence": "medium"},
            ],
            data_categories=["manual_report"],
        )
    )

    # --- 10:05–10:15 其他来源：余票还在，局部已承压 ---
    store.append(
        _capacity(
            store, "cap-zjj-ticket", "zjj-scenic-bureau", _ts("10:05"),
            _window("10:05", "10:15"),
            {"dimension": "scenic_area_entry", "source_id": "zjj-ticket", "label": "张家界森林公园票务"},
            [
                {"metric": "tickets_available", "value": 8600, "unit": "person", "basis": "票务系统拉单", "confidence": "confirmed"},
                {"metric": "occupancy_rate", "value": 83, "unit": "pct", "basis": "闸门计数去重", "confidence": "high"},
            ],
            data_categories=["ticket_system_pull", "gate_count_dedup"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-entry-safety", "zjj-scenic-bureau", _ts("10:05"),
            _window("10:05", "10:15"),
            {"dimension": "scenic_area_entry", "source_id": "zjj-entry-safety", "label": "门票站广场安全容量"},
            [
                {"metric": "safety_margin", "value": 12, "unit": "pct", "basis": "现场指挥点核定人数比对", "confidence": "high"},
            ],
            data_categories=["manual_report", "gate_count_dedup"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-taxrefund", "zjj-taxrefund-mall", _ts("10:10"),
            _window("10:10", "10:20"),
            {"dimension": "tax_refund_shop", "source_id": "zjj-taxrefund", "label": "标志门退税商店外卡受理点"},
            [
                {"metric": "infrastructure_saturation", "value": 0.70, "basis": "POS 受理笔数/窗口能力", "confidence": "high"},
                {"metric": "wait_time_minutes", "value": 13, "unit": "minute", "basis": "叫号系统", "confidence": "medium"},
            ],
            data_categories=["ticket_system_pull"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-medical", "zjj-health-point", _ts("10:12"),
            _window("10:12", "10:22"),
            {"dimension": "medical_point", "source_id": "zjj-medical", "label": "门票站多语急救点"},
            [
                {"metric": "wait_time_minutes", "value": 9, "unit": "minute", "basis": "急救点登记", "confidence": "medium"},
                {"metric": "staff_on_duty_stations", "value": 1, "unit": "station", "basis": "值班排班表", "confidence": "confirmed"},
                {"metric": "staff_idle_ratio", "value": 0.45, "basis": "值班排班表", "confidence": "confirmed"},
            ],
            data_categories=["staff_roster_duty", "manual_report"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-community", "zjj-community-office", _ts("10:15"),
            _window("10:15", "10:30"),
            {"dimension": "community_road", "source_id": "zjj-community-road", "label": "锣鼓塔社区出入道路"},
            [
                {"metric": "resident_road_impact_level", "value": 2, "unit": "level", "basis": "社区网格员巡查+交警反馈", "confidence": "medium"},
            ],
            data_categories=["community_report", "police_report"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-lodging", "zjj-lodging-association", _ts("10:15"),
            _window("10:15", "10:45"),
            {"dimension": "accommodation", "source_id": "zjj-lodging", "label": "标志门片区住宿业"},
            [
                {"metric": "occupancy_rate", "value": 91, "unit": "pct", "basis": "住宿业协会抽样报送", "confidence": "medium"},
            ],
            data_categories=["manual_report"],
        )
    )


def _capacity(
    store: EventStore,
    aggregate_id: str,
    org_id: str,
    occurred_at: str,
    window: dict,
    scope: dict,
    readings: list[dict],
    *,
    ingest: str = "live",
    received_at: str | None = None,
    sensor_id: str | None = None,
    outage: dict | None = None,
    data_categories: list[str] | None = None,
) -> dict:
    return capacity_event(
        source_aggregate_id=aggregate_id,
        org_id=org_id,
        occurred_at=occurred_at,
        received_at=received_at,
        window=window,
        scope=scope,
        readings=readings,
        version=store.next_version(aggregate_id),
        ingest=ingest,
        sensor_id=sensor_id,
        outage=outage,
        data_categories=data_categories,
    )


def build() -> dict:
    """构建完整场景并返回上下文。"""
    reset_id_sequence()
    store = EventStore()
    _build_raw_events(store)

    forecast = ForecastFactors(
        group_arrivals=2600,
        independent_arrivals=5400,
        visa_policy_notes="240小时免签政策延续，近期新增东南亚客源市场",
        confidence="medium",
    )

    engine = DiversionEngine(
        store,
        resource_catalog={
            "*:shuttle_bus": [
                # 仅示例资源描述，真实目录由交通公司维护
            ],
        },
    )
    # 显式注册可用资源（接驳备用车、外卡移动 POS、第二多语急救组）
    engine.resource_catalog = {
        "zjj-shuttle-queue:shuttle_bus": [
            _resource("shuttle_bus", "shuttle-reserve-03", "备用接驳车3台（西线车队）", 30.0),
        ],
        "zjj-taxrefund:foreign_card_pos": [
            _resource("foreign_card_pos", "mobile-pos-11", "移动外卡受理终端2台", 25.0, confidence_reason="需退税店店长签字领取"),
        ],
        "zjj-medical:multilingual_first_aid_team": [
            _resource("multilingual_first_aid_team", "first-aid-team-b", "多语急救第二值班组（英/韩/俄）", 45.0, confidence_level="high"),
        ],
        "zjj-community-road:road_lane": [
            _resource("road_lane", "lane-freight-bypass", "货运外环备用通道", 20.0),
        ],
    }

    # 1) 触发信号（基于按 occurred_at 重放的事实流，补报读数已归位）
    signals = engine.detect_pressure(
        _ts("10:25"), correlation_id=CORRELATION, forecast=forecast
    )

    # 2) 系统提案：替代时段/线路/资源；既有预约只提供选择、绝不静默取消
    proposal = engine.propose(
        _ts("10:27"),
        correlation_id=CORRELATION,
        trigger_events=signals,
        forecast=forecast,
        reservation_options=[
            {"count": 1320, "treatment": "offer_choice"},
        ],
    )

    # 3) 责任方决定（各自只在自己的授权范围内确认）
    decision_scenic = confirm_action(
        store, _ts("10:35"),
        correlation_id=CORRELATION,
        proposal=proposal,
        action_kind="hold_entry",
        scope_ids=["zjj-ticket", "zjj-entry-safety"],
        window=_window("10:35", "12:00"),
        org_id="zjj-scenic-bureau",
        role="景区值班总指挥",
        person_ref="duty-A07",
        authority="本景区门票站入口放行节奏与限流启动",
        reservation_commitment_policy="offer_choice_no_cancel",
        recovery_conditions=[
            {
                "condition_id": "rc-queue-down",
                "metric": "queue_length_minutes",
                "operator": "lte",
                "threshold": 15,
                "window": _window("10:35", "12:00"),
                "evidence_source_ids": ["zjj-shuttle-queue"],
                "sustain_minutes": 20,
            },
            {
                "condition_id": "rc-safety-back",
                "metric": "safety_margin",
                "operator": "gte",
                "threshold": 15,
                "window": _window("10:35", "12:00"),
                "evidence_source_ids": ["zjj-entry-safety"],
                "sustain_minutes": 10,
            },
        ],
    )
    decision_police = confirm_action(
        store, _ts("10:35"),
        correlation_id=CORRELATION,
        proposal=proposal,
        action_kind="divert_traffic_control",
        scope_ids=["zjj-community-road", "zjj-shuttle-queue"],
        window=_window("10:35", "12:00"),
        org_id="zjj-traffic-police",
        role="辖区交警值班长",
        person_ref="duty-P03",
        authority="辖区道路临时分流与居民通道保障",
        recovery_conditions=[
            {
                "condition_id": "rc-community-road",
                "metric": "resident_road_impact_level",
                "operator": "lte",
                "threshold": 1,
                "window": _window("10:35", "12:00"),
                "evidence_source_ids": ["zjj-community-road"],
                "sustain_minutes": 15,
            },
        ],
    )

    # 4) 措施见效后的读数
    for hm, value in [("11:00", 24), ("11:10", 18), ("11:20", 14), ("11:30", 11), ("11:40", 10)]:
        store.append(
            _capacity(
                store, "cap-zjj-shuttle-q", "zjj-transport-bureau", _ts(hm),
                _window(hm, f"{int(hm[:2])}:{int(hm[3:]) + 10:02d}"),
                {"dimension": "transport_shuttle", "source_id": "zjj-shuttle-queue", "label": "门票站接驳车排队点"},
                [
                    {"metric": "queue_length_minutes", "value": value, "unit": "minute", "basis": "排队登记折半", "confidence": "medium"},
                ],
                sensor_id="shuttle-q-gate-07", data_categories=["queue_registry_half"],
            )
        )
    store.append(
        _capacity(
            store, "cap-zjj-entry-safety", "zjj-scenic-bureau", _ts("11:20"),
            _window("11:20", "11:30"),
            {"dimension": "scenic_area_entry", "source_id": "zjj-entry-safety", "label": "门票站广场安全容量"},
            [
                {"metric": "safety_margin", "value": 18, "unit": "pct", "basis": "现场指挥点核定人数比对", "confidence": "high"},
            ],
            data_categories=["manual_report", "gate_count_dedup"],
        )
    )
    store.append(
        _capacity(
            store, "cap-zjj-community", "zjj-community-office", _ts("11:25"),
            _window("11:25", "11:40"),
            {"dimension": "community_road", "source_id": "zjj-community-road", "label": "锣鼓塔社区出入道路"},
            [
                {"metric": "resident_road_impact_level", "value": 1, "unit": "level", "basis": "社区网格员巡查+交警反馈", "confidence": "medium"},
            ],
            data_categories=["community_report", "police_report"],
        )
    )

    board = CapacityBoard(store.replay())
    evaluator = RecoveryEvaluator(board)
    # 11:30 第一次评估：排队条件持续观察不满 20 分钟，不能恢复
    early_status_scenic = evaluator.evaluate(decision_scenic, _ts("11:30"))
    early_restore = evaluator.restore(
        store, _ts("11:30"),
        decision=decision_scenic,
        resolved_by={"org_id": "zjj-scenic-bureau", "role": "景区值班总指挥", "person_ref": "duty-A07"},
    )

    # 11:40 证据齐备，两路分别解除
    restore_scenic = evaluator.restore(
        store, _ts("11:40"),
        decision=decision_scenic,
        resolved_by={"org_id": "zjj-scenic-bureau", "role": "景区值班总指挥", "person_ref": "duty-A07"},
        notes="排队与安全余量均满足恢复条件并持续达标",
    )
    restore_police = evaluator.restore(
        store, _ts("11:40"),
        decision=decision_police,
        resolved_by={"org_id": "zjj-traffic-police", "role": "辖区交警值班长", "person_ref": "duty-P03"},
        residual_risk_level="medium",
        notes="午后团队高峰仍需关注，保留绕行引导",
    )

    # 匿名位置与语言偏好：只在当次会话内存中聚合
    signals_ephemeral = EphemeralVisitorSignals(ttl_minutes=120)
    _seed_language_signals(signals_ephemeral)
    language_needs = signals_ephemeral.language_needs(_ts("10:25"))

    # 全链路终态视图（11:40 恢复后）
    trace = CoordinationTrace(store).trace(CORRELATION, board=board, moment=_ts("11:40"))

    # 公众页：10:40 时点，决策已生效、恢复尚未发生；事件按“当时已发生”裁剪，
    # 容量看板也只纳入当时已有读数（补报读数 10:15 已到，后续恢复读数不可见）。
    board_1040 = CapacityBoard(store.as_of_occurred(_ts("10:40")))
    trace_1040 = CoordinationTrace(store).trace(CORRELATION, board=board_1040, moment=_ts("10:40"))
    public = PublicProjection(board_1040).build(_ts("10:40"), active_traces=[trace_1040])

    correlation_index = {}
    for s in trace["stages"]["trigger_signals"]:
        correlation_index[f"{s['source_id']}:{s['metric']}"] = CORRELATION
    board_1025 = CapacityBoard(store.as_of_occurred(_ts("10:25")))
    duty = DutyProjection(board_1025, store).build(
        _ts("10:25"), forecast=forecast, correlation_index=correlation_index
    )

    return {
        "store": store,
        "forecast": forecast,
        "engine": engine,
        "signals": signals,
        "proposal": proposal,
        "decisions": [decision_scenic, decision_police],
        "early_status_scenic": early_status_scenic,
        "early_restore": early_restore,
        "restorations": [restore_scenic, restore_police],
        "trace": trace,
        "public_view": public,
        "duty_view": duty,
        "language_needs": language_needs,
        "ephemeral": signals_ephemeral,
        "board": board,
    }


def _resource(kind: str, target_id: str, label: str, relief: float, *, confidence_level: str = "medium", confidence_reason: str = ""):
    return ResourceOption(
        kind=kind, target_id=target_id, label=label, relief_pct=relief,
        confidence_level=confidence_level, confidence_reason=confidence_reason,
    )


def _seed_language_signals(bucket: EphemeralVisitorSignals) -> None:
    # 同一网格的匿名计数；小样本语种（示例中的法语2人）不会被单独输出
    for _ in range(18):
        bucket.add(grid="grid:zjj-gate-menshan", language="en", at="2026-10-03T10:10:00+08:00")
    for _ in range(9):
        bucket.add(grid="grid:zjj-gate-menshan", language="ko", at="2026-10-03T10:11:00+08:00")
    for _ in range(6):
        bucket.add(grid="grid:zjj-gate-menshan", language="ru", at="2026-10-03T10:12:00+08:00")
    for _ in range(2):
        bucket.add(grid="grid:zjj-gate-menshan", language="fr", at="2026-10-03T10:12:00+08:00")

