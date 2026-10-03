"""领域事件校验。

- validate_event：信封级校验，向后兼容 v0.1（无 payload 的旧样例仍通过）。
- validate_event_full：在信封之外，按 event_type 校验业务负载与跨字段一致性，
  接入方在启用协调能力时应使用严格校验。
不依赖第三方库，规则与 contracts/domain.schema.json 保持一致。
"""

from __future__ import annotations

from .timeutil import parse_ts

REQUIRED = (
    "event_id",
    "event_type",
    "aggregate_type",
    "aggregate_id",
    "occurred_at",
    "version",
    "summary",
)

EVENT_TYPES = {
    "CAPACITY_REPORTED",
    "PRESSURE_DETECTED",
    "DIVERSION_PROPOSED",
    "ACTION_CONFIRMED",
    "NORMAL_SERVICE_RESTORED",
}

AGGREGATE_FOR_EVENT = {
    "CAPACITY_REPORTED": "capacity_source",
    "PRESSURE_DETECTED": "pressure_signal",
    "DIVERSION_PROPOSED": "diversion_plan",
    "ACTION_CONFIRMED": "operating_decision",
    "NORMAL_SERVICE_RESTORED": "operating_decision",
}

CONFIDENCE_LEVELS = {"confirmed", "high", "medium", "low", "stale", "unverified"}
SEVERITIES = {"watch", "warning", "critical"}
DIMENSIONS = {
    "scenic_area_entry",
    "transport_shuttle",
    "accommodation",
    "tax_refund_shop",
    "medical_point",
    "community_road",
}
OPERATORS = {"lte", "lt", "gte", "gt", "eq"}
RESERVATION_TREATMENTS = {"honor", "offer_choice"}
ACTION_KINDS = {
    "hold_entry",
    "limit_ticketing",
    "divert_traffic_control",
    "add_shuttle_capacity",
    "deploy_emergency_staffing",
    "open_backup_lane",
    "communicate_advisory",
}


def _check_window(window: dict, errors: list[str], path: str) -> None:
    if not isinstance(window, dict) or "start" not in window or "end" not in window:
        errors.append(f"{path} 必须是含 start/end 的时间窗")
        return
    try:
        start, end = parse_ts(window["start"]), parse_ts(window["end"])
    except ValueError as exc:
        errors.append(f"{path} 时间格式错误：{exc}")
        return
    if end <= start:
        errors.append(f"{path} 结束时间必须晚于开始时间")


def validate_event(record: dict) -> list[str]:
    """信封级校验（v0.1 兼容）：旧生产方的无 payload 事件仍可通过。"""
    errors = [f"缺少字段：{name}" for name in REQUIRED if name not in record]
    if errors:
        return errors

    if not isinstance(record["event_id"], str) or not record["event_id"]:
        errors.append("event_id 必须是非空字符串")
    if record["event_type"] not in EVENT_TYPES:
        errors.append(f"event_type 非法：{record['event_type']}")
    if not isinstance(record["aggregate_id"], str) or not record["aggregate_id"]:
        errors.append("aggregate_id 必须是非空字符串")
    if not isinstance(record["version"], int) or isinstance(record["version"], bool) or record["version"] < 1:
        errors.append("version 必须是正整数")
    if not isinstance(record["summary"], str) or not record["summary"]:
        errors.append("summary 必须是非空字符串")
    try:
        parse_ts(record["occurred_at"])
    except (ValueError, TypeError) as exc:
        errors.append(f"occurred_at 格式错误：{exc}")
    if "received_at" in record:
        try:
            occurred = parse_ts(record["occurred_at"])
            received = parse_ts(record["received_at"])
        except (ValueError, TypeError) as exc:
            errors.append(f"received_at 格式错误：{exc}")
        else:
            if received < occurred:
                errors.append("received_at 不得早于 occurred_at（补报也不能改写发生时间）")
    if "time_window" in record:
        _check_window(record["time_window"], errors, "time_window")
    if "confidence" in record and record["confidence"] not in CONFIDENCE_LEVELS:
        errors.append(f"confidence 非法：{record['confidence']}")
    return errors


def validate_event_full(record: dict) -> list[str]:
    """严格校验：聚合类型匹配、payload 完整、关键治理规则可机器检查。"""
    errors = validate_event(record)
    event_type = record.get("event_type")
    if event_type not in EVENT_TYPES:
        return errors

    expected_agg = AGGREGATE_FOR_EVENT[event_type]
    if record.get("aggregate_type") != expected_agg:
        errors.append(f"{event_type} 的 aggregate_type 必须是 {expected_agg}")

    payload = record.get("payload")
    if not isinstance(payload, dict):
        errors.append(f"{event_type} 必须包含 payload 对象")
        return errors

    if event_type == "CAPACITY_REPORTED":
        errors.extend(_validate_capacity(payload))
    elif event_type == "PRESSURE_DETECTED":
        errors.extend(_validate_pressure(payload))
    elif event_type == "DIVERSION_PROPOSED":
        errors.extend(_validate_proposal(payload))
    elif event_type == "ACTION_CONFIRMED":
        errors.extend(_validate_action(payload))
    elif event_type == "NORMAL_SERVICE_RESTORED":
        errors.extend(_validate_restored(payload))
    return errors


def _validate_scope(scope: object, errors: list[str]) -> None:
    if not isinstance(scope, dict):
        errors.append("scope 必须是对象")
        return
    if scope.get("dimension") not in DIMENSIONS:
        errors.append("scope.dimension 非法")
    if not isinstance(scope.get("source_id"), str) or not scope["source_id"]:
        errors.append("scope.source_id 必须是非空字符串")


def _validate_capacity(payload: dict) -> list[str]:
    errors: list[str] = []
    _validate_scope(payload.get("scope"), errors)
    _check_window(payload.get("window"), errors, "payload.window")
    readings = payload.get("readings")
    if not isinstance(readings, list) or not readings:
        errors.append("readings 必须是非空数组")
    else:
        for i, reading in enumerate(readings):
            if not isinstance(reading, dict) or "metric" not in reading or "value" not in reading:
                errors.append(f"readings[{i}] 必须含 metric/value")
                continue
            if not isinstance(reading["value"], (int, float)) or isinstance(reading["value"], bool):
                errors.append(f"readings[{i}].value 必须是数字")
            if reading.get("confidence") is not None and reading.get("confidence") not in CONFIDENCE_LEVELS:
                errors.append(f"readings[{i}].confidence 非法")
    return errors


def _validate_pressure(payload: dict) -> list[str]:
    errors: list[str] = []
    _validate_scope(payload.get("scope"), errors)
    if payload.get("severity") not in SEVERITIES:
        errors.append("severity 必须是 watch/warning/critical")
    _check_window(payload.get("window"), errors, "payload.window")
    based_on = payload.get("based_on")
    if not isinstance(based_on, list) or not based_on:
        errors.append("based_on 必须是非空事件 id 数组")
    confidence = payload.get("confidence")
    if confidence is not None and confidence not in CONFIDENCE_LEVELS:
        errors.append("confidence 非法")
    impact = payload.get("resident_impact")
    if impact is not None:
        level = impact.get("road_impact_level")
        if not isinstance(level, int) or not 0 <= level <= 3:
            errors.append("resident_impact.road_impact_level 必须是 0..3")
    return errors


def _validate_proposal(payload: dict) -> list[str]:
    errors: list[str] = []
    triggers = payload.get("trigger_signal_ids")
    if not isinstance(triggers, list) or not triggers:
        errors.append("trigger_signal_ids 必须是非空数组")
    alternatives = payload.get("alternatives")
    if not isinstance(alternatives, dict) or not any(
        isinstance(alternatives.get(k), list) and alternatives[k]
        for k in ("time_slots", "routes", "resources")
    ):
        errors.append("alternatives 至少包含一个非空的 time_slots/routes/resources")
    for i, item in enumerate(payload.get("affected_reservations", [])):
        if not isinstance(item, dict) or item.get("treatment") not in RESERVATION_TREATMENTS:
            errors.append(f"affected_reservations[{i}].treatment 只能是 honor/offer_choice")
        if not isinstance(item.get("count"), int) or item["count"] < 0:
            errors.append(f"affected_reservations[{i}].count 必须是非负整数")
    return errors


def _validate_action(payload: dict) -> list[str]:
    errors: list[str] = []
    if not isinstance(payload.get("proposal_id"), str) or not payload["proposal_id"]:
        errors.append("proposal_id 必须是非空字符串")
    action = payload.get("action")
    if not isinstance(action, dict):
        errors.append("action 必须是对象")
    else:
        if action.get("kind") not in ACTION_KINDS:
            errors.append("action.kind 非法")
        scopes = action.get("scope_ids")
        if not isinstance(scopes, list) or not scopes:
            errors.append("action.scope_ids 必须是非空数组")
        _check_window(action.get("window"), errors, "action.window")
    confirmer = payload.get("confirmed_by")
    if not isinstance(confirmer, dict) or not confirmer.get("org_id") or not confirmer.get("role"):
        errors.append("confirmed_by 必须含 org_id/role")
    if not isinstance(payload.get("authority"), str) or not payload["authority"]:
        errors.append("authority 必须说明责任边界")
    for i, cond in enumerate(payload.get("recovery_conditions", [])):
        if cond.get("operator") not in OPERATORS:
            errors.append(f"recovery_conditions[{i}].operator 非法")
        if not isinstance(cond.get("threshold"), (int, float)):
            errors.append(f"recovery_conditions[{i}].threshold 必须是数字")
        _check_window(cond.get("window"), errors, f"recovery_conditions[{i}].window")
    return errors


def _validate_restored(payload: dict) -> list[str]:
    errors: list[str] = []
    if not isinstance(payload.get("decision_id"), str) or not payload["decision_id"]:
        errors.append("decision_id 必须是非空字符串")
    evidence = payload.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        errors.append("evidence 必须是非空数组")
    else:
        for i, item in enumerate(evidence):
            if not isinstance(item, dict):
                errors.append(f"evidence[{i}] 必须是对象")
                continue
            for name in ("condition_id", "metric", "window"):
                if not item.get(name):
                    errors.append(f"evidence[{i}].{name} 缺失")
            if not isinstance(item.get("observed"), (int, float)):
                errors.append(f"evidence[{i}].observed 必须是数字")
            if item.get("confidence") not in CONFIDENCE_LEVELS:
                errors.append(f"evidence[{i}].confidence 非法")
    return errors
