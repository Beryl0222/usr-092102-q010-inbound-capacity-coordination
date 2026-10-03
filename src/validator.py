"""校验领域事件信封及各事件类型的 payload 结构。

仅依赖标准库；信封字段自 v1 起稳定，payload 按 event_type 分别校验。
"""

from datetime import datetime

REQUIRED = ("event_id", "event_type", "aggregate_type", "aggregate_id", "occurred_at", "version", "summary")

EVENT_TYPES = {
    "CAPACITY_REPORTED",
    "PRESSURE_DETECTED",
    "DIVERSION_PROPOSED",
    "ACTION_CONFIRMED",
    "NORMAL_SERVICE_RESTORED",
}
AGGREGATE_TYPES = {"capacity_source", "pressure_signal", "diversion_plan", "operating_decision"}
OWNERS = {"scenic_area", "transport", "lodging", "tax_refund_shop", "medical_post", "community"}

_PAYLOAD_FOR_EVENT = {
    "CAPACITY_REPORTED": "capacity_report",
    "PRESSURE_DETECTED": "pressure",
    "DIVERSION_PROPOSED": "diversion_plan",
    "ACTION_CONFIRMED": "operating_decision",
    "NORMAL_SERVICE_RESTORED": "operating_decision",
}
_AGGREGATE_FOR_EVENT = {
    "CAPACITY_REPORTED": "capacity_source",
    "PRESSURE_DETECTED": "pressure_signal",
    "DIVERSION_PROPOSED": "diversion_plan",
    "ACTION_CONFIRMED": "operating_decision",
    "NORMAL_SERVICE_RESTORED": "operating_decision",
}


def _require_keys(obj: dict, keys: tuple[str, ...], path: str) -> list[str]:
    return [f"{path} 缺少字段：{k}" for k in keys if k not in obj]


def _check_enum(value, allowed: set, path: str) -> list[str]:
    return [] if value in allowed else [f"{path} 取值 {value!r} 不在 {sorted(allowed)} 中"]


def _check_datetime(value, path: str) -> list[str]:
    if not isinstance(value, str):
        return [f"{path} 必须是时间字符串"]
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return [f"{path} 不是合法 date-time：{value!r}"]
    if dt.tzinfo is None:
        return [f"{path} 必须带时区偏移：{value!r}"]
    return []


def _check_window(w: dict, path: str) -> list[str]:
    errors = _require_keys(w, ("starts_at", "ends_at"), path)
    if errors:
        return errors
    errors += _check_datetime(w["starts_at"], f"{path}.starts_at")
    errors += _check_datetime(w["ends_at"], f"{path}.ends_at")
    if not errors:
        starts, ends = datetime.fromisoformat(w["starts_at"]), datetime.fromisoformat(w["ends_at"])
        if ends <= starts:
            errors.append(f"{path}.ends_at 必须晚于 starts_at")
    g = w.get("granularity_minutes")
    if g is not None and (not isinstance(g, int) or g < 1):
        errors.append(f"{path}.granularity_minutes 必须为正整数")
    return errors


def _check_metric(m: dict, path: str) -> list[str]:
    errors = _require_keys(m, ("code", "unit", "basis"), path)
    if errors:
        return errors
    if not isinstance(m["code"], str) or not m["code"]:
        errors.append(f"{path}.code 必须是非空字符串")
    if not isinstance(m["unit"], str) or not m["unit"]:
        errors.append(f"{path}.unit 必须是非空字符串")
    errors += _check_enum(m["basis"], {"measured", "estimated", "forecast"}, f"{path}.basis")
    return errors


def _check_confidence(c: dict, path: str) -> list[str]:
    errors = _require_keys(c, ("level",), path)
    if errors:
        return errors
    errors += _check_enum(c["level"], {"high", "medium", "low"}, f"{path}.level")
    score = c.get("score")
    if score is not None and not (isinstance(score, (int, float)) and 0 <= score <= 1):
        errors.append(f"{path}.score 必须在 0..1 之间")
    return errors


def _check_payload(payload: dict, kind: str, path: str, event_type: str | None = None) -> list[str]:
    if not isinstance(payload, dict):
        return [f"{path} 必须是对象"]
    if kind == "capacity_report":
        return _check_capacity_report(payload, path)
    if kind == "pressure":
        return _check_pressure(payload, path)
    if kind == "diversion_plan":
        return _check_diversion_plan(payload, path)
    if kind == "operating_decision":
        return _check_operating_decision(payload, path, event_type)
    return [f"{path} 未知 payload 类型 {kind}"]


def _check_capacity_report(p: dict, path: str) -> list[str]:
    errors = _require_keys(p, ("owner", "window", "metric"), path)
    if errors:
        return errors
    errors += _check_enum(p["owner"], OWNERS, f"{path}.owner")
    errors += _check_window(p["window"], f"{path}.window")
    errors += _check_metric(p["metric"], f"{path}.metric")
    if "confidence" in p:
        errors += _check_confidence(p["confidence"], f"{path}.confidence")
    coll = p.get("collection")
    if coll is not None:
        if coll.get("transmission") not in (None, "live", "backfill"):
            errors.append(f"{path}.collection.transmission 必须为 live/backfill")
        for k in ("observed_at", "ingested_at"):
            if coll.get(k) is not None:
                errors += _check_datetime(coll[k], f"{path}.collection.{k}")
    return errors


def _check_pressure(p: dict, path: str) -> list[str]:
    errors = _require_keys(p, ("owner", "pressure_kind", "severity", "source_capacity_id"), path)
    if errors:
        return errors
    errors += _check_enum(p["owner"], OWNERS, f"{path}.owner")
    errors += _check_enum(
        p["pressure_kind"],
        {
            "ticket_vs_onsite_mismatch",
            "shuttle_queue",
            "card_terminal_saturation",
            "multilingual_first_aid",
            "lodging_absorption",
            "tax_refund_congestion",
            "community_road",
        },
        f"{path}.pressure_kind",
    )
    errors += _check_enum(p["severity"], {"watch", "breach", "critical"}, f"{path}.severity")
    if "window" in p:
        errors += _check_window(p["window"], f"{path}.window")
    proj = p.get("projection_minutes")
    if proj is not None and (not isinstance(proj, int) or proj < 1):
        errors.append(f"{path}.projection_minutes 必须为正整数")
    return errors


def _check_diversion_plan(p: dict, path: str) -> list[str]:
    errors = _require_keys(p, ("proposed_by", "pressure_event_id", "options"), path)
    if errors:
        return errors
    if p["proposed_by"] != "coordination-system":
        errors.append(f"{path}.proposed_by 必须为 coordination-system（系统只建议，不决策）")
    options = p.get("options", [])
    if not isinstance(options, list) or not options:
        errors.append(f"{path}.options 至少包含一个替代时段/线路/资源方案")
    else:
        for i, opt in enumerate(options):
            opath = f"{path}.options[{i}]"
            errs = _require_keys(opt, ("option_type", "description"), opath)
            if not errs:
                errs += _check_enum(
                    opt["option_type"],
                    {"alternative_time_window", "alternative_route", "alternative_resource"},
                    f"{opath}.option_type",
                )
            errors += errs
    for i, c in enumerate(p.get("protected_commitments", [])):
        cpath = f"{path}.protected_commitments[{i}]"
        if "commitment_ref" not in c:
            errors.append(f"{cpath} 缺少 commitment_ref")
        elif c.get("handling") not in (None, "honor", "offer_reschedule_with_consent"):
            errors.append(f"{cpath}.handling 只能是 honor 或 offer_reschedule_with_consent，禁止静默取消")
    return errors


def _check_operating_decision(p: dict, path: str, event_type: str | None = None) -> list[str]:
    errors = _require_keys(p, ("decision", "confirmed_by"), path)
    if errors:
        return errors
    errors += _check_enum(
        p["decision"],
        {"throttle_entry", "reroute", "add_resource", "hold_no_change", "lift_throttle"},
        f"{path}.decision",
    )
    by = p["confirmed_by"]
    errors += _require_keys(by, ("owner", "role", "party"), f"{path}.confirmed_by")
    if not errors and by["owner"] not in OWNERS:
        errors += _check_enum(by["owner"], OWNERS, f"{path}.confirmed_by.owner")
    if not errors and by["party"] == "coordination-system":
        errors.append(f"{path}.confirmed_by.party 不能是协调系统自身：关闭入口只能由责任方确认")
    rc = p.get("recovery_condition")
    if rc is not None:
        rpath = f"{path}.recovery_condition"
        errors += _require_keys(rc, ("metric_code", "restore_below", "hold_minutes"), rpath)
        if not errors and (not isinstance(rc["hold_minutes"], int) or rc["hold_minutes"] < 0):
            errors.append(f"{rpath}.hold_minutes 必须是非负整数")
        if rc.get("comparator") not in (None, "below", "above"):
            errors.append(f"{rpath}.comparator 只能为 below/above")
        if rc.get("observed_field") not in (None, "sellable_quantity", "realtime_occupancy", "safety_headroom"):
            errors.append(f"{rpath}.observed_field 取值非法")
    if p.get("decision") == "throttle_entry" and rc is None:
        errors.append(f"{path}: throttle_entry 必须附带 recovery_condition，限流要能追到解除条件")
    if event_type == "NORMAL_SERVICE_RESTORED":
        if p.get("decision") != "lift_throttle":
            errors.append(f"{path}: NORMAL_SERVICE_RESTORED 的 decision 必须为 lift_throttle")
        if not p.get("restores_decision_event_id"):
            errors.append(f"{path}: 恢复事件必须回填 restores_decision_event_id，闭合因果链")
        evaluation = p.get("evaluation")
        if not isinstance(evaluation, dict) or evaluation.get("satisfied") is not True:
            errors.append(f"{path}: 恢复必须附 evaluation 且 satisfied=true，未满足恢复条件不得解除")
    return errors


def validate_event(record: dict) -> list[str]:
    """返回错误信息列表；空列表表示通过。兼容仅含信封字段的 v1 记录。"""
    errors = [f"缺少字段：{name}" for name in REQUIRED if name not in record]
    if errors:
        return errors

    if not isinstance(record["event_id"], str) or not record["event_id"]:
        errors.append("event_id 必须是非空字符串")
    errors += _check_enum(record["event_type"], EVENT_TYPES, "event_type")
    errors += _check_enum(record["aggregate_type"], AGGREGATE_TYPES, "aggregate_type")
    if not isinstance(record["aggregate_id"], str) or not record["aggregate_id"]:
        errors.append("aggregate_id 必须是非空字符串")
    errors += _check_datetime(record["occurred_at"], "occurred_at")
    if not isinstance(record["version"], int) or isinstance(record["version"], bool) or record["version"] < 1:
        errors.append("version 必须是正整数")
    if not isinstance(record["summary"], str) or not record["summary"]:
        errors.append("summary 必须是非空字符串")

    expected_aggregate = _AGGREGATE_FOR_EVENT.get(record.get("event_type"))
    if expected_aggregate and record.get("aggregate_type") != expected_aggregate:
        errors.append(
            f"event_type {record['event_type']} 的 aggregate_type 应为 {expected_aggregate}"
        )

    for link in ("correlation_id", "caused_by"):
        if link in record and not isinstance(record[link], str):
            errors.append(f"{link} 必须是字符串")

    if "payload" in record:
        kind = _PAYLOAD_FOR_EVENT.get(record["event_type"])
        if kind:
            errors += _check_payload(record["payload"], kind, "payload", record["event_type"])

    sharing = record.get("data_sharing")
    if sharing is not None:
        errors += _require_keys(sharing, ("visibility",), "data_sharing")
        if not errors:
            errors += _check_enum(sharing["visibility"], {"public", "partners", "restricted"}, "data_sharing.visibility")
            for i, partner in enumerate(sharing.get("allowed_partners", [])):
                errors += _check_enum(partner, OWNERS, f"data_sharing.allowed_partners[{i}]")
    return errors
