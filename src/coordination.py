"""跨机构协同层。

治理边界：
- 引擎只“提案”（DIVERSION_PROPOSED）：替代时段/线路/资源；
- 任何关闭入口、改变运营的动作必须由对应责任方确认（ACTION_CONFIRMED），
  责任方也可以拒绝；
- 既有预约承诺只能 honor 或 offer_choice，引擎不提供“取消”原语；
- 动作生效时带恢复条件，恢复需要证据（NORMAL_SERVICE_RESTORED）；
- correlation_id 把触发信号 → 提案 → 决定 → 恢复串成可追全链路。
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass

from .eventstore import EventStore, StoredEvent
from .model import CapacityBoard, PressureAnalyzer, ForecastFactors
from .timeutil import minutes_between, parse_ts, shift

OPERATOR_CHECKS = {
    "lte": lambda a, b: a <= b,
    "lt": lambda a, b: a < b,
    "gte": lambda a, b: a >= b,
    "gt": lambda a, b: a > b,
    "eq": lambda a, b: a == b,
}

SEVERITY_RANK = {"watch": 0, "warning": 1, "critical": 2}

# 确定性 id：进程内按前缀计数，便于联调数据稳定复现；
# 生产部署可替换为 uuid/雪花算法，事件身份语义不变。
_id_counters: dict[str, itertools.count] = {}


def reset_id_sequence() -> None:
    _id_counters.clear()


def new_id(prefix: str) -> str:
    counter = _id_counters.setdefault(prefix, itertools.count(1))
    return f"{prefix}-{next(counter):04d}"


# ---------------------------------------------------------------------------
# 事件构造助手
# ---------------------------------------------------------------------------

def capacity_event(
    *,
    source_aggregate_id: str,
    org_id: str,
    occurred_at: str,
    window: dict,
    scope: dict,
    readings: list[dict],
    version: int,
    received_at: str | None = None,
    ingest: str = "live",
    sensor_id: str | None = None,
    outage: dict | None = None,
    data_categories: list[str] | None = None,
    confidence: str | None = None,
) -> dict:
    payload: dict = {"scope": scope, "window": window, "readings": readings}
    if data_categories:
        payload["data_categories"] = data_categories
    event = {
        "event_id": new_id("evt"),
        "event_type": "CAPACITY_REPORTED",
        "aggregate_type": "capacity_source",
        "aggregate_id": source_aggregate_id,
        "occurred_at": occurred_at,
        "received_at": received_at or occurred_at,
        "version": version,
        "summary": f"{scope.get('label', scope['source_id'])} 容量读数",
        "payload": payload,
        "source": {"org_id": org_id, "ingest": ingest},
    }
    if sensor_id:
        event["source"]["sensor_id"] = sensor_id
    if outage:
        event["source"]["outage"] = outage
    if confidence:
        event["confidence"] = confidence
    return event


def _signal_aggregate_id(source_id: str, metric: str) -> str:
    return f"pressure:{source_id}:{metric}"


# ---------------------------------------------------------------------------
# 分流策略
# ---------------------------------------------------------------------------

@dataclass
class ResourceOption:
    kind: str
    target_id: str
    label: str
    relief_pct: float
    confidence_level: str = "medium"
    confidence_reason: str = ""


# 各局部瓶颈默认的替代方向；现场资源目录可覆盖/补充具体 target。
DEFAULT_POLICY: dict[tuple[str, str], dict] = {
    ("transport_shuttle", "queue_length_minutes"): {
        "resource_kinds": ["shuttle_bus"],
        "route": ("route-pianzhen", "经偏镇路备用接驳线"),
        "time_slot_offset": 45,
    },
    ("transport_shuttle", "wait_time_minutes"): {
        "resource_kinds": ["shuttle_bus"],
        "route": ("route-pianzhen", "经偏镇路备用接驳线"),
        "time_slot_offset": 40,
    },
    ("scenic_area_entry", "occupancy_rate"): {
        "resource_kinds": ["entry_gate"],
        "time_slot_offset": 60,
    },
    ("tax_refund_shop", "infrastructure_saturation"): {
        "resource_kinds": ["foreign_card_pos", "staffing"],
        "time_slot_offset": 30,
    },
    ("tax_refund_shop", "wait_time_minutes"): {
        "resource_kinds": ["foreign_card_pos"],
        "time_slot_offset": 30,
    },
    ("medical_point", "wait_time_minutes"): {
        "resource_kinds": ["multilingual_first_aid_team"],
    },
    ("medical_point", "staff_idle_ratio"): {
        "resource_kinds": ["multilingual_first_aid_team"],
    },
    ("community_road", "resident_road_impact_level"): {
        "route": ("route-freight-bypass", "货运外环分流线（保居民通道）"),
        "resource_kinds": ["road_lane"],
    },
}


class DiversionEngine:
    """根据压力信号生成替代方案。引擎没有关闭入口、取消预约的能力。"""

    def __init__(
        self,
        store: EventStore,
        *,
        resource_catalog: dict[str, list[ResourceOption]] | None = None,
        proposed_by: str = "coordination-engine:v1",
    ) -> None:
        self.store = store
        self.resource_catalog = resource_catalog or {}
        self.proposed_by = proposed_by

    def detect_pressure(
        self,
        moment: str,
        *,
        correlation_id: str,
        forecast: ForecastFactors | None = None,
        board: CapacityBoard | None = None,
    ) -> list[StoredEvent]:
        """运行分析并把压力信号持久化为 PRESSURE_DETECTED 事件。"""
        board = board or CapacityBoard(self.store.as_of_occurred(moment))
        analyzer = PressureAnalyzer(board)
        pressures = analyzer.analyze(moment, forecast=forecast)
        stored: list[StoredEvent] = []
        for p in pressures:
            aggregate_id = _signal_aggregate_id(p.source_id, p.metric)
            payload = {
                "scope": {"dimension": p.dimension, "source_id": p.source_id, "label": p.label},
                "severity": p.severity,
                "window": p.window,
                "based_on": p.based_on,
                "bottleneck_metric": p.metric,
                "observed_value": p.observed,
                "threshold_value": p.threshold_critical,
                "minutes_to_breach": p.minutes_to_breach,
                "confidence": p.confidence,
                "message": p.message,
            }
            event = {
                "event_id": new_id("sig"),
                "event_type": "PRESSURE_DETECTED",
                "aggregate_type": "pressure_signal",
                "aggregate_id": aggregate_id,
                "occurred_at": moment,
                "received_at": moment,
                "version": self.store.next_version(aggregate_id),
                "summary": f"{p.label or p.source_id} {p.metric} {p.severity}",
                "correlation_id": correlation_id,
                "confidence": p.confidence,
                "payload": payload,
            }
            stored.append(self.store.append(event))
        return stored

    def propose(
        self,
        moment: str,
        *,
        correlation_id: str,
        trigger_events: list[StoredEvent],
        forecast: ForecastFactors | None = None,
        reservation_options: list[dict] | None = None,
    ) -> StoredEvent:
        """对一组触发信号提出替代时段/线路/资源方案。"""
        if not trigger_events:
            raise ValueError("提案至少需要一个触发信号")

        time_slots: dict[str, dict] = {}
        routes: dict[str, dict] = {}
        resources: dict[str, dict] = {}

        for signal in trigger_events:
            payload = signal.record["payload"]
            dimension = payload["scope"]["dimension"]
            key = (dimension, payload["bottleneck_metric"])
            policy = DEFAULT_POLICY.get(key)
            if policy is None:
                continue
            if "time_slot_offset" in policy:
                window = {
                    "start": shift(moment, policy["time_slot_offset"]),
                    "end": shift(moment, policy["time_slot_offset"] + 90),
                }
                marker = window["start"]
                slot = time_slots.setdefault(
                    marker,
                    {
                        "window": window,
                        "estimated_relief_pct": self._relief(payload["severity"]),
                        "confidence": {"level": "medium", "reason": "基于同类日历史分流曲线"},
                        "applies_to_dimensions": [],
                    },
                )
                if dimension not in slot["applies_to_dimensions"]:
                    slot["applies_to_dimensions"].append(dimension)
            if "route" in policy:
                route_id, label = policy["route"]
                route = routes.setdefault(
                    route_id,
                    {
                        "route_id": route_id,
                        "label": label,
                        "estimated_relief_pct": self._relief(payload["severity"]),
                        "confidence": {"level": "medium", "reason": "备用线路当前占用低于四成"},
                        "applies_to_dimensions": [],
                    },
                )
                if dimension not in route["applies_to_dimensions"]:
                    route["applies_to_dimensions"].append(dimension)
            for kind in policy.get("resource_kinds", []):
                for option in self._lookup_resources(payload["scope"]["source_id"], kind):
                    res = resources.setdefault(
                        option.target_id,
                        {
                            "resource": {
                                "kind": option.kind,
                                "target_id": option.target_id,
                                "label": option.label,
                            },
                            "estimated_relief_pct": option.relief_pct,
                            "confidence": {
                                "level": option.confidence_level,
                                "reason": option.confidence_reason,
                            },
                            "applies_to_dimensions": [],
                        },
                    )
                    if dimension not in res["applies_to_dimensions"]:
                        res["applies_to_dimensions"].append(dimension)

        if not (time_slots or routes or resources):
            raise ValueError("没有可生成的替代方向，需补充策略或资源目录")

        proposal_payload: dict = {
            "trigger_signal_ids": [e.event_id for e in trigger_events],
            "alternatives": {
                "time_slots": list(time_slots.values()),
                "routes": list(routes.values()),
                "resources": list(resources.values()),
            },
            "proposed_by": self.proposed_by,
        }
        if forecast is not None:
            factors: dict = {
                "group_arrivals": forecast.group_arrivals,
                "independent_arrivals": forecast.independent_arrivals,
            }
            if forecast.visa_policy_notes:
                factors["visa_free_policies"] = [
                    {
                        "policy_id": "visa-free-240h",
                        "change": "extended",
                        "effective_at": moment,
                        "confidence": {"level": forecast.confidence, "reason": forecast.visa_policy_notes},
                    }
                ]
            proposal_payload["forecast_factors"] = factors
        if reservation_options:
            # 只有两种处理原则；引擎结构上无法表达 silent_cancel。
            proposal_payload["affected_reservations"] = [
                {"count": int(o["count"]), "treatment": o["treatment"]}
                for o in reservation_options
                if o["treatment"] in ("honor", "offer_choice")
            ]

        aggregate_id = f"plan:{correlation_id}"
        event = {
            "event_id": new_id("plan"),
            "event_type": "DIVERSION_PROPOSED",
            "aggregate_type": "diversion_plan",
            "aggregate_id": aggregate_id,
            "occurred_at": moment,
            "received_at": moment,
            "version": self.store.next_version(aggregate_id),
            "summary": f"针对 {len(trigger_events)} 个瓶颈信号的分流方案（仅建议）",
            "correlation_id": correlation_id,
            "causation_id": trigger_events[0].event_id,
            "payload": proposal_payload,
        }
        return self.store.append(event)

    def _lookup_resources(self, source_id: str, kind: str) -> list[ResourceOption]:
        options = list(self.resource_catalog.get(f"{source_id}:{kind}", []))
        options += self.resource_catalog.get(f"*:{kind}", [])
        return options

    @staticmethod
    def _relief(severity: str) -> float:
        return {"critical": 35.0, "warning": 25.0, "watch": 15.0}.get(severity, 20.0)


# ---------------------------------------------------------------------------
# 责任方确认
# ---------------------------------------------------------------------------

def confirm_action(
    store: EventStore,
    moment: str,
    *,
    correlation_id: str,
    proposal: StoredEvent,
    action_kind: str,
    scope_ids: list[str],
    window: dict,
    org_id: str,
    role: str,
    authority: str,
    person_ref: str | None = None,
    reservation_commitment_policy: str = "honor_all",
    recovery_conditions: list[dict] | None = None,
    rejected_reason: str | None = None,
) -> StoredEvent:
    """责任方确认（或拒绝）运营动作。未确认前现场不发生任何改变。"""
    if proposal.event_type != "DIVERSION_PROPOSED":
        raise ValueError("决定必须针对一份 DIVERSION_PROPOSED")
    if not rejected_reason and not recovery_conditions:
        raise ValueError("限流类动作必须在确认时同时设定恢复条件")

    aggregate_id = f"decision:{correlation_id}"
    confirmed_by = {"org_id": org_id, "role": role}
    if person_ref:
        confirmed_by["person_ref"] = person_ref
    payload: dict = {
        "proposal_id": proposal.event_id,
        "action": {"kind": action_kind, "scope_ids": scope_ids, "window": window},
        "confirmed_by": confirmed_by,
        "authority": authority,
        "decision_basis": {
            "signal_ids": proposal.record["payload"]["trigger_signal_ids"],
            "proposal_id": proposal.event_id,
        },
        "reservation_commitment_policy": reservation_commitment_policy,
    }
    if recovery_conditions:
        payload["recovery_conditions"] = recovery_conditions
    if rejected_reason:
        payload["rejected"] = {"reason": rejected_reason}

    event = {
        "event_id": new_id("dec"),
        "event_type": "ACTION_CONFIRMED",
        "aggregate_type": "operating_decision",
        "aggregate_id": aggregate_id,
        "occurred_at": moment,
        "received_at": moment,
        "version": store.next_version(aggregate_id),
        "summary": ("拒绝分流提案" if rejected_reason else f"{org_id} 确认 {action_kind}") + f"（{correlation_id}）",
        "correlation_id": correlation_id,
        "causation_id": proposal.event_id,
        "payload": payload,
    }
    return store.append(event)


# ---------------------------------------------------------------------------
# 恢复条件评估
# ---------------------------------------------------------------------------

@dataclass
class ConditionStatus:
    condition_id: str
    satisfied: bool
    observed: float | None
    detail: str


class RecoveryEvaluator:
    """根据最新容量读数核对某决定的恢复条件。

    满足要求：条件窗内有读数、比较成立、且最近 sustain_minutes 内
    的同指标读数全部成立（防止单点抖动误恢复）。
    """

    def __init__(self, board: CapacityBoard) -> None:
        self.board = board

    def evaluate(self, decision: StoredEvent, moment: str) -> list[ConditionStatus]:
        payload = decision.record["payload"]
        statuses: list[ConditionStatus] = []
        for cond in payload.get("recovery_conditions", []):
            check = OPERATOR_CHECKS[cond["operator"]]
            readings = self._matching_readings(cond, payload["action"]["scope_ids"])
            if not readings:
                statuses.append(ConditionStatus(cond["condition_id"], False, None, "条件窗内暂无读数"))
                continue
            latest = readings[-1]
            if not check(latest.value, cond["threshold"]):
                statuses.append(
                    ConditionStatus(
                        cond["condition_id"], False, latest.value,
                        f"最新 {latest.value} 未满足 {cond['operator']} {cond['threshold']}",
                    )
                )
                continue
            sustain = cond.get("sustain_minutes", 1)
            moment_dt = parse_ts(moment)
            satisfying = [
                r for r in readings
                if parse_ts(r.occurred_at) <= moment_dt and check(r.value, cond["threshold"])
            ]
            if not satisfying:
                statuses.append(
                    ConditionStatus(
                        cond["condition_id"], False, latest.value,
                        f"最新 {latest.value} 未满足 {cond['operator']} {cond['threshold']}",
                    )
                )
                continue
            earliest = min(satisfying, key=lambda r: parse_ts(r.occurred_at))
            later_violation = any(
                parse_ts(r.occurred_at) >= parse_ts(earliest.occurred_at)
                and not check(r.value, cond["threshold"])
                for r in readings
                if parse_ts(r.occurred_at) <= moment_dt
            )
            held_minutes = minutes_between(earliest.occurred_at, moment)
            if later_violation:
                statuses.append(
                    ConditionStatus(cond["condition_id"], False, latest.value, "达标后出现反复，持续时间重新计算")
                )
            elif held_minutes + 1e-9 >= sustain:
                statuses.append(
                    ConditionStatus(
                        cond["condition_id"], True, latest.value,
                        f"{latest.value} {cond['operator']} {cond['threshold']}，已持续达标约 {held_minutes:.0f} 分钟",
                    )
                )
            else:
                statuses.append(
                    ConditionStatus(
                        cond["condition_id"], False, latest.value,
                        f"已达标但仅持续约 {held_minutes:.0f} 分钟，需满 {sustain} 分钟",
                    )
                )
        return statuses

    def _matching_readings(self, cond: dict, scope_ids: list[str]):
        source_ids = cond.get("evidence_source_ids") or scope_ids
        result = []
        for source_id in source_ids:
            state = self.board.sources.get(source_id)
            if state is None:
                continue
            for reading in state.history.get(cond["metric"], []):
                if parse_ts(reading.window_start) <= parse_ts(cond["window"]["end"]) and \
                        parse_ts(reading.window_end) >= parse_ts(cond["window"]["start"]):
                    result.append(reading)
        result.sort(key=lambda r: parse_ts(r.window_end))
        return result

    def restore(
        self,
        store: EventStore,
        moment: str,
        *,
        decision: StoredEvent,
        resolved_by: dict,
        residual_risk_level: str = "low",
        notes: str = "",
    ) -> StoredEvent | None:
        """全部恢复条件满足时，追加 NORMAL_SERVICE_RESTORED；否则返回 None。"""
        statuses = self.evaluate(decision, moment)
        if not statuses or not all(s.satisfied for s in statuses):
            return None

        payload = decision.record["payload"]
        evidence = []
        for cond, status in zip(payload["recovery_conditions"], statuses):
            readings = self._matching_readings(cond, payload["action"]["scope_ids"])
            latest = readings[-1]
            evidence.append(
                {
                    "condition_id": cond["condition_id"],
                    "metric": cond["metric"],
                    "observed": status.observed,
                    "operator": cond["operator"],
                    "threshold": cond["threshold"],
                    "window": {"start": latest.window_start, "end": latest.window_end},
                    "confidence": latest.confidence,
                }
            )
        correlation_id = decision.record["correlation_id"]
        aggregate_id = decision.aggregate_id
        restore_payload = {
            "decision_id": decision.event_id,
            "evidence": evidence,
            "residual_risk_level": residual_risk_level,
            "resolved_by": resolved_by,
        }
        if notes:
            restore_payload["notes"] = notes
        event = {
            "event_id": new_id("rst"),
            "event_type": "NORMAL_SERVICE_RESTORED",
            "aggregate_type": "operating_decision",
            "aggregate_id": aggregate_id,
            "occurred_at": moment,
            "received_at": moment,
            "version": store.next_version(aggregate_id),
            "summary": f"限流解除、恢复常态（{correlation_id}）",
            "correlation_id": correlation_id,
            "causation_id": decision.event_id,
            "payload": restore_payload,
        }
        return store.append(event)


# ---------------------------------------------------------------------------
# 全链路追踪
# ---------------------------------------------------------------------------

class CoordinationTrace:
    """一次限流：触发信号 → 协同提案 → 责任方决定 → 恢复条件 → 恢复。"""

    def __init__(self, store: EventStore) -> None:
        self.store = store

    def trace(self, correlation_id: str, *, board: CapacityBoard | None = None, moment: str | None = None) -> dict:
        events = self.store.by_correlation(correlation_id)
        # 时点视图：只保留 moment 之前已经发生的链路事件
        # （10:40 的公众页看不到 11:40 才发生的恢复事件）。
        if moment is not None:
            moment_dt = parse_ts(moment)
            events = [e for e in events if parse_ts(e.occurred_at) <= moment_dt]
        signals, proposals, decisions, restorations = [], [], [], []
        for e in events:
            if e.event_type == "PRESSURE_DETECTED":
                signals.append(e)
            elif e.event_type == "DIVERSION_PROPOSED":
                proposals.append(e)
            elif e.event_type == "ACTION_CONFIRMED":
                decisions.append(e)
            elif e.event_type == "NORMAL_SERVICE_RESTORED":
                restorations.append(e)

        decision_view = []
        active_actions = []
        for d in decisions:
            rejected = "rejected" in d.record["payload"]
            conditions = []
            evaluator = RecoveryEvaluator(board) if board else None
            for cond in d.record["payload"].get("recovery_conditions", []):
                status = None
                if evaluator is not None and moment is not None:
                    for s in evaluator.evaluate(d, moment):
                        if s.condition_id == cond["condition_id"]:
                            status = s
                conditions.append(
                    {
                        "condition_id": cond["condition_id"],
                        "metric": cond["metric"],
                        "operator": cond["operator"],
                        "threshold": cond["threshold"],
                        "window": cond["window"],
                        "sustain_minutes": cond.get("sustain_minutes", 1),
                        "current": None
                        if status is None
                        else {"satisfied": status.satisfied, "observed": status.observed, "detail": status.detail},
                    }
                )
            restored = any(r.record["payload"]["decision_id"] == d.event_id for r in restorations)
            item = {
                "decision_event_id": d.event_id,
                "action": d.record["payload"]["action"]["kind"],
                "scope_ids": d.record["payload"]["action"]["scope_ids"],
                "window": d.record["payload"]["action"]["window"],
                "confirmed_by": d.record["payload"]["confirmed_by"],
                "rejected": rejected,
                "recovery_conditions": conditions,
                "restored": restored,
            }
            decision_view.append(item)
            if not rejected and not restored:
                active_actions.append(item)

        if restorations:
            status = "restored"
        elif any(not d["rejected"] for d in decision_view):
            status = "in_effect"
        elif decisions:
            status = "rejected"
        elif proposals:
            status = "proposed_awaiting_responsible_party"
        elif signals:
            status = "triggered"
        else:
            status = "unknown"

        return {
            "correlation_id": correlation_id,
            "status": status,
            "stages": {
                "trigger_signals": [self._signal_view(e) for e in signals],
                "proposals": [self._proposal_view(e) for e in proposals],
                "decisions": decision_view,
                "restorations": [self._restoration_view(e) for e in restorations],
            },
            "active_actions": active_actions,
            "timeline": [
                {
                    "at": e.occurred_at,
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "causation_id": e.record.get("causation_id"),
                    "ingest": e.record.get("source", {}).get("ingest", "live")
                    if e.event_type == "CAPACITY_REPORTED"
                    else "live",
                    "summary": e.record["summary"],
                }
                for e in events
            ],
        }

    @staticmethod
    def _signal_view(e: StoredEvent) -> dict:
        p = e.record["payload"]
        return {
            "event_id": e.event_id,
            "at": e.occurred_at,
            "dimension": p["scope"]["dimension"],
            "source_id": p["scope"]["source_id"],
            "metric": p["bottleneck_metric"],
            "severity": p["severity"],
            "observed_value": p["observed_value"],
            "minutes_to_breach": p.get("minutes_to_breach"),
            "confidence": p.get("confidence"),
            "based_on": p["based_on"],
        }

    @staticmethod
    def _proposal_view(e: StoredEvent) -> dict:
        p = e.record["payload"]
        return {
            "event_id": e.event_id,
            "at": e.occurred_at,
            "alternatives": p["alternatives"],
            "reservation_policy": p.get("affected_reservations", []),
            "forecast_factors": p.get("forecast_factors"),
        }

    @staticmethod
    def _restoration_view(e: StoredEvent) -> dict:
        p = e.record["payload"]
        return {
            "event_id": e.event_id,
            "at": e.occurred_at,
            "decision_id": p["decision_id"],
            "evidence": p["evidence"],
            "residual_risk_level": p.get("residual_risk_level"),
        }
