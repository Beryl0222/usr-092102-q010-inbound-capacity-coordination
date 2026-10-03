"""限流协同闭环。

一条限流的完整生命周期：

  CAPACITY_REPORTED（各机构，分离的五类信号）
        │ 安全余量耗尽 / 趋势外推即将失守
        ▼
  PRESSURE_DETECTED            ← 触发信号（局部瓶颈，非游客总量）
        │ 系统只可建议
        ▼
  DIVERSION_PROPOSED           ← 替代时段/线路/资源方案 + 受保护预约承诺
        │ 责任方审阅，可采纳、可拒绝、可暂不动作
        ▼
  ACTION_CONFIRMED(throttle_entry)  ← 协同决定（只能由相应责任方确认，必须带恢复条件）
        │ 持续观测，恢复条件需连续保持 hold_minutes
        ▼
  NORMAL_SERVICE_RESTORED(lift_throttle) ← 恢复（回填被解除的决定与核对结果）

所有事件共用 correlation_id、以 caused_by 串起因果链；任何已作出的预约承诺
都不会被新预测静默取消。
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .event_store import EventStore, parse_dt
from .monitoring import Projection

_TZ = "+08:00"

# 各类局部压力的系统建议库：系统只给方案，不执行运营变更
_OPTION_LIBRARY: dict[str, list[dict]] = {
    "shuttle_queue": [
        {"option_type": "alternative_time_window", "description": "建议后续游客改约 60 分钟后的接驳班次，错峰上山"},
        {"option_type": "alternative_route", "description": "引导至东门备用接驳线路 B，绕开饱和站点"},
        {"option_type": "alternative_resource", "description": "建议临时增开 2 台接驳车（需交通方确认调派）", "estimated_relief_pct": 35},
    ],
    "card_terminal_saturation": [
        {"option_type": "alternative_resource", "description": "建议调派 1 台移动外卡受理终端至该退税商店（需商店/支付方确认）"},
        {"option_type": "alternative_time_window", "description": "引导持外卡游客错开 14:00-15:00 高峰结算"},
    ],
    "multilingual_first_aid": [
        {"option_type": "alternative_resource", "description": "建议召集第二组多语急救人员上岗（需医疗点值班长确认）"},
        {"option_type": "alternative_route", "description": "将非急诊多语咨询引导至山下医疗点分站"},
    ],
    "ticket_vs_onsite_mismatch": [
        {"option_type": "alternative_time_window", "description": "对尚未入园游客推送更晚分时入园建议（改签须游客同意）"},
        {"option_type": "alternative_route", "description": "开放东线分流，降低核心区实时占用"},
    ],
    "lodging_absorption": [
        {"option_type": "alternative_resource", "description": "协调远端协议酒店房源（需住宿方确认）"},
    ],
    "tax_refund_congestion": [
        {"option_type": "alternative_time_window", "description": "引导退税办理分散至非高峰时段"},
        {"option_type": "alternative_resource", "description": "建议增设临时退税核验通道（需商店确认）"},
    ],
    "community_road": [
        {"option_type": "alternative_route", "description": "启用游客绕行外环路线，释放社区道路给居民出行"},
    ],
}

# 限流类决定与恢复事件的生命周期配对
_THROTTLE_DECISIONS = {"throttle_entry"}


@dataclass
class Commitment:
    """一条已作出的预约承诺（团队或散客）。"""

    commitment_ref: str
    holder_kind: str  # large_group / independent_traveler

    def as_protected(self) -> dict:
        return {"commitment_ref": self.commitment_ref, "holder_kind": self.holder_kind, "handling": "honor"}


class CoordinationError(ValueError):
    pass


class ProposalBuilder:
    """根据 PRESSURE_DETECTED 生成 DIVERSION_PROPOSED。proposed_by 恒为系统。"""

    def __init__(self, store: EventStore, seq: "Sequence"):
        self.store = store
        self.seq = seq

    def propose(
        self,
        pressure_event: dict,
        commitments: list[Commitment] | None = None,
        occurred_at: datetime | None = None,
    ) -> dict:
        if pressure_event["event_type"] != "PRESSURE_DETECTED":
            raise CoordinationError("只能针对 PRESSURE_DETECTED 提出方案")
        kind = pressure_event["payload"]["pressure_kind"]
        options = _OPTION_LIBRARY.get(kind)
        if not options:
            raise CoordinationError(f"未知压力类型，无法生成建议：{kind}")
        event = {
            "event_id": self.seq.next_id("div"),
            "event_type": "DIVERSION_PROPOSED",
            "aggregate_type": "diversion_plan",
            "aggregate_id": f"plan-{pressure_event['aggregate_id']}",
            "occurred_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
            "version": 1,
            "summary": f"系统针对 {kind} 提出分流建议，待责任方确认",
            "caused_by": pressure_event["event_id"],
            "payload": {
                "proposed_by": "coordination-system",
                "pressure_event_id": pressure_event["event_id"],
                "options": options,
                "protected_commitments": [c.as_protected() for c in (commitments or [])],
            },
        }
        if pressure_event.get("correlation_id"):
            event["correlation_id"] = pressure_event["correlation_id"]
        return self.store.append(event).event


class Sequence:
    """测试/联调用的简单序号生成器。"""

    def __init__(self, prefix: str = "zjj"):
        self.prefix = prefix
        self._n = 0

    def next_id(self, kind: str) -> str:
        self._n += 1
        return f"{self.prefix}-{kind}-{self._n:04d}"


class Coordinator:
    """责任方决定的入口：校验权限、因果链与承诺保护。"""

    def __init__(self, store: EventStore, seq: Sequence):
        self.store = store
        self.seq = seq

    def confirm(
        self,
        *,
        decision: str,
        confirmed_by: dict,
        pressure_event: dict,
        plan_event: dict | None = None,
        scope: str = "",
        recovery_condition: dict | None = None,
        occurred_at: datetime | None = None,
        reschedule_commitments: list[str] | None = None,
    ) -> dict:
        if confirmed_by.get("party") == "coordination-system":
            raise CoordinationError("协调系统不得自行确认运营变更")
        if decision == "throttle_entry" and recovery_condition is None:
            raise CoordinationError("限流必须附带恢复条件")

        payload: dict = {
            "decision": decision,
            "confirmed_by": confirmed_by,
            "pressure_event_id": pressure_event["event_id"],
            "plan_event_id": plan_event["event_id"] if plan_event else None,
            "scope": scope,
        }
        if recovery_condition is not None:
            payload["recovery_condition"] = recovery_condition

        # 承诺保护：只有计划里显式列出且经持有人同意的承诺可以 offer_reschedule…，
        # 任何决定都不允许出现‘取消’语义。受保护清单随方案事件留存（方案校验拦截
        # 非法 handling），决定事件只引用 plan_event_id，不复制个人预约信息。
        if plan_event is not None and decision in ("throttle_entry", "reroute"):
            protected = {c["commitment_ref"]: c for c in plan_event["payload"].get("protected_commitments", [])}
            for ref in reschedule_commitments or []:
                if ref not in protected:
                    raise CoordinationError(f"承诺 {ref} 不在受保护清单中，禁止借限流之名变更预约")

        event = {
            "event_id": self.seq.next_id("dec"),
            "event_type": "ACTION_CONFIRMED",
            "aggregate_type": "operating_decision",
            "aggregate_id": f"throttle-{pressure_event['aggregate_id']}",
            "occurred_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
            "version": 1,
            "summary": f"{confirmed_by['owner']} 责任方确认：{decision}",
            "caused_by": plan_event["event_id"] if plan_event else pressure_event["event_id"],
            "payload": payload,
        }
        if pressure_event.get("correlation_id"):
            event["correlation_id"] = pressure_event["correlation_id"]
        return self.store.append(event).event


class RecoveryTracker:
    """根据容量事件流评估各限流的恢复条件是否已连续满足。"""

    def __init__(self, store: EventStore, projection: Projection):
        self.store = store
        self.projection = projection

    def active_throttles(self) -> list[dict]:
        by_corr: dict[str, list[dict]] = {}
        for event in self.projection.decisions:
            corr = event.get("correlation_id")
            if corr:
                by_corr.setdefault(corr, []).append(event)
        active = []
        for corr, events in by_corr.items():
            throttles = [e for e in events if e["event_type"] == "ACTION_CONFIRMED"
                         and e["payload"]["decision"] in _THROTTLE_DECISIONS]
            lifts = [e for e in events if e["event_type"] == "NORMAL_SERVICE_RESTORED"]
            if throttles and not lifts:
                active.append(throttles[-1])
        return active

    def evaluate(self, throttle_event: dict, now: datetime) -> dict:
        """返回 {satisfied, observed_value, threshold, held_since, reasons[]}。

        held_since 取‘最后一次不满足的观测点之后’的首个满足时刻；要求从那时起
        每个观测点都满足，且至 now 已持续 hold_minutes。
        """
        rc = throttle_event["payload"]["recovery_condition"]
        pressure = self.store.get(throttle_event["payload"]["pressure_event_id"])
        source_id = rc.get("source_capacity_id") or pressure["payload"]["source_capacity_id"]
        state = self.projection.capacities.get(source_id)
        reasons: list[str] = []
        if state is None:
            return {"satisfied": False, "reasons": [f"找不到恢复指标来源 {source_id}"]}

        field_name = rc.get("observed_field", "realtime_occupancy")
        comparator = rc.get("comparator", "below")
        threshold = rc["restore_below"]
        points = state.field_series(field_name, metric_code=rc["metric_code"])
        if not points:
            return {"satisfied": False, "reasons": [f"{source_id} 暂无 {rc['metric_code']}.{field_name} 观测"]}

        def holds(value: float) -> bool:
            return value < float(threshold) if comparator == "below" else value > float(threshold)

        held_since: datetime | None = None
        for ts, value in points:
            if holds(value):
                if held_since is None:
                    held_since = ts
            else:
                held_since = None  # 中途失守，重新计时
        latest_ts, latest_value = points[-1]
        if held_since is None:
            return {
                "satisfied": False,
                "observed_value": latest_value,
                "threshold": threshold,
                "reasons": [f"最新观测 {latest_value:g} 未满足 {comparator} {threshold}"],
            }

        held_minutes = (now - held_since).total_seconds() / 60.0
        required = rc["hold_minutes"]
        if held_minutes + 1e-9 < required:
            return {
                "satisfied": False,
                "observed_value": latest_value,
                "threshold": threshold,
                "held_since": held_since.isoformat(),
                "reasons": [f"条件已保持 {held_minutes:.0f} 分钟，需连续保持 {required} 分钟"],
            }

        # 附加条件，例如社区影响降回 none
        for clause in rc.get("also_require", []):
            key, _, wanted = clause.partition(":")
            current = {
                "community_impact": state.community_level,
                "service_degradation": state.service_level,
            }.get(key)
            if current is not None and current != wanted:
                reasons.append(f"附加条件 {clause} 未满足（当前 {current}）")
        if reasons:
            return {"satisfied": False, "observed_value": latest_value, "threshold": threshold,
                    "held_since": held_since.isoformat(), "reasons": reasons}

        return {
            "satisfied": True,
            "observed_value": latest_value,
            "threshold": threshold,
            "held_since": held_since.isoformat(),
            "reasons": [],
        }

    def restore(self, throttle_event: dict, now: datetime, confirmed_by: dict, seq: Sequence) -> dict | None:
        """满足恢复条件时生成 NORMAL_SERVICE_RESTORED；否则返回 None。"""
        result = self.evaluate(throttle_event, now)
        if not result["satisfied"]:
            return None
        event = {
            "event_id": seq.next_id("rst"),
            "event_type": "NORMAL_SERVICE_RESTORED",
            "aggregate_type": "operating_decision",
            "aggregate_id": throttle_event["aggregate_id"],
            "occurred_at": now.isoformat(),
            "version": throttle_event["version"] + 1,
            "summary": "恢复条件连续满足，解除限流",
            "correlation_id": throttle_event.get("correlation_id"),
            "caused_by": throttle_event["event_id"],
            "data_sharing": throttle_event.get("data_sharing", {"visibility": "restricted",
                                                                 "purpose": "current_safeguard_only"}),
            "payload": {
                "decision": "lift_throttle",
                "confirmed_by": confirmed_by,
                "pressure_event_id": throttle_event["payload"]["pressure_event_id"],
                "plan_event_id": throttle_event["payload"].get("plan_event_id"),
                "restores_decision_event_id": throttle_event["event_id"],
                "evaluation": {
                    "observed_value": result["observed_value"],
                    "threshold": result["threshold"],
                    "held_since": result["held_since"],
                    "satisfied": True,
                },
            },
        }
        return self.store.append(event).event


def trace_incident(store: EventStore, correlation_id: str) -> list[dict]:
    """端到端追溯一次限流：返回因果链上的事件（按事件时间）。"""
    chain = [
        s.event
        for s in store.replay()
        if s.event.get("correlation_id") == correlation_id
    ]
    return chain
