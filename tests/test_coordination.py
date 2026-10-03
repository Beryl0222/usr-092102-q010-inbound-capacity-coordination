import unittest

from src.coordination import (
    Commitment,
    Coordinator,
    CoordinationError,
    ProposalBuilder,
    RecoveryTracker,
    Sequence,
    trace_incident,
)
from src.event_store import EventStore, parse_dt
from src.monitoring import Projection
from src.scenario import load_zhangjiajie

CORR = "INC-20261003-WESTGATE-01"


class IncidentLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = load_zhangjiajie()

    def _tracker(self, tm: str) -> RecoveryTracker:
        now = parse_dt(f"2026-10-03T{tm}:00+08:00")
        return RecoveryTracker(self.store, Projection.build(self.store, as_of=now))

    def test_full_chain_is_traceable(self) -> None:
        chain_types = [e["event_type"] for e in trace_incident(self.store, CORR)]
        self.assertEqual(
            chain_types,
            ["PRESSURE_DETECTED", "DIVERSION_PROPOSED", "ACTION_CONFIRMED"],
        )
        # 因果链逐环可追
        pressure = self.store.get("zjj-pressure-01")
        self.assertEqual(pressure["caused_by"], "zjj-cap-shuttle-04")
        decision = self.store.get("zjj-decision-01")
        self.assertEqual(decision["caused_by"], "zjj-plan-01")

    def test_active_throttle_until_lifted(self) -> None:
        tracker = self._tracker("13:00")
        active = tracker.active_throttles()
        self.assertEqual([e["event_id"] for e in active], ["zjj-decision-01"])

    def test_recovery_requires_continuous_hold_then_lifts(self) -> None:
        throttle = self.store.get("zjj-decision-01")

        r_1320 = self._tracker("13:20").evaluate(throttle, parse_dt("2026-10-03T13:20:00+08:00"))
        self.assertFalse(r_1320["satisfied"])  # 余量 110 < 120

        r_1335 = self._tracker("13:35").evaluate(throttle, parse_dt("2026-10-03T13:35:00+08:00"))
        self.assertFalse(r_1335["satisfied"])  # 刚满足，未满 15 分钟
        self.assertIn("15", r_1335["reasons"][0])

        t_1350 = parse_dt("2026-10-03T13:50:00+08:00")
        tracker = self._tracker("13:50")
        r_1350 = tracker.evaluate(throttle, t_1350)
        self.assertTrue(r_1350["satisfied"])

        restored = tracker.restore(
            throttle, t_1350,
            confirmed_by={"owner": "transport", "role": "接驳运营值班经理", "party": "transport-duty-w03"},
            seq=Sequence("zjj-test"),
        )
        self.assertIsNotNone(restored)
        self.assertEqual(restored["event_type"], "NORMAL_SERVICE_RESTORED")
        self.assertEqual(restored["payload"]["restores_decision_event_id"], "zjj-decision-01")
        self.assertEqual(restored["correlation_id"], CORR)
        self.assertEqual(restored["aggregate_id"], throttle["aggregate_id"])
        self.assertEqual(restored["version"], 2)
        self.assertTrue(restored["payload"]["evaluation"]["satisfied"])

        # 解除后不再出现在生效限流中
        self.assertEqual(RecoveryTracker(self.store, Projection.build(self.store)).active_throttles(), [])

        # 端到端链条闭合
        chain = [e["event_type"] for e in trace_incident(self.store, CORR)]
        self.assertEqual(
            chain,
            ["PRESSURE_DETECTED", "DIVERSION_PROPOSED", "ACTION_CONFIRMED", "NORMAL_SERVICE_RESTORED"],
        )

    def test_cannot_lift_before_condition_met(self) -> None:
        throttle = self.store.get("zjj-decision-01")
        tracker = self._tracker("13:20")
        result = tracker.restore(
            throttle, parse_dt("2026-10-03T13:20:00+08:00"),
            confirmed_by={"owner": "transport", "role": "经理", "party": "p"},
            seq=Sequence("zjj-test"),
        )
        self.assertIsNone(result)


class AuthorityBoundaryTest(unittest.TestCase):
    def _pressure(self) -> dict:
        return self.store.get("zjj-pressure-01")

    def setUp(self) -> None:
        self.store = load_zhangjiajie()
        self.coordinator = Coordinator(self.store, Sequence("zjj-auth"))

    def test_system_cannot_confirm_operating_change(self) -> None:
        with self.assertRaises(CoordinationError):
            self.coordinator.confirm(
                decision="throttle_entry",
                confirmed_by={"owner": "transport", "role": "自动", "party": "coordination-system"},
                pressure_event=self._pressure(),
                recovery_condition={"metric_code": "h", "restore_below": 1, "hold_minutes": 5},
            )

    def test_responsibility_stays_with_owning_party(self) -> None:
        # 交通压力由医疗方确认属于跨权操作：领域层不伪造机构身份，
        # 这里校验 owner 必须是六类责任方之一
        decision = self.coordinator.confirm(
            decision="hold_no_change",
            confirmed_by={"owner": "transport", "role": "值班经理", "party": "transport-duty-w03"},
            pressure_event=self._pressure(),
        )
        self.assertEqual(decision["payload"]["decision"], "hold_no_change")

    def test_plan_protects_commitments_and_unknown_ref_rejected(self) -> None:
        plan = self.store.get("zjj-plan-01")
        with self.assertRaises(CoordinationError):
            self.coordinator.confirm(
                decision="reroute",
                confirmed_by={"owner": "transport", "role": "经理", "party": "transport-duty-w03"},
                pressure_event=self._pressure(),
                plan_event=plan,
                reschedule_commitments=["NOT-A-REAL-BOOKING"],
            )

    def test_builder_only_proposes(self) -> None:
        store = EventStore()
        pressure = {
            "event_id": "p-1", "event_type": "PRESSURE_DETECTED",
            "aggregate_type": "pressure_signal", "aggregate_id": "sig-1",
            "occurred_at": "2026-10-03T12:40:00+08:00", "version": 1,
            "summary": "接驳告急",
            "payload": {
                "owner": "transport", "pressure_kind": "shuttle_queue",
                "severity": "critical", "source_capacity_id": "cap-1",
            },
        }
        store.append(pressure)
        plan = ProposalBuilder(store, Sequence("t")).propose(
            pressure, commitments=[Commitment("BK-9", "large_group")],
            occurred_at=parse_dt("2026-10-03T12:42:00+08:00"),
        )
        self.assertEqual(plan["payload"]["proposed_by"], "coordination-system")
        self.assertGreaterEqual(len(plan["payload"]["options"]), 1)
        self.assertEqual(plan["payload"]["protected_commitments"][0]["handling"], "honor")
        # 建议本身不改变任何运营状态：尚无 operating_decision
        self.assertEqual(Projection.build(store).decisions, [])


if __name__ == "__main__":
    unittest.main()
