import unittest

from src.event_store import parse_dt
from src.monitoring import DutyBoard, Projection
from src.scenario import load_zhangjiajie


class MonitoringTest(unittest.TestCase):
    def test_tickets_available_while_local_bottlenecks_breach(self) -> None:
        """核心诉求：余票数字不能掩盖局部瓶颈。"""
        store = load_zhangjiajie()
        proj = Projection.build(store, known_at=parse_dt("2026-10-03T12:39:00+08:00"))
        board = DutyBoard(proj, parse_dt("2026-10-03T12:39:00+08:00"))
        by_id = {s.state.aggregate_id: s for s in board.service_statuses()}

        # 门票仍有千余张……
        self.assertGreater(by_id["cap-tickets-main"].state.sellable_quantity, 1000)
        # ……但接驳、急救、社区道路各自失守或预警
        self.assertEqual(by_id["cap-firstaid-i18n"].severity, "critical")
        self.assertEqual(by_id["cap-shuttle-west"].severity, "breach")
        self.assertEqual(by_id["cap-community-road-west"].severity, "breach")

    def test_projected_warning_before_headroom_hits_zero(self) -> None:
        store = load_zhangjiajie()
        proj = Projection.build(store, known_at=parse_dt("2026-10-03T12:39:00+08:00"))
        board = DutyBoard(proj, parse_dt("2026-10-03T12:39:00+08:00"))
        core = next(s for s in board.service_statuses() if s.state.aggregate_id == "cap-onsite-core")
        self.assertEqual(core.severity, "projected_watch")
        self.assertIsNotNone(core.eta_minutes)
        self.assertLessEqual(core.eta_minutes, 30)
        self.assertTrue(core.projected)

    def test_backfill_changes_picture_at_1240(self) -> None:
        store = load_zhangjiajie()
        before = DutyBoard(
            Projection.build(store, known_at=parse_dt("2026-10-03T12:39:00+08:00")),
            parse_dt("2026-10-03T12:39:00+08:00"),
        )
        card_before = next(s for s in before.service_statuses()
                           if s.state.aggregate_id == "cap-card-terminal-07")
        self.assertEqual(card_before.severity, "stale")  # 断网期间：陈旧而非正常

        after = DutyBoard(
            Projection.build(store),
            parse_dt("2026-10-03T12:40:00+08:00"),
        )
        card_after = next(s for s in after.service_statuses()
                          if s.state.aggregate_id == "cap-card-terminal-07")
        self.assertEqual(card_after.severity, "critical")
        self.assertEqual(card_after.state.safety_headroom, 3)

    def test_five_signal_kinds_stay_separated(self) -> None:
        store = load_zhangjiajie()
        # 12:40 时点：接驳容量本身在运转（无服务降级），但居民影响已 disrupted
        proj = Projection.build(store, as_of=parse_dt("2026-10-03T12:40:59+08:00"))
        shuttle = proj.capacities["cap-shuttle-west"]
        self.assertIsNotNone(shuttle.realtime_occupancy)
        self.assertIsNotNone(shuttle.safety_headroom)
        self.assertEqual(shuttle.service_level, "normal")       # 接驳本身在加密运转
        self.assertEqual(shuttle.community_level, "disrupted")  # 居民影响单列
        tickets = proj.capacities["cap-tickets-main"]
        self.assertIsNone(tickets.realtime_occupancy)           # 可售 ≠ 在场，不混用

    def test_board_ranking_puts_nearest_failure_first(self) -> None:
        store = load_zhangjiajie()
        proj = Projection.build(store)
        board = DutyBoard(proj, parse_dt("2026-10-03T12:40:00+08:00"))
        top = board.service_statuses()[0]
        self.assertIn(top.severity, ("critical",))


if __name__ == "__main__":
    unittest.main()
