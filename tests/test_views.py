import unittest
from datetime import timedelta

from src.event_store import parse_dt
from src.scenario import load_zhangjiajie
from src.views import EphemeralContext, PartnerView, PublicView


class PublicViewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = load_zhangjiajie()

    def test_no_personal_or_internal_fields_leak(self) -> None:
        view = PublicView(self.store, parse_dt("2026-10-03T12:50:00+08:00"),
                          known_at=parse_dt("2026-10-03T12:50:00+08:00"))
        rows = view.render("zh")
        blob = str(rows)
        # 预约引用、团队号、精确内部数值不出现在公众数据中
        self.assertNotIn("BK-G7781", blob)
        self.assertNotIn("FIT-EN-9917", blob)
        self.assertNotIn("708", blob)   # 接驳精确占用
        self.assertNotIn("1820", blob)  # 住宿占用
        for row in rows:
            self.assertIn(row["accessibility"],
                          {"open", "busy", "very_busy", "throttled", "unknown"})
            self.assertTrue(row["community_pressure"] in {"none", "noticeable", "disrupted"})

    def test_throttle_and_community_pressure_visible(self) -> None:
        view = PublicView(self.store, parse_dt("2026-10-03T12:50:00+08:00"),
                          known_at=parse_dt("2026-10-03T12:50:00+08:00"))
        rows = {(r["point"]["owner"], r["point"]["location_id"]): r for r in view.render("zh")}
        shuttle = rows[("transport", "shuttle-west-gate")]
        self.assertEqual(shuttle["accessibility"], "throttled")
        # 社区压力在同页呈现
        community = rows[("community", "west-access-road")]
        self.assertEqual(community["community_pressure"], "disrupted")

    def test_throttle_lift_requires_restore_event_not_band_change(self) -> None:
        # 13:50 公众发布的接驳档位已恢复正常，但在恢复事件生成前仍标记
        # throttled——解除只能由满足恢复条件的恢复事件驱动，不能只看档位变化
        view = PublicView(self.store, parse_dt("2026-10-03T13:50:00+08:00"))
        rows = {(r["point"]["owner"], r["point"]["location_id"]): r for r in view.render("zh")}
        self.assertEqual(rows[("transport", "shuttle-west-gate")]["accessibility"], "throttled")

        # 恢复事件生成并入库后，同一点位回到 open
        from src.coordination import RecoveryTracker, Sequence
        from src.monitoring import Projection
        now = parse_dt("2026-10-03T13:50:00+08:00")
        throttle = self.store.get("zjj-decision-01")
        restored = RecoveryTracker(self.store, Projection.build(self.store, as_of=now)).restore(
            throttle, now,
            confirmed_by={"owner": "transport", "role": "经理", "party": "transport-duty-w03"},
            seq=Sequence("zjj-pub"),
        )
        self.assertIsNotNone(restored)
        view2 = PublicView(self.store, now)
        rows2 = {(r["point"]["owner"], r["point"]["location_id"]): r for r in view2.render("zh")}
        self.assertEqual(rows2[("transport", "shuttle-west-gate")]["accessibility"], "open")

    def test_only_public_events_used(self) -> None:
        view = PublicView(self.store, parse_dt("2026-10-03T12:40:00+08:00"))
        ids = {s.aggregate_id for s in view.projection.capacities.values()}
        self.assertNotIn("cap-card-terminal-07", ids)   # restricted 外卡终端不入公众投影
        self.assertNotIn("cap-lodging-west", ids)       # 默认 restricted
        self.assertNotIn("cap-onsite-core", ids)        # partners only


class PartnerViewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = load_zhangjiajie()

    def test_minimum_need_to_know(self) -> None:
        transport = PartnerView(self.store, "transport")
        lodging = PartnerView(self.store, "lodging")

        # 交通方拿不到退税商店终端细粒度信号（未列入 allowed_partners）
        self.assertIsNone(transport.shared_signal("cap-card-terminal-07"))
        # 住宿方默认只看得见自己的 restricted 数据
        self.assertIsNone(lodging.shared_signal("cap-shuttle-west"))
        self.assertIsNotNone(lodging.shared_signal("cap-lodging-west"))

    def test_owner_sees_own_restricted(self) -> None:
        shop = PartnerView(self.store, "tax_refund_shop")
        signal = shop.shared_signal("cap-card-terminal-07")
        self.assertIsNotNone(signal)
        # 最小化：不回传传感器身份等内部采集细节
        self.assertNotIn("collection", signal)

    def test_partner_listing_respects_visibility(self) -> None:
        community = PartnerView(self.store, "community")
        visible_ids = {e["event_id"] for e in community.visible_events()}
        self.assertIn("zjj-cap-shuttle-04", visible_ids)     # partners 含 community
        self.assertNotIn("zjj-cap-firstaid-02", visible_ids)  # 医疗仅对景区共享


class EphemeralContextTest(unittest.TestCase):
    def test_hint_is_minimal_and_expires(self) -> None:
        ctx = EphemeralContext(ttl_minutes=30)
        t0 = parse_dt("2026-10-03T12:40:00+08:00")
        token = ctx.open_session("en", "grid-A3", now=t0)
        hint = ctx.to_partner_hint(token, now=t0 + timedelta(minutes=1))
        self.assertEqual(set(hint.keys()),
                         {"language_preference", "coarse_area", "purpose", "expires_at"})
        self.assertEqual(hint["purpose"], "current_safeguard_only")
        self.assertIsNone(ctx.to_partner_hint(token, now=t0 + timedelta(minutes=31)))

    def test_close_revokes(self) -> None:
        ctx = EphemeralContext()
        t0 = parse_dt("2026-10-03T12:40:00+08:00")
        token = ctx.open_session("ja", "grid-B1", now=t0)
        ctx.close(token)
        self.assertIsNone(ctx.use(token, now=t0))


if __name__ == "__main__":
    unittest.main()
