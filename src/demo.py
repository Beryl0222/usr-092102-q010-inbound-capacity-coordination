"""端到端演示：一次接驳限流从预警、协同决定到恢复条件满足的全过程。

运行：python3 -m src.demo
"""

from datetime import datetime

from .coordination import Commitment, Coordinator, RecoveryTracker, Sequence, trace_incident
from .event_store import parse_dt
from .monitoring import DutyBoard, Projection
from .scenario import load_zhangjiajie
from .views import EphemeralContext, PartnerView, PublicView

CORR = "INC-20261003-WESTGATE-01"


def section(title: str) -> None:
    print("\n" + "=" * 64)
    print(title)
    print("=" * 64)


def main() -> None:
    store = load_zhangjiajie()
    problems = store.integrity_check()
    assert not problems, problems
    print(f"已装载 {len(store.all_stored())} 条事件；其中补传 {len(store.backfilled_events())} 条。")

    # 1) 补传到达前（12:39）：值班视角已能看出下一处将要失守的环节 ----------------
    section("① 12:39 值班看板（外卡终端仍断网，补传尚未到达）")
    known_1239 = parse_dt("2026-10-03T12:39:00+08:00")
    proj_early = Projection.build(store, known_at=known_1239)
    board_early = DutyBoard(proj_early, known_1239)
    print(board_early.render_text())
    failing = [s for s in board_early.service_statuses() if s.severity in ("critical", "breach")]
    projected = [s for s in board_early.service_statuses() if s.severity == "projected_watch"]
    print("\n>> 已经失守/越线：")
    for s in failing:
        print(f"   [{s.severity}] {s.label}")
    print(">> 即将失守（趋势外推）：")
    for s in projected:
        print(f"   {s.label}：约 {s.eta_minutes:.0f} 分钟后余量归零")
    print("（售票页此时仍显示上千余票——总量看不见这些局部瓶颈）")

    # 2) 12:40 补传到达：断网数据按发生时间插回，外卡饱和暴露 ----------------------
    section("② 12:40 断网恢复，补传到达后按发生时间重放")
    for s in store.backfilled_events():
        print(f"补传事件 {s.event_id}: 发生={s.occurred_at:%H:%M} 入库={s.ingested_at:%H:%M}")
    proj_now = Projection.build(store)
    board_now = DutyBoard(proj_now, parse_dt("2026-10-03T12:40:00+08:00"))
    top = board_now.service_statuses()[0]
    print(f">> 重放后最高优先级：[{top.severity}] {top.label}")
    for r in top.reasons:
        print(f"   - {r}")

    # 3) 端到端追溯：触发信号 → 系统建议 → 责任方决定 ------------------------------
    section("③ 限流协同链追溯（correlation_id 端到端）")
    for e in trace_incident(store, CORR):
        who = ""
        if e["event_type"] in ("ACTION_CONFIRMED",):
            who = f" | 确认方={e['payload']['confirmed_by']['party']}"
        print(f"{e['occurred_at'][11:16]} {e['event_type']:<24} {e['event_id']}{who}")
    plan = store.get("zjj-plan-01")
    print("\n受保护预约承诺（任何预测都不得静默取消）：")
    for c in plan["payload"]["protected_commitments"]:
        print(f"  - {c['commitment_ref']}（{c['holder_kind']}）→ {c['handling']}")

    # 4) 系统只能建议：尝试让系统自行确认会被拒绝 ---------------------------------
    section("④ 权限边界")
    seq = Sequence(prefix="zjj-demo")
    coordinator = Coordinator(store, seq)
    pressure = store.get("zjj-pressure-01")
    try:
        coordinator.confirm(
            decision="throttle_entry",
            confirmed_by={"owner": "transport", "role": "自动", "party": "coordination-system"},
            pressure_event=pressure,
            recovery_condition={},
        )
    except ValueError as exc:
        print(f"系统自批被拦截：{exc}")
    try:
        coordinator.confirm(
            decision="reroute",
            confirmed_by={"owner": "transport", "role": "值班经理", "party": "transport-duty-w03"},
            pressure_event=pressure, plan_event=plan,
            reschedule_commitments=["UNKNOWN-BOOKING"],
        )
    except ValueError as exc:
        print(f"借分流变更清单外预约被拦截：{exc}")

    # 5) 恢复条件：未满足不能解除；满足后生成恢复事件 -------------------------------
    section("⑤ 恢复条件核对（安全余量>120 且连续保持15分钟，社区影响归零）")
    tracker = RecoveryTracker(store, proj_now)
    throttle = store.get("zjj-decision-01")
    for check_time in ("13:20", "13:35", "13:50"):
        now = parse_dt(f"2026-10-03T{check_time}:00+08:00")
        proj = Projection.build(store, as_of=now)
        tracker_t = RecoveryTracker(store, proj)
        result = tracker_t.evaluate(throttle, now)
        detail = result.get("reasons", ["满足"])
        print(f"{check_time} satisfied={result['satisfied']}：{'; '.join(detail) or '满足'}")

    now = parse_dt("2026-10-03T13:50:00+08:00")
    proj_final = Projection.build(store, as_of=now)
    restored = RecoveryTracker(store, proj_final).restore(
        throttle, now,
        confirmed_by={"owner": "transport", "role": "接驳运营值班经理", "party": "transport-duty-w03"},
        seq=seq,
    )
    assert restored is not None
    print(f"\n>> 已生成恢复事件 {restored['event_id']}（{restored['aggregate_id']} v{restored['version']}），"
          f"解除 {restored['payload']['restores_decision_event_id']}")
    print(f"   核对：observed={restored['payload']['evaluation']['observed_value']} "
          f"held_since={restored['payload']['evaluation']['held_since'][11:16]}")

    # 6) 公众页：只有分档可达性与社区压力，没有任何行程信息 -------------------------
    section("⑥ 公众页面（不暴露个人行程）")
    public_1245 = PublicView(store, parse_dt("2026-10-03T12:45:00+08:00"),
                             known_at=parse_dt("2026-10-03T12:45:00+08:00"))
    print(public_1245.render_text("zh"))
    print()
    public_en = PublicView(store, now)
    print(public_en.render_text("en"))

    # 7) 合作机构最小授权 -----------------------------------------------------------
    section("⑦ 合作机构最小授权视图")
    for partner in ("transport", "lodging", "tax_refund_shop"):
        view = PartnerView(store, partner)
        visible = view.visible_events()
        print(f"{partner}: 可见 {len(visible)} 条事件")
    card_signal = PartnerView(store, "transport").shared_signal("cap-card-terminal-07")
    print(f"transport 视角看外卡终端（未授权）：{card_signal}")
    card_signal = PartnerView(store, "tax_refund_shop").shared_signal("cap-card-terminal-07")
    print(f"tax_refund_shop 视角看自有终端：占用 {card_signal['realtime_occupancy']}%（无传感器/采集细节）")

    # 8) 匿名上下文只服务当次保障 ---------------------------------------------------
    section("⑧ 匿名位置与语言偏好（当次有效，不入事件流）")
    ephemeral = EphemeralContext(ttl_minutes=120)
    token = ephemeral.open_session("en", "grid-A3", now=parse_dt("2026-10-03T12:40:00+08:00"))
    hint = ephemeral.to_partner_hint(token, now=parse_dt("2026-10-03T12:41:00+08:00"))
    print(f"合作方一次性提示：{hint}")
    ephemeral.close(token)
    print(f"会话关闭后再取：{ephemeral.to_partner_hint(token)}")
    stream_text = str([s.event for s in store.all_stored()])
    print(f"语言偏好/粗区域是否进入事件流：{'grid-A3' in stream_text or 'language_preference' in stream_text}")


if __name__ == "__main__":
    main()
