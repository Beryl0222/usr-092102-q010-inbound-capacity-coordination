# 入境游承载协调

景区售票页仍显示余票时，接驳车可能已排起长队、外卡受理点接近饱和、多语急救只剩
一组值班，社区道路也开始受影响——**“游客总量”一个数字既看不见局部瓶颈，也无法
指导谁来分流**。本仓库在领域事件契约之上，提供一套跨机构承载协调的参考实现：
景区、交通、住宿、退税商店、医疗点、社区各自维护带时间窗、统计口径与可信度的
容量/压力信号，系统只提出替代方案，运营变更由相应责任方确认。

## 核心约定

### 五类信号必须分开

每条 `CAPACITY_REPORTED` 分别报告，禁止合并成单一总量：

| 信号 | 字段 | 示例 |
| --- | --- | --- |
| 可售数量 | `sellable_quantity` | 分时余票 860 张 |
| 实时占用 | `realtime_occupancy` | 在队 708 人 / 终端占用 97% |
| 安全余量 | `safety_headroom` | 距安全上限 -8 人（已越线） |
| 服务降级 | `service_degradation` | 多语急救仅一组值班（severely_degraded） |
| 居民影响 | `community_impact` | 排队外溢、社区道路 disrupted |

每条信号带 `window`（时间窗与粒度）、`metric`（口径代码、单位、实测/估算/预测）
与 `confidence`（可信度）。口径不同的数字不得直接相加比较。大型团队、散客与
免签政策变化记录在 `driver_factors` 中，只用于生成预测与方案。

### 一次限流的完整生命周期

```
CAPACITY_REPORTED（各机构，分离信号）
      │ 安全余量耗尽 / 趋势外推即将失守
      ▼
PRESSURE_DETECTED              触发信号（局部瓶颈）
      ▼
DIVERSION_PROPOSED             系统建议：替代时段 / 线路 / 资源 + 受保护预约清单
      ▼
ACTION_CONFIRMED(throttle_entry)   协同决定：只能由责任方确认，必须带恢复条件
      │ 恢复条件连续保持 hold_minutes（可附社区影响归零等附加条件）
      ▼
NORMAL_SERVICE_RESTORED(lift_throttle)   恢复：回填被解除的决定与实测核对
```

- 全链共用 `correlation_id`、以 `caused_by` 串起因果链，可用
  `coordination.trace_incident()` 端到端追溯。
- **系统只建议、不决策**：`proposed_by` 恒为 `coordination-system`；确认方
  `party` 不得是系统自身；限流必须携带 `recovery_condition`，未满足条件不得
  生成恢复事件（校验器强制 `evaluation.satisfied=true`）。
- **已作出的预约承诺不得被新预测静默取消**：承诺进入方案的
  `protected_commitments`，只允许 `honor` 或 `offer_reschedule_with_consent`，
  不存在“取消”语义；决定引用清单外预约会被拦截。

### 断网传感器按发生时间重放

事件顺序以 `occurred_at`（现场观测时刻，必须带时区）为准，入库时刻另记
`payload.collection.ingested_at`。断网恢复后补传的事件
`transmission=backfill`：补传到达前在 `known_at` 视角下不可见（值班看板显示
数据陈旧），到达后仍按发生时刻插回时间轴正确位置。

### 隐私与最小授权

- 公众页（`PublicView`）只消费显式标记 `visibility=public` 的事件，输出分档
  可达性（通畅/拥挤/限流）与社区压力，不含任何预约引用、团队号或精确数值。
- 合作机构（`PartnerView`）按 `data_sharing` 的 `public / partners / restricted`
  过滤，只能得到完成本次调整所需的信号；无共享声明默认仅本机构可见。
- 匿名位置（粗网格）与语言偏好由 `EphemeralContext` 纯内存保存、短时过期，
  **不进入事件流**，对合作方只产生一次性、用途限定的提示。

## 目录

```
contracts/domain.schema.json   事件信封 + 各事件类型 payload 的 JSON Schema
src/validator.py               纯标准库事件校验（兼容仅含信封的 v1 记录）
src/event_store.py             事件存储：事件时间重放、known_at 视角、版本完整性
src/monitoring.py              分机构信号投影、趋势外推、值班看板（下一处将失守）
src/coordination.py            建议生成、责任方确认、恢复条件核对与限流闭环
src/views.py                   公众页 / 合作方最小授权视图 / 当次匿名上下文
src/scenario.py                装载 data/ 下的事件
src/demo.py                    端到端演示
tools/build_scenario.py        生成张家界场景数据
data/sample.json               v1 信封联调样例
data/scenario_zhangjiajie.json 2026-10-03 西入口接驳限流全链条（含 2 条断网补传）
tests/                         36 个用例：契约、重放、监测、闭环、隐私
```

## 本地运行

```bash
python3 tools/build_scenario.py     # 重新生成场景数据
python3 -m src.demo                 # 端到端演示（看板→补传→决定→恢复→公众页）
python3 -m unittest discover -s tests
```

演示覆盖：12:39 补传未到时看板已靠趋势外推标出核心区约 18 分钟后失守；12:40
断网终端补传到达后外卡饱和暴露；交通值班经理确认限流（团队预约照发、散客改签
须同意）；13:20/13:35 恢复条件未满足时拒绝解除，13:50 连续保持 15 分钟后生成
恢复事件；公众页同时呈现可达性与社区压力且不泄露任何行程。

## 扩展边界

- 信封字段（`event_id/event_type/aggregate_type/aggregate_id/occurred_at/
  version/summary`）保持稳定；新增业务内容放入 `payload` 与 `data_sharing`。
- 新增机构类别或压力类型时，同步更新 schema 的枚举与 `validator.py`。
- 本实现是单机参考内核（内存事件存储）；接入消息总线时，`EventStore.append`
  与按 `occurred_at` 重放的语义应原样保留。
