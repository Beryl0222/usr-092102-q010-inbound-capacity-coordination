# 入境游承载协调

张家界等入境游目的地在“售票页仍有余票”时，接驳车、外卡受理、多语急救、社区道路可能已先后承压。
本仓库以 `contracts/domain.schema.json` 的**领域事件**为接入基础，提供一套跨机构承载协调的参考实现：
各机构（景区、交通、住宿、退税商店、医疗点、社区）维护各自带**时间窗、统计口径、可信度**的容量与压力信号，
系统只**提出**替代时段/线路/资源方案，关闭入口与改变运营由相应**责任方确认**，既有预约**不被静默取消**。

## 领域模型与治理约束

事件流五类事件（信封 v0.1 向后兼容，新增字段可选）：

| 事件 | 聚合 | 含义 |
| --- | --- | --- |
| `CAPACITY_REPORTED` | `capacity_source` | 某来源某时间窗的**分项**读数 |
| `PRESSURE_DETECTED` | `pressure_signal` | 指向具体环节的局部压力信号 |
| `DIVERSION_PROPOSED` | `diversion_plan` | 系统的替代时段/线路/资源**提案** |
| `ACTION_CONFIRMED` | `operating_decision` | 责任方确认（也可拒绝）运营动作 |
| `NORMAL_SERVICE_RESTORED` | `operating_decision` | 基于证据满足恢复条件后解除 |

关键约定：

- **五条互不相混的指标通道**：可售数量（sellable）、实时占用（occupancy）、安全余量（safety）、
  服务降级（degradation）、居民影响（community）。拒绝用一个“游客总量”掩盖局部瓶颈。
- **时间语义**：`occurred_at`=现场发生时间、`received_at`=平台接收时间、`time_window`=业务时间窗；
  所有时间必须带时区偏移。断网传感器补报时 `occurred_at` 仍是读数当时，重放一律按发生时间归位。
- **口径与可信度**：每个读数自带 `basis`（闸门去重/排队折半/警方反馈…）与 `confidence`。
- **因果与关联**：`correlation_id` 串联一次限流的全链路，`causation_id` 指向上游事件。
- **仅提案 + 责任方确认**：没有 `ACTION_CONFIRMED`，入口不会关闭；责任方确认时必须给出 `authority`
  与可机器核对的**恢复条件**（阈值 + 持续时长 + 跨机构证据来源）。
- **预约承诺**：处理原则只有 `honor` / `offer_choice`，结构上不存在“静默取消”。
- **最小共享与匿名**：匿名位置只能是分段网格（`grid:` 前缀），语言偏好只存当次会话（默认 120 分钟过期清除）；
  事件中禁止个人标识/证件/联系方式/精确坐标/个人行程；合作机构按角色白名单各取所需。

## 目录

- `contracts/domain.schema.json`：领域事件信封 + 各事件类型 payload 的 JSON Schema（2020-12）。
- `data/sample.json`：一条符合完整契约的中文业务样例（旧 v0.1 无 payload 信封仍可被信封校验接受）。
- `data/scenario.json`：2026-10-03 张家界当日 35 条事件流，含断网补报、六类机构信号与一次限流全链路。
- `src/timeutil.py`：带时区的时间解析与时间窗工具。
- `src/validator.py`：信封级（兼容）与严格级（含治理规则）校验，规则与 schema 一致。
- `src/eventstore.py`：只追加存储，event_id 幂等、聚合版本递增、按 `occurred_at` 重放、as-of 查询。
- `src/model.py`：容量看板、阈值策略、压力分析与“下一处失守”线性外推、预测因子。
- `src/coordination.py`：压力持久化、分流引擎（仅提案）、责任方确认、恢复条件评估、全链路 trace。
- `src/privacy.py`：当次会话信号（k-匿名）、事件个人数据扫描、合作机构最小信息包。
- `src/projections.py`：公众页（可达性 + 社区压力，无个人行程）与值班页（下一处失守排序）。
- `src/scenario.py` / `src/demo.py` / `src/export_data.py`：场景构建、终端演示、数据导出。
- `tests/`：26 项一致性测试（无 jsonschema 时 23 项仍可运行，schema 测试自动跳过）。

## 本地检查与演示

```bash
python3 -m unittest discover -s tests     # 测试（安装 jsonschema 后额外跑 JSON Schema 校验）
python3 -m src.demo                       # 打印断网重放、值班研判、限流全链路、公众页与机构信息包
python3 -m src.export_data                # 重新生成 data/scenario.json
```

## 一次限流的生命周期（correlation_id 可端到端追溯）

```
CAPACITY_REPORTED（含断网补报，按 occurred_at 归位）
        │  based_on 引用读数事件
        ▼
PRESSURE_DETECTED × 多源局部瓶颈（watch/warning/critical，附口径与可信度）
        │  trigger_signal_ids
        ▼
DIVERSION_PROPOSED（替代时段/线路/资源；预约 honor/offer_choice；仅建议）
        │  proposal_id + causation_id
        ▼
ACTION_CONFIRMED（景区/交警在各自 authority 内确认或拒绝，附恢复条件）
        │  阈值持续达标 + 跨机构证据
        ▼
NORMAL_SERVICE_RESTORED（证据齐备才解除；11:30 持续不足时不会误解除）
```
