# Architecture Decision Record 0001

## 产品内核

系统不是“聊天机器人加监控面板”，而是一个以 Observation 为事实来源的 Agent Operations 平台：每次 Agent、模型、工具和工作流执行都必须产生可关联的观测记录；诊断、策略和计费只能引用这些记录。

## 有界上下文

1. **Telemetry**：接收、规范化、采样、关联 trace/span。
2. **Investigation**：查询观测、生成 finding、保留证据链。
3. **Agent Runtime**：工具调用、审批、重试、预算和执行状态。
4. **Control Plane**：租户、项目、API key、RBAC、数据保留策略。
5. **Usage & Billing**：按事件量、保留时长、Agent 执行量和模型成本计量；早期只做 usage ledger，不绑定具体支付商。

## 第一阶段验收

- 核心 crate 可编译并通过单元测试。
- 一个 Observation 能被验证、序列化并关联到 AgentRun。
- 后续 API 必须保留 tenant_id，避免先做单租户 demo 再返工。
- SQLite 单实例路径必须先持久化摄取请求，再由租约 worker 消费；失败任务经过退避重试后进入租户隔离的死信队列。

## 摄取一致性边界

- `direct` 模式用于开发兼容，API 在 observation store 写入完成后返回 `201`。
- `durable` 模式只允许 SQLite，批量请求在同一事务中入队后返回 `202`。
- worker 写 observation 成功后再确认队列项；确认失败可能导致重复消费，因此 observation ID 是幂等键。
- 进程崩溃后，超过租约时间的 `processing` 项会重新变为可领取状态。
- 当前 SQLite mutex 与单 worker 设计只承诺单实例；多实例部署需要替换为 PostgreSQL/Cloudflare Queues 等具备并发领取语义的实现。

## 非目标

- 不复制 Grafana 的全部能力。
- 不在没有数据保留、隐私和成本策略前接入任意生产日志。
- 不把 Cloudflare、Vercel 或某个模型供应商写死到领域核心。
