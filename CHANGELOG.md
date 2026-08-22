# Changelog

## Unreleased

下一阶段聚焦 OTLP metrics/logs、gzip、组织身份与多实例存储。

## 0.3.0 - 2026-08-23

- 增加标准 `POST /v1/traces` OTLP/HTTP trace 入口，支持 protobuf 与 OTLP JSON。
- 将 resource、instrumentation scope 和 span attributes 映射为 tenant-scoped Observation。
- 对 Agent、Tool、Model、HTTP 和 Workflow span 做确定性分类，并保留 trace/span evidence。
- 使用 tenant + trace ID + span ID 生成确定性 UUID，SQLite/durable 模式可对 exporter 重试去重。
- 支持 OTLP partial success、4 MiB / 1000 spans 限制、并发入口背压和 Prometheus OTLP 计数器。
- 控制台改为真实 API 驱动，覆盖观测、诊断、队列/死信、Agent evidence、用量和报价。
- 补充 OTLP、tenant auth/CORS、durable restart smoke，以及 Rust/JavaScript CI 门禁。

## 0.2.0 - 2026-08-22

- 增加 SQLite 单实例持久化摄取队列、租约恢复、指数退避、死信和 tenant-scoped replay。
- 增加 production tenant key 映射、公开 `/health` 和显式 CORS allowlist。
- 修复 Docker build context，并通过 Linux、容器和 GitHub Release 验证。

## 0.1.0

- 建立 Rust workspace、Observation 领域模型、诊断与 evidence-backed Agent API。
- 增加安全工具 allowlist、JSONL/SQLite 存储、usage ledger、整数分报价和 Prometheus metrics。
- 增加 deterministic 与 OpenAI-compatible model provider 边界。

## 尚未达到生产 SaaS 的部分

- 组织/项目、OIDC/session、RBAC、tenant key rotation 和审计。
- 多实例数据库/队列、租户配额、保留策略和容量治理。
- OTLP metrics/logs、gzip request body 与 gRPC。
- 支付 provider webhook、订阅同步、退款和税务边界。
- Cloudflare/Vercel 实际部署、域名与云端 smoke。
