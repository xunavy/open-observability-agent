# Open Observability Agent

开源仓库：[github.com/xunavy/open-observability-agent](https://github.com/xunavy/open-observability-agent)

一个以 Rust 为核心、面向 Agent 与工作流的开源可观测智能系统。目标是把“运行数据 → 可解释诊断 → Agent 执行 → 结果验证”做成可运营的多租户服务。

项目采用 Apache-2.0 许可证，欢迎通过 GitHub Issue/PR 参与。当前代码是早期开发版本，不应直接接收生产敏感数据。

## 当前能力边界

- Rust API 支持租户隔离的 Observation、诊断、Agent 计划/安全工具执行、OpenAI-compatible 模型适配、用量账本与月度报价。
- OTLP/HTTP `POST /v1/traces` 支持 protobuf 与 OTLP JSON，将标准 span 映射为可供诊断和 Agent 引用的 Observation。
- SQLite 模式支持单实例持久化摄取队列、租约恢复、指数退避、死信查询和租户级重放。
- 静态 Web 控制台已接入真实 API，支持观测筛选、诊断、队列/死信重放、证据选择、Agent 调查计划、用量和月度报价；页面不注入演示数据。
- 这仍是可运行 MVP：尚未完成多实例数据库/队列、组织 RBAC、支付订阅同步和真实云环境验收，不应直接接收生产敏感数据。

## 目标架构

```text
SDK / OTLP / Webhook -> Rust ingest -> queue -> storage/index
                                      |-> query/API -> dashboard
                                      |-> policy/evaluator -> Agent runtime
                                      |-> usage meter -> subscription/billing
```

Rust 是业务核心与数据平面；Cloudflare 可承载边缘入口、对象存储/队列和轻量 Agent 协调；Vercel 适合托管控制台前端；Figma 用于先固化信息架构和可观测交互模型。具体服务选择需在实现阶段按官方文档和成本验证。

## 开发

```powershell
cargo test --workspace
cargo run -p observability-api
cargo run -p observability-agent -- --prompt "解释最近一次失败执行"
cargo run -p observability-agent -- --prompt "解释失败" --evidence examples/observations.json
# 可选：调用 OpenAI-compatible provider（不会把 key 写入仓库）
cargo run -p observability-agent -- --endpoint https://provider.example/v1/chat/completions --model your-model --api-key-env MODEL_API_KEY --prompt "解释失败" --evidence examples/observations.json
```

API smoke path:

```text
GET  /health
POST /v1/traces
POST /v1/observations
POST /v1/observations/batch
GET  /v1/observations?tenant_id=<uuid>
POST /v1/diagnostics
POST /v1/agent/plan
POST /v1/agent/execute
POST /v1/model/complete
POST /v1/usage
GET  /v1/usage?tenant_id=<uuid>&period=2026-08
POST /v1/billing/quote
GET  /v1/ingestion/queue?tenant_id=<uuid>
GET  /v1/ingestion/dead-letters?tenant_id=<uuid>&limit=100
POST /v1/ingestion/dead-letters/replay
```

## 启动

```powershell
cargo run -p observability-api
```

服务默认监听 `0.0.0.0:8080`，数据写入 `data/observations.jsonl`。控制台位于 `apps/console`，可直接部署到静态托管服务，也可在仓库根目录运行 `python -m http.server 4173 --directory apps/console` 后访问 `http://127.0.0.1:4173`。在“连接设置”中填写 API 地址、tenant UUID 和 API key；API key 仅保存在当前浏览器标签页的 `sessionStorage`。

API 同时提供 Prometheus 风格的 `GET /metrics`，可由 Prometheus 或 Grafana Cloud 抓取；指标包括服务存活、存储/摄取模式、接收量、模型与 Agent 调用量、队列处理、重试和死信计数。

OpenTelemetry SDK/Collector 可把 OTLP/HTTP endpoint 指向 `http://localhost:8080`，trace path 使用标准 `/v1/traces`；exporter headers 必须包含 `x-tenant-id=<tenant UUID>`，启用认证后还需 `x-api-key=<secret>`。服务接受 `application/x-protobuf` 和 OTLP JSON，以及 `Content-Encoding: gzip`；压缩前和解压后均限制为 4 MiB，单请求最多 1000 spans，非法 span 通过 OTLP `partial_success` 返回。

生产环境请设置 `OBSERVABILITY_API_KEY`，客户端使用 `X-API-Key` 请求头；本地开发可以不设置。

用量账本默认写入 `data/usage.jsonl`，可通过 `OBSERVABILITY_USAGE_DATA` 指定路径；API 启动时会恢复已有账本。

单实例 SQLite 模式：设置 `OBSERVABILITY_STORAGE=sqlite`，数据库默认写入 `data/observability.sqlite`，也可通过 `OBSERVABILITY_SQLITE_DATA` 配置。再设置 `OBSERVABILITY_INGEST_MODE=durable` 后，单条和批量摄取先原子写入队列并返回 `202 Accepted`，后台 worker 再写入 observation store；默认 `direct` 模式保持同步写入并返回 `201 Created`。

持久队列可通过 `OBSERVABILITY_QUEUE_MAX_ATTEMPTS`、`OBSERVABILITY_QUEUE_POLL_MS` 和 `OBSERVABILITY_QUEUE_LEASE_MS` 调整。死信接口始终执行 tenant 校验；队列未启用时返回 `409 Conflict`。

生产部署建议设置 `OBSERVABILITY_ENV=production`、认证密钥和 `OBSERVABILITY_CORS_ORIGINS`（逗号分隔的前端来源）。认证可以使用全局 `OBSERVABILITY_API_KEY`，也可以使用 `OBSERVABILITY_API_KEYS=tenant_uuid=secret,...`；生产模式两者都未配置时，除公开 `/health` 外的请求返回 `500`。当前密钥配置仍是原型级能力，正式 SaaS 需要持久化组织、项目、RBAC、轮换和审计日志。

单租户部署可额外设置 `OBSERVABILITY_TENANT_ID=<uuid>`；所有 observation、Agent、model、usage 和 billing 请求的 tenant_id 不匹配时都会返回 `403`。多租户 SaaS 仍需将此配置替换为持久化的 tenant-scoped key/RBAC。

容器和云部署边界见 [docs/deployment.md](docs/deployment.md)。GitHub Actions 会执行格式检查、Clippy、workspace 测试、控制台 JavaScript 语法检查、持久队列重启 smoke、tenant auth/CORS smoke 和 OTLP trace smoke。

本地容器启动：复制 `.env.example` 为 `.env` 后运行 `docker compose up --build`；API 使用 SQLite volume 持久化并提供 `/health` 健康检查。

API 契约见 [docs/openapi.yaml](docs/openapi.yaml)。

生产验收边界见 [docs/production-readiness.md](docs/production-readiness.md)。

SQLite backend 位于 `crates/observability-sqlite`，当前用于单实例验证，不是默认 API backend。

模型适配和数据处理边界见 [docs/model-providers.md](docs/model-providers.md)。

服务启动后可运行 `powershell -File .\scripts\smoke-api.ps1` 验证 health、观测写入/查询和 Agent 证据链。

在 Linux/WSL 中可运行 `bash ./scripts/smoke-api.sh` 验证实际 HTTP 启动链路。

可运行 `bash ./scripts/smoke-usage-persistence.sh` 验证服务重启后 usage 账本仍可恢复。

可运行 `bash ./scripts/smoke-durable-queue.sh` 验证 observation 在 worker 消费前写入 SQLite、API 重启后仍能被消费，并最终从队列移除。

可运行 `bash ./scripts/smoke-tenant-auth.sh` 验证生产 tenant key、公开健康检查和跨租户拒绝链路。

可运行 `bash ./scripts/smoke-otlp.sh` 验证 OTLP JSON trace、tenant key、持久队列、重试去重和 Observation 映射链路。
