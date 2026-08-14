# Open Observability Agent

一个以 Rust 为核心、面向 Agent 与工作流的开源可观测智能系统。目标是把“运行数据 → 可解释诊断 → Agent 执行 → 结果验证”做成可运营的多租户服务。

项目采用 Apache-2.0 许可证，欢迎通过 GitHub Issue/PR 参与。当前代码是早期开发版本，不应直接接收生产敏感数据。

## 第一阶段边界

- `observability-core`：租户、Observation、AgentRun 等领域对象与输入校验。
- 暂不承诺生产级采集、LLM provider、计费或云部署；这些必须在运行验证后再声明。

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
```

## 启动

```powershell
cargo run -p observability-api
```

服务默认监听 `0.0.0.0:8080`，数据写入 `data/observations.jsonl`。控制台原型位于 `apps/console/index.html`，可部署到静态托管服务后，将 API 地址接入前端。

生产环境请设置 `OBSERVABILITY_API_KEY`，客户端使用 `X-API-Key` 请求头；本地开发可以不设置。

用量账本默认写入 `data/usage.jsonl`，可通过 `OBSERVABILITY_USAGE_DATA` 指定路径；API 启动时会恢复已有账本。

单实例 SQLite 模式：设置 `OBSERVABILITY_STORAGE=sqlite`，数据库默认写入 `data/observability.sqlite`，也可通过 `OBSERVABILITY_SQLITE_DATA` 配置。

生产部署建议设置 `OBSERVABILITY_ENV=production`、`OBSERVABILITY_API_KEY` 和 `OBSERVABILITY_CORS_ORIGINS`（逗号分隔的前端来源）。当前 API key 是原型级全局密钥，正式 SaaS 仍需接入组织、项目、RBAC、密钥轮换和审计日志。

单租户部署可额外设置 `OBSERVABILITY_TENANT_ID=<uuid>`；所有 observation、Agent、model、usage 和 billing 请求的 tenant_id 不匹配时都会返回 `403`。多租户 SaaS 仍需将此配置替换为持久化的 tenant-scoped key/RBAC。

容器和云部署边界见 [docs/deployment.md](docs/deployment.md)。GitHub Actions 会执行格式检查和 workspace 测试。

本地容器启动：复制 `.env.example` 为 `.env` 后运行 `docker compose up --build`；API 使用 SQLite volume 持久化并提供 `/health` 健康检查。

API 契约见 [docs/openapi.yaml](docs/openapi.yaml)。

生产验收边界见 [docs/production-readiness.md](docs/production-readiness.md)。

SQLite backend 位于 `crates/observability-sqlite`，当前用于单实例验证，不是默认 API backend。

模型适配和数据处理边界见 [docs/model-providers.md](docs/model-providers.md)。

服务启动后可运行 `powershell -File .\scripts\smoke-api.ps1` 验证 health、观测写入/查询和 Agent 证据链。

在 Linux/WSL 中可运行 `bash ./scripts/smoke-api.sh` 验证实际 HTTP 启动链路。

可运行 `bash ./scripts/smoke-usage-persistence.sh` 验证服务重启后 usage 账本仍可恢复。
