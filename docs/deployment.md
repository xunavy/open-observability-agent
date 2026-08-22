# 部署边界

## Rust API

本地或通用容器环境：

```powershell
docker build -t open-observability-api .
docker run --rm -p 8080:8080 -v ${PWD}/data:/app/data open-observability-api
```

生产环境应将 `data/` 替换为具备备份、保留期和租户隔离策略的持久化存储。当前 JSONL 存储只适合开发和单实例验证。

API 支持 `OBSERVABILITY_STORAGE=sqlite` 启用 SQLite 单实例 backend；它适合本地或边缘原型，不等同于多实例生产数据库。

持久化 Investigation Run 只在 SQLite backend 开放。服务会以认证 tenant 为边界验证 evidence，并把完成状态、结果 Observation 与 AgentRun UsageEvent 原子提交；生产调查接口不接受全局运维 key，以避免由请求 body 选择 tenant。

设置 `OBSERVABILITY_INGEST_MODE=durable` 可启用 SQLite 持久化摄取队列。Docker Compose 已默认采用该组合，并把数据库放在命名 volume 中。队列参数为：

- `OBSERVABILITY_QUEUE_MAX_ATTEMPTS`：进入死信前最大处理次数，默认 5。
- `OBSERVABILITY_QUEUE_POLL_MS`：空闲轮询间隔，默认 250ms。
- `OBSERVABILITY_QUEUE_LEASE_MS`：消费者租约，默认 30 秒。

`GET /health` 保持公开以供容器编排健康检查；其他 API 在配置密钥后受认证保护。

## OTLP trace 接入

Rust API 按 [OTLP 1.11.0](https://opentelemetry.io/docs/specs/otlp/) 的 HTTP trace 路径，在 `POST /v1/traces` 接受 protobuf 与 JSON 编码的 `ExportTraceServiceRequest`。OpenTelemetry SDK 或 Collector 的 endpoint 可设置为 `http://api-host:8080`，并通过 exporter headers 发送 `x-tenant-id=<UUID>` 和 `x-api-key=<secret>`。API 会把 resource、instrumentation scope 和 span attributes 保留到 Observation，并把 GenAI/Agent/Tool/HTTP 语义分类到现有领域模型。

当前每个请求最多 4 MiB / 1000 spans；非法 span 使用 OTLP `partial_success` 报告。`Content-Encoding: gzip` 使用纯 Rust backend 解压，并再次执行 4 MiB 解压后上限以限制压缩炸弹；其他 content encoding 返回 `415`。OTLP/gRPC、metrics 和 logs signals 仍未实现。

## 控制台

`apps/console` 是零构建依赖的静态控制台，可部署到 Vercel 或 Cloudflare Pages。它直接读取 Rust API，覆盖观测筛选、诊断发现、持久队列、死信重放、Agent evidence 调查执行/恢复、usage 和 billing quote；没有连接或 API 返回空集时显示明确的空状态，不生成演示数据。

控制台会把 API 地址和 tenant UUID 写入 `localStorage`，API key 只写入当前标签页的 `sessionStorage`。这是用于开发和受控运维的连接方式，不是面向客户的 SaaS 登录系统；生产仍需要 OIDC/session、组织/项目权限、服务端密钥代理、CSRF 防护和审计。

本地开发未设置 `OBSERVABILITY_CORS_ORIGINS` 时仍启用 permissive CORS；设置 allowlist 后，API 只允许指定来源使用 `GET`/`POST` 及 `content-type`、`X-API-Key`、`X-Tenant-ID`、`Idempotency-Key` 请求头。生产必须设置逗号分隔的控制台域名 allowlist，例如 `https://console.example.com`。设置 `OBSERVABILITY_ENV=production` 后，必须配置非空 `OBSERVABILITY_API_KEY` 或 `OBSERVABILITY_API_KEYS`，否则除 `/health` 外的请求会失败。环境变量密钥仍不是完整的组织/RBAC/轮换系统。

设置 `OBSERVABILITY_API_KEY` 后，业务 API 要求请求头 `X-API-Key`；租户映射模式还要求 `X-Tenant-ID`。未设置时认证关闭，仅适合本地开发。

## Cloudflare 边缘层

推荐后续将 Cloudflare 用于 API Gateway、队列、对象存储和边缘入口；Rust API 保留领域逻辑、策略和数据处理。具体绑定、配额和价格必须在部署时以官方文档为准。
