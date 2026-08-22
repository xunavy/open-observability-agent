# 部署边界

## Rust API

本地或通用容器环境：

```powershell
docker build -t open-observability-api .
docker run --rm -p 8080:8080 -v ${PWD}/data:/app/data open-observability-api
```

生产环境应将 `data/` 替换为具备备份、保留期和租户隔离策略的持久化存储。当前 JSONL 存储只适合开发和单实例验证。

API 支持 `OBSERVABILITY_STORAGE=sqlite` 启用 SQLite 单实例 backend；它适合本地或边缘原型，不等同于多实例生产数据库。

设置 `OBSERVABILITY_INGEST_MODE=durable` 可启用 SQLite 持久化摄取队列。Docker Compose 已默认采用该组合，并把数据库放在命名 volume 中。队列参数为：

- `OBSERVABILITY_QUEUE_MAX_ATTEMPTS`：进入死信前最大处理次数，默认 5。
- `OBSERVABILITY_QUEUE_POLL_MS`：空闲轮询间隔，默认 250ms。
- `OBSERVABILITY_QUEUE_LEASE_MS`：消费者租约，默认 30 秒。

`GET /health` 保持公开以供容器编排健康检查；其他 API 在配置密钥后受认证保护。

## 控制台

`apps/console` 是静态控制台原型，可部署到 Vercel 或 Cloudflare Pages。当前页面使用演示数据，尚未接入 API，因此不能当作生产控制台。

本地开发未设置 `OBSERVABILITY_CORS_ORIGINS` 时仍启用 permissive CORS；生产必须设置逗号分隔的控制台域名 allowlist，例如 `https://console.example.com`。设置 `OBSERVABILITY_ENV=production` 后，必须配置非空 `OBSERVABILITY_API_KEY` 或 `OBSERVABILITY_API_KEYS`，否则除 `/health` 外的请求会失败。环境变量密钥仍不是完整的组织/RBAC/轮换系统。

设置 `OBSERVABILITY_API_KEY` 后，业务 API 要求请求头 `X-API-Key`；租户映射模式还要求 `X-Tenant-ID`。未设置时认证关闭，仅适合本地开发。

## Cloudflare 边缘层

推荐后续将 Cloudflare 用于 API Gateway、队列、对象存储和边缘入口；Rust API 保留领域逻辑、策略和数据处理。具体绑定、配额和价格必须在部署时以官方文档为准。
