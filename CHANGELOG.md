# Changelog

## Unreleased

当前主分支是一个可运行的 Rust observability-agent MVP，包含：

- 多租户 observation、diagnostics 和 evidence-backed Agent API
- 安全工具 allowlist 与显式 approval
- JSONL 和单实例 SQLite 持久化
- deterministic provider 与可选 OpenAI-compatible provider
- usage ledger、整数分计费报价和 Prometheus metrics
- Docker Compose、GitHub Actions 和静态控制台原型

尚未宣称为生产 SaaS 的部分：

- 多租户 RBAC、组织/项目模型和 tenant-scoped key rotation
- 多实例数据库、持久化队列、重试与死信
- 支付 provider webhook、订阅同步、退款和税务边界
- Cloudflare/Vercel 实际部署及 GitHub 远程发布
