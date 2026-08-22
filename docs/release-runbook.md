# Release runbook

## GitHub release

1. 确认 `main` worktree 干净，`cargo fmt`、Clippy、workspace tests 和全部 smoke 通过。
2. 更新 workspace version、`CHANGELOG.md` 与 OpenAPI version。
3. 推送 `main`，等待 `quality` 和 `container` 两条 GitHub Actions 成功。
4. 创建 annotated tag（例如 `git tag -a v0.3.0 -m "v0.3.0"`）并推送该 tag。
5. `.github/workflows/release.yml` 会创建 GitHub Release；发布后核对 tag、commit 和生成的 release notes。

任何新的 release 都必须指向已经通过远端质量门禁的 commit，并在 GitHub Release API 中确认不是 draft/prerelease。

## Cloud deployment

- Vercel: 将 `apps/console` 部署为静态项目；工作流需要 `VERCEL_TOKEN`、`VERCEL_ORG_ID` 和 `VERCEL_PROJECT_ID`。
- Cloudflare Pages: 工作流需要 `CLOUDFLARE_API_TOKEN`、`CLOUDFLARE_ACCOUNT_ID` 和 `CLOUDFLARE_PAGES_PROJECT`。
- Rust API: 设置 `OBSERVABILITY_ENV=production`、tenant/global API keys、`OBSERVABILITY_CORS_ORIGINS` 和持久化存储配置。
- OTLP exporter: endpoint 指向 API，headers 包含 `x-tenant-id` 和 `x-api-key`；支持 identity 或 gzip request body。
- 上线后验证 `/health`、`/metrics`、API-key 拒绝、tenant 隔离、OTLP trace、模型调用、持久化重启和控制台浏览器链路。

Vercel/Cloudflare 工作流在 secrets 缺失时会成功结束但跳过 deploy step。只有 provider 返回部署 URL，且针对该 URL 的 smoke 通过后，才能宣称已经云部署。
