# Release runbook

## GitHub

The local repository contains a clean `main` branch and the annotated `v0.1.0` tag. After creating an empty GitHub repository, run from the repository root:

```powershell
.\scripts\publish-github.ps1 -RemoteUrl https://github.com/<owner>/<repo>.git
```

The script refuses to publish a dirty worktree or overwrite an existing `origin` remote.

## Cloud deployment

- Vercel: deploy `apps/console` as a static project and configure its API base URL.
- Cloudflare: place an API Gateway/Worker or Pages frontend in front of the Rust API; keep SQLite single-instance only and use a production database for multiple replicas.
- Set `OBSERVABILITY_ENV=production`, `OBSERVABILITY_API_KEY`, `OBSERVABILITY_CORS_ORIGINS`, and provider secrets through the platform secret manager.
- Verify `/health`, `/metrics`, API-key rejection, tenant isolation, model completion, and persistence after deployment.

No cloud deployment is claimed until the provider CLI reports a deployment URL and the smoke checks pass against that URL.
