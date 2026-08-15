# Production readiness

## 已验证

- Rust workspace 在 WSL Linux 编译通过。
- API 与核心领域测试通过。
- `/health` 可真实启动并响应。
- Observation 与 usage ledger 可 JSONL 持久化并重启恢复。
- Agent 决策返回 evidence IDs。
- 工具执行具备 allowlist 和 approval 状态。
- 月度报价使用整数分。

## 尚未达到生产标准

- JSONL 不是多实例一致性存储；生产需要 PostgreSQL/ClickHouse/SurrealDB 等明确选型。
- 已新增 `observability-sqlite` backend，适合单实例/边缘原型；API 默认仍使用 JSONL，SQLite 切换尚未作为默认部署路径。
- 当前批量 ingestion 是同步追加；生产需要队列、背压、重试和死信处理。
- 当前批量 ingestion 限制为每请求 1000 条；该限制不是队列或租户级配额的替代品。
- 当前 API 以 8 个并发槽位提供轻量背压，槽位耗尽返回 429；生产应替换为持久化队列和消费者。
- API key 不是完整身份系统；生产需要组织、项目、RBAC、密钥轮换和审计。
- CORS 当前为 permissive 原型配置，生产必须使用 allowlist。
- 已增加 `OBSERVABILITY_TENANT_ID` 单租户运行时边界；不匹配的请求返回 403，但这仍不是多租户 RBAC。
- 已增加 `OBSERVABILITY_API_KEYS` 的 tenant-to-secret 映射模式；请求必须同时提供 `X-API-Key` 与 `X-Tenant-ID`，业务 body/query tenant 也会再次校验。该模式仍缺少持久化密钥轮换和组织 RBAC。
- Agent 当前是确定性规则规划器，尚未接入模型 provider、token 预算、提示词版本和模型成本记录。
- 尚未完成支付商 webhook、订阅状态同步、退款和税务边界。
- 尚未完成 Cloudflare/Vercel 的真实部署与域名、密钥、日志验证。

## 下一验收顺序

1. 选择生产存储与队列，并保留当前 repository trait 级替换边界。
2. 完成 OTLP/批量 ingestion 的异步消费、重试和租户配额。
3. 接入一个模型 provider adapter，所有调用产生 Model Observation 和 UsageEntry。
4. 完成控制台 API 联调和认证流程。
5. 在 Linux CI、容器和目标云环境分别执行测试、smoke 和回滚演练。
