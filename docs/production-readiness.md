# Production readiness

## 已验证

- Rust workspace 在 WSL Linux 编译通过。
- API 与核心领域测试通过。
- `/health` 可真实启动并响应。
- Observation 与 usage ledger 可 JSONL 持久化并重启恢复。
- SQLite 单实例持久化摄取队列已覆盖原子批量入队、租约恢复、重试、死信和重放测试。
- Agent 决策返回 evidence IDs。
- 工具执行具备 allowlist 和 approval 状态。
- 月度报价使用整数分。

## 尚未达到生产标准

- JSONL 不是多实例一致性存储；生产需要 PostgreSQL/ClickHouse/SurrealDB 等明确选型。
- 已新增 `observability-sqlite` backend，适合单实例/边缘原型；Docker Compose 默认使用 SQLite + durable ingestion，但 API 裸启动仍默认 JSONL + direct。
- 已实现单实例持久化队列、背压入口、重试和死信；多实例场景仍需要外部队列、并发消费者协调、队列容量上限和租户配额。
- 当前批量 ingestion 限制为每请求 1000 条；该限制不是队列或租户级配额的替代品。
- 当前 API 以 8 个并发批次槽位提供入口背压，槽位耗尽返回 429；这不能替代租户级速率限制和磁盘容量保护。
- API key 不是完整身份系统；生产需要组织、项目、RBAC、密钥轮换和审计。
- CORS 当前为 permissive 原型配置，生产必须使用 allowlist。
- 已增加 `OBSERVABILITY_TENANT_ID` 单租户运行时边界；不匹配的请求返回 403，但这仍不是多租户 RBAC。
- 已增加 `OBSERVABILITY_API_KEYS` 的 tenant-to-secret 映射模式；请求必须同时提供 `X-API-Key` 与 `X-Tenant-ID`，业务 body/query tenant 也会再次校验。该模式仍缺少持久化密钥轮换和组织 RBAC。
- Agent 规划器仍是确定性规则；`/v1/model/complete` 已支持确定性 fallback 和 OpenAI-compatible provider，并记录 token usage，但尚缺 token 预算、提示词版本、超时/熔断和精确模型成本。
- 尚未完成支付商 webhook、订阅状态同步、退款和税务边界。
- 尚未完成 Cloudflare/Vercel 的真实部署与域名、密钥、日志验证。

## 下一验收顺序

1. 用 PostgreSQL/ClickHouse 与托管队列实现当前 repository trait，完成多实例一致性和容量保护。
2. 完成 OTLP 接入、租户速率/存储配额和数据保留策略。
3. 为模型 provider 增加预算、提示词版本、超时/熔断和真实成本映射。
4. 完成控制台 API 联调和认证流程。
5. 在 Linux CI、容器和目标云环境分别执行测试、smoke 和回滚演练。
