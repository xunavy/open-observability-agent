# Model provider contract

模型调用不是黑盒副作用。每次 provider 请求都应关联：

- `tenant_id`
- `model`
- prompt/version 标识
- `evidence_ids`
- input/output token
- latency、HTTP 状态和错误分类
- provider request ID（如果供应商提供）

当前实现：

- `DeterministicModelProvider`：本地可测试，无网络依赖。
- `OpenAiCompatibleProvider`：CLI 中的 HTTP 适配器，支持显式 endpoint 和 API key 环境变量。
- API 服务端也支持通过 `MODEL_PROVIDER_URL` + `MODEL_API_KEY` 启用 OpenAI-compatible completion；未配置 endpoint 时使用 deterministic provider，配置 endpoint 但缺失 key 或上游失败会明确返回错误。

生产适配器要求：

1. 不记录完整 prompt 或敏感响应，除非租户策略明确允许。
2. 每次成功调用写入 `ObservationKind::Model`。
3. token 和 provider 成本写入 `UsageLedger`。
4. 超时、429、5xx 必须分类，不能无限重试。
5. 重试必须有预算，并避免重复计费。
6. provider API key 只能来自 secret manager 或运行时环境。

真实 provider 联调需要用户提供 endpoint、凭据和数据处理授权；当前仓库只对适配器编译和本地 provider 进行验证。
