# Contributing

感谢参与 Open Observability Agent。

## 开发原则

- Rust 领域核心必须保持供应商解耦。
- 任何 Agent 决策都必须返回可审计的 `evidence_ids`。
- 工具执行必须经过 allowlist 和审批边界。
- 新增 API 时必须保留 `tenant_id` 隔离。
- 不提交真实日志、密钥、租户数据或 `data/` 运行文件。

## 本地检查

```powershell
cargo fmt --all -- --check
cargo metadata --no-deps --format-version 1
cargo test --workspace
```

如果 Windows 工具链缺少链接器或 SDK，请先修复本机 Rust 编译环境，再提交测试结果。

