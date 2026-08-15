# AGENTS.md

## 仓库用途

`reloader` 是事件源无关的资源热更新库：工厂构建、原子换新（世代号）、旧资源排空、优雅关停。术语以 `CONTEXT.md` 为准（重载器/资源/工厂/重载/世代/排空/关停），改核心语义前必读 `docs/adr/`。

## 仓库结构（单 module）

- 根模块 `github.com/jninng/reloader`：`reloader.go`（状态机）、`options.go`、`observer.go`、`metrics.go`、`attr.go`、`doc.go`
- `reloader_test.go`：fake 资源状态机测试；`example_test.go`：godoc 可运行示例
- `examples/mockdb/`：可运行演示（mock db 完整生命周期，仅 stdlib + observ）
- `CONTEXT.md`：术语表；`docs/adr/`：设计决策（0001 单类型参数、0002 宽限期排空、0003 指标按 Meter 共享）

## 常用命令（Windows / Git Bash）

```bash
go test -race ./...        # 契约要求并发安全，测试须过 -race
go vet ./...
```

## 依赖规则（硬约束）

- 根模块唯一第三方依赖 `github.com/jninng/observ`；不得引入其他第三方依赖（事件源如 redis 订阅归业务侧，README 演示写法）。
- 可观测接入对齐 observ 手册：类型化非泛型 Observer（事件按值、不含资源句柄）；option 注入（`WithMeter` 默认 Noop、`WithLogger` 构造期快照）；回调同步快速执行且由库 recover；指标仅无 label 与枚举拆名两种形态，多实例按 Meter 去重共享。

## API 约定

- `Option` 为非泛型接口；携带资源类型的选项（`WithCloser`）经类型断言绑定到 `T`，类型不匹配使 `New` 报错而非静默忽略。
- 不变量：`Current` 在 `Shutdown` 前永不为零值；世代失败不消耗；排空与关停对同一资源恰关闭一次；热路径（`Current`/`Snapshot`/`Generation`）单次原子读、零埋点。
- 错误：业务错误原样透传不包前缀；哨兵仅 `ErrShutdown`；工厂/关闭/观察者 panic 被 recover 转为错误或日志。
- 等待异步结果（如排空事件）时，测试须等待事件本身而非中间标志（关闭先于事件分发，存在窗口）。

## 文档与注释惯例

- 代码注释与文档使用中文；公共 API 必须有说明注释。
- 新术语进 `CONTEXT.md`（含 avoid 词表）；难逆转、无上下文会困惑、真实取舍的决策进 `docs/adr/`（格式见现有篇目）。
