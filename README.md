# reloader

事件源无关的资源热更新库：工厂构建新资源、成功后原子换上（携带世代号）、旧资源经宽限期排空、支持进程优雅关停。泛型管理任意目标资源（如 go-redis 客户端、配置快照），调用方经 `Current()` 直接使用资源原生 API。

## 特性

- **事件源无关**：何时重载由业务侧决定——配置监听（如 redis pubsub）回调里写配置槽并调用 `Reload`；工厂闭包自行读取最新配置（pull 语义，更新调用不携带配置参数）。
- **原子换新**：资源与世代号作为一个不可变整体经 `atomic.Pointer` 换上；`Current()` 在 `Shutdown` 前永不为零值。
- **世代（Generation）**：首建为 1、成功重载加 1、失败不消耗；经工厂参数、`Snapshot`、`Generation`、事件/指标/日志同源暴露，用于新旧资源识别与变更感知。
- **排空（Drain）**：换下的旧资源经宽限期（缺省 30s，可配）后关闭，在途持有者宽限期内仍可用；无 closer 时为纯换新，适配值类型配置快照。
- **串行 + 合并**：重载轮串行执行，执行期间到达的调用合并为至多一轮补跑（pull 语义下无损）。
- **优雅关停**：`Shutdown` 等待在途重载（受 ctx 约束）后立即关闭当前资源、提前触发未到期排空。
- **零依赖可观测**：对齐 [observ](https://github.com/jninng/observ) 手册——类型化非泛型 Observer、option 注入、回调同步执行且被 recover、指标无 label/枚举拆名。

## 安装

```bash
go get github.com/jninng/reloader
```

唯一第三方依赖为 `github.com/jninng/observ`（自身零依赖）。要求 Go 1.21+。

## 快速上手

### 热换 go-redis 客户端

```go
// 业务侧配置槽：配置监听写这里，工厂读这里。
var redisCfg atomic.Value // redis.Options

build := func(ctx context.Context, gen uint64) (*redis.Client, error) {
    opts := redisCfg.Load().(redis.Options)
    cli := redis.NewClient(&opts)
    if err := cli.Ping(ctx).Err(); err != nil { // 契约：返回即可用，预检在工厂内完成
        _ = cli.Close()
        return nil, err
    }
    return cli, nil
}

r, err := reloader.New(ctx, build,
    reloader.WithCloser(func(ctx context.Context, cli *redis.Client) error {
        return cli.Close() // 宽限期内旧客户端仍可用
    }),
)
if err != nil {
    return err // 首建失败 fail-fast
}

// 使用当前资源：直接调用原生 API
val, err := r.Current().Get(ctx, "key").Result()
```

### 对接配置监听（如 redis pubsub）

```go
sub := client.Subscribe(ctx, "config-changed")
go func() {
    for msg := range sub.Channel() {
        if opts, err := parseConfig(ctx, msg); err == nil {
            redisCfg.Store(opts) // 1. 写配置槽
        }
        _ = r.Reload(ctx)        // 2. 触发重载（同步返回结果，工厂 pull 最新配置）
    }
}()
```

### 世代与严格配对

```go
res, gen := r.Snapshot() // 单次原子载入，配对不会跨过重载
logger.Info("handle", slog.Uint64(reloader.AttrGeneration, gen))
```

### 进程优雅退出

```go
// 在 HTTP 服务排空在途请求之后调用：
// 关闭当前资源并提前触发所有未到期的排空；此后 Reload 返回 ErrShutdown。
_ = r.Shutdown(ctx)
```

## 生命周期与语义

| 阶段 | 行为 |
|---|---|
| `New` | 同步首建（世代 1，fail-fast）；成功后 `Current` 永不为零值 |
| `Reload` | 串行执行；成功原子换上、旧资源挂排空 timer；失败保旧、错误原样透传、世代不消耗；执行期到达的调用合并为一轮补跑 |
| 排空 | 宽限期（`WithCloseDelay`，缺省 30s，0 为立即）后关闭旧资源 |
| `Shutdown` | 等在途重载（ctx 约束，超时可重试）→ 立即关当前 → 提前触发未到期排空 → 幂等 |

业务错误原样透传；工厂/关闭/观察者的 panic 被 recover，不逃逸到其他调用方 goroutine。

## 可观测

- **Observer**（`WithObserver`，可多次注入）：`OnReloadStart` / `OnReloadEnd` / `OnResourceClosed`，事件按值、不含资源句柄，回调同步快速执行、panic 被 recover。
- **指标**（`WithMeter`，缺省 Noop）：`reloader_reload_success_total` / `reloader_reload_fail_total` / `reloader_reload_seconds` / `reloader_generation` / `reloader_reload_coalesced_total`。多实例共用同一 Meter 时按 Meter 去重共享、聚合为进程口径（[ADR-0003](docs/adr/0003-shared-metrics-per-meter.md)）。
- **日志**（`WithLogger`，缺省构造期快照 `observ.DefaultLogger()`）：仅低频生命周期事件；热路径（`Current`/`Snapshot`）零埋点。

## 仓库结构

```
github.com/jninng/reloader
├── reloader.go / options.go / observer.go / metrics.go / attr.go / doc.go
├── reloader_test.go / example_test.go   // -race 必过
├── examples/mockdb/                     // 可运行演示：mock db 完整生命周期
├── CONTEXT.md                            // 术语表（重载器/资源/工厂/世代/排空/关停）
└── docs/adr/                             // 设计决策记录
```

## 测试与演示

```bash
go test -race ./...
go run ./examples/mockdb   # 演示：热换/失败保旧/世代复用/排空/优雅关停
```

## License

[MIT](LICENSE)
