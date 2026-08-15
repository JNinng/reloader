# 单类型参数 + 工厂闭包，更新参数不泛型化

Reloader 只对资源类型 `T` 泛型化；创建新资源所需的配置不进入类型参数（否决 `Reloader[T, C]` + `Update(ctx, C)` 方案），而是封装进业务侧提供的工厂闭包，`Reload(ctx)` 无参。理由：库对配置类型只透传、不解构不比较，泛型化收益仅剩调用侧类型安全，却要传染所有方法、选项与事件签名，且 Go 方法级类型参数不可推断；push 语义可由业务侧自持的 atomic 配置槽模拟。

## Considered Options

- `Reloader[T, C]` 双类型参数，`Update(ctx, cfg C)` 显式传配置 — 否决：C 在库内零逻辑，纯签名负担。
- 每次重载带工厂 `Reload(ctx, build)` — 否决：工厂归属（构造期 vs 调用期）模糊。
