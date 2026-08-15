// Package reloader 是事件源无关的资源热更新库：工厂构建新资源、成功
// 后原子换上（携带世代号）、旧资源经宽限期排空、支持进程优雅关停。
// 泛型管理任意目标资源（如 go-redis 客户端、配置快照），调用方经
// Current 直接使用资源原生 API。
//
// 核心语义（详见各 API 文档注释与 docs/adr/）：
//
//  1. 事件源无关：何时重载由业务侧决定——配置监听（如 redis pubsub）
//     的回调里写配置槽并调用 Reload；工厂闭包自行读取最新配置
//     （pull 语义，更新调用不携带配置参数，见 ADR-0001）。
//  2. 世代（Generation）：首次构建为 1，成功重载加 1、失败不消耗；
//     经工厂参数、Snapshot、Generation、事件/指标/日志同源暴露，
//     用于新旧资源识别与变更感知。
//  3. 原子换新：资源与世代作为一个不可变整体经 atomic.Pointer 换上；
//     Current 在 Shutdown 前永不为零值。
//  4. 排空（Drain）：换下的旧资源经 WithCloseDelay 宽限期后关闭
//     （缺省 30s，0 为立即）；不做引用计数（ADR-0002）。无 closer
//     时为纯换新，适配值类型配置快照。
//  5. 串行 + 合并：重载轮串行执行，执行期间到达的调用合并为至多
//     一轮补跑，合并者返回补跑轮结果。
//  6. 关停（Shutdown）：等待在途重载（受 ctx 约束）后立即关闭当前
//     资源并提前触发未到期排空；此后 Reload 返回 ErrShutdown、
//     Current 返回最后资源（已关闭，避免 nil 解引用）。
//  7. 错误透传：业务错误（工厂、关闭）原样返回；工厂/关闭/观察者的
//     panic 被 recover 转为错误或日志，不逃逸到其他调用方 goroutine。
//
// 可观测接入对齐 github.com/jninng/observ 手册：类型化非泛型 Observer
// （事件按值、不含资源句柄）、option 注入（WithMeter 默认 Noop、
// WithLogger 构造期快照）、回调同步快速执行且被 recover、指标仅
// 无 label 与枚举拆名两种形态（多实例按 Meter 去重共享，ADR-0003）。
// 日志仅记录低频生命周期事件，热路径（Current/Snapshot）零埋点。
package reloader
