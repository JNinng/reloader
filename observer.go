package reloader

import "time"

// CloseReason 资源被关闭的原因。
type CloseReason string

const (
	// CloseDrain 排空到点：重载换下的旧资源经宽限期后关闭。
	CloseDrain CloseReason = "drain"

	// CloseShutdown 关停：重载器 Shutdown 关闭当前资源，
	// 或提前触发未到期的排空任务。
	CloseShutdown CloseReason = "shutdown"
)

// ReloadStartEvent 一轮构建开始。Generation 为本次若成功将生效的世代
// （构建失败不消耗，重试将复用同一世代号）。
type ReloadStartEvent struct {
	Generation uint64
}

// ReloadEndEvent 一轮构建结束。Err 非 nil 表示构建失败、旧资源保留。
// 事件覆盖首次构建（Generation 为 1）。
type ReloadEndEvent struct {
	Generation uint64
	Duration   time.Duration
	Err        error
}

// ResourceClosedEvent 一个资源被关闭（排空到点或关停，含关停提前触发）。
type ResourceClosedEvent struct {
	Generation uint64
	Reason     CloseReason
	Err        error
}

// Observer 重载器生命周期事件的观察者。事件按值传递、不含资源句柄——
// 需要当前资源时调用 Reloader.Current / Snapshot。
//
// 回调在触发点所在 goroutine 同步执行，必须快速返回（微秒级）；
// panic 由重载器 recover（纯旁路，不得改变重载语义）。
type Observer interface {
	OnReloadStart(ReloadStartEvent)
	OnReloadEnd(ReloadEndEvent)
	OnResourceClosed(ResourceClosedEvent)
}
