package reloader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jninng/observ"
)

// ErrShutdown 在 Shutdown 之后调用 Reload 返回。
var ErrShutdown = errors.New("reloader: shutdown")

// DefaultCloseDelay 排空宽限期缺省值：换下的旧资源在此期限内关闭，
// 在途持有者可继续使用。
const DefaultCloseDelay = 30 * time.Second

// entry 当前生效资源的不可变封装：世代与资源作为一个整体原子换上。
type entry[T any] struct {
	gen uint64
	res T
}

// Reloader 接管某一类资源的生命周期：工厂构建新资源、成功后原子换上、
// 旧资源经宽限期排空、关停收尾。事件源无关——何时触发重载由业务侧
// 决定（如配置中心通知回调里调用 Reload）。
//
// 并发安全；零值不可用，经 New 构造。生成后 build/closer 等字段不可再改。
type Reloader[T any] struct {
	build  func(ctx context.Context, gen uint64) (T, error)
	closer func(ctx context.Context, res T) error
	delay  time.Duration

	logger    observ.Logger
	observers []Observer // 构造后只读
	ms        *metricSet // 构造后只读；nil 表示不埋点

	ptr atomic.Pointer[entry[T]]

	mu           sync.Mutex
	running      bool          // 有重载轮在执行
	pending      bool          // 执行期间到达的请求合并为至多一轮补跑
	seq          uint64        // 已完成的重载轮数（不含首次构建）
	lastErr      error         // 最近完成一轮的结果
	done         chan struct{} // 每轮完成即关闭并重建
	shutdown     bool          // 已请求关停
	shutdownDone bool          // 关停收尾完成

	shutdownClosed atomic.Bool // 当前资源的关停关闭只执行一次

	drains map[uint64]*drainTask[T] // 未触发的排空任务（gen → task）
}

// New 构造重载器并同步完成首次构建（世代 1，fail-fast）：build 失败
// 原样透传且不返回可用实例。此后 Current 永不为零值（直至 Shutdown
// 亦返回最后资源）。
//
// build 契约：返回 nil error 时资源必须已可用——拨号、握手、健康预检
// （如 Ping）由 build 自行完成后再返回。gen 为本次若成功将获得的
// 世代号，构建失败不消耗（重试复用同一号）；资源需自带世代时（如
// 包装进日志标记）直接使用该参数。
func New[T any](ctx context.Context, build func(ctx context.Context, gen uint64) (T, error), opts ...Option) (*Reloader[T], error) {
	if build == nil {
		return nil, errors.New("reloader: build 为 nil")
	}
	cfg, err := newConfig[T](opts...)
	if err != nil {
		return nil, err
	}
	r := &Reloader[T]{
		build:     build,
		closer:    cfg.closer,
		delay:     cfg.delay,
		logger:    cfg.logger,
		observers: cfg.observers,
		ms:        metricsFor(cfg.meter),
		done:      make(chan struct{}),
	}
	if cfg.closer != nil {
		r.drains = map[uint64]*drainTask[T]{}
	}
	if err := r.doReload(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Reload 触发一轮重载：工厂构建新资源，成功即原子换上并安排旧资源
// 排空；失败保留旧资源、世代不变，错误原样透传。
//
// 并发语义：重载轮串行执行；执行期间到达的调用合并为至多一轮补跑
// （pull 语义下无损），合并的调用返回补跑轮结果、计入
// reloader_reload_coalesced_total。ctx 仅透传给工厂——换新判定只看
// 工厂返回值（关闭函数一律收 context.Background()，见 WithCloser）；
// 等待补跑期间 ctx 取消立即返回 ctx.Err()，
// 补跑可能因此多执行一轮（无害）。Shutdown 之后返回 ErrShutdown。
func (r *Reloader[T]) Reload(ctx context.Context) error {
	r.mu.Lock()
	if r.shutdown {
		r.mu.Unlock()
		return ErrShutdown
	}
	if !r.running {
		r.running = true
		r.mu.Unlock()
		return r.runRounds(ctx)
	}
	// 有轮在执行：登记合并，等待补跑轮结果。
	r.pending = true
	r.ms.observeCoalesced()
	target := r.seq + 2
	ch := r.done
	r.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
		r.mu.Lock()
		switch {
		case r.seq >= target:
			err := r.lastErr
			r.mu.Unlock()
			return err
		case r.shutdown:
			r.mu.Unlock()
			return ErrShutdown
		case !r.running:
			// 执行者已交还执行权（如执行者 ctx 取消中断补跑链）而目标
			// 轮尚未完成：由本调用方接管补跑。
			r.running = true
			r.mu.Unlock()
			return r.runRounds(ctx)
		default:
			ch = r.done // 轮次更替，换新信号继续等
			r.mu.Unlock()
		}
	}
}

// runRounds 由抢到执行权的调用方循环执行重载轮：每轮完成广播结果，
// pending 存在则再补跑一轮。shutdown、执行者 ctx 取消或无 pending
// 时交还执行权并唤醒剩余等待者。
func (r *Reloader[T]) runRounds(ctx context.Context) error {
	var err error
	for {
		err = r.doReload(ctx)
		r.mu.Lock()
		r.lastErr = err
		r.seq++
		r.finishRoundLocked()
		cont := r.pending && !r.shutdown && ctx.Err() == nil
		if cont {
			r.pending = false
		} else {
			r.running = false
			r.finishRoundLocked() // 交还执行权：唤醒仍在等待的调用方
		}
		r.mu.Unlock()
		if !cont {
			return err
		}
	}
}

// finishRoundLocked 关闭当前完成信号并重建。mu 由调用方持有。
func (r *Reloader[T]) finishRoundLocked() {
	close(r.done)
	r.done = make(chan struct{})
}

// doReload 执行一轮构建：成功则原子换上、安排旧资源排空；失败保留
// 旧资源。首次构建（ptr 为空）无旧资源可排空。
func (r *Reloader[T]) doReload(ctx context.Context) error {
	nextGen := uint64(1)
	if old := r.ptr.Load(); old != nil {
		nextGen = old.gen + 1
	}
	start := time.Now()
	r.dispatch(func(o Observer) { o.OnReloadStart(ReloadStartEvent{Generation: nextGen}) })

	res, err := r.buildSafe(ctx, nextGen)
	dur := time.Since(start)

	r.ms.observeBuild(err, dur, nextGen)
	r.dispatch(func(o Observer) {
		o.OnReloadEnd(ReloadEndEvent{Generation: nextGen, Duration: dur, Err: err})
	})

	if err != nil {
		r.log(slog.LevelWarn, "reloader: build failed, keep current",
			slog.Uint64(AttrGeneration, nextGen),
			slog.String(observ.AttrErrorType, fmt.Sprintf("%T", err)),
			slog.Float64(observ.AttrDurationSeconds, dur.Seconds()))
		return err
	}

	old := r.ptr.Swap(&entry[T]{gen: nextGen, res: res})
	r.log(slog.LevelInfo, "reloader: resource swapped",
		slog.Uint64(AttrGeneration, nextGen))
	if old != nil && r.closer != nil {
		r.scheduleDrain(*old)
	}
	return nil
}

// buildSafe 执行工厂并 recover 其 panic（重载轮可能由其他调用方的
// goroutine 链式代跑，panic 不得逃离到无关调用方），转为错误返回。
func (r *Reloader[T]) buildSafe(ctx context.Context, gen uint64) (res T, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("reloader: build panic: %v", p)
		}
	}()
	return r.build(ctx, gen)
}

// closeSafe 执行关闭函数并 recover 其 panic，转为错误返回。
func (r *Reloader[T]) closeSafe(ctx context.Context, res T) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("reloader: closer panic: %v", p)
		}
	}()
	return r.closer(ctx, res)
}

// scheduleDrain 安排换下资源的排空关闭。
func (r *Reloader[T]) scheduleDrain(old entry[T]) {
	t := &drainTask[T]{r: r, ent: old}
	t.timer = time.AfterFunc(r.delay, func() { t.fire(CloseDrain) })
	r.mu.Lock()
	if r.drains != nil {
		r.drains[old.gen] = t
	}
	r.mu.Unlock()
}

// drainTask 一次排空任务：宽限期到点或关停提前触发，二者竞争同一
// 资源，CAS 保证仅一次生效。
type drainTask[T any] struct {
	r     *Reloader[T]
	ent   entry[T]
	timer *time.Timer
	fired atomic.Bool
}

// fire 关闭资源并移除任务。
func (t *drainTask[T]) fire(reason CloseReason) {
	if !t.fired.CompareAndSwap(false, true) {
		return
	}
	t.r.mu.Lock()
	delete(t.r.drains, t.ent.gen)
	t.r.mu.Unlock()
	t.timer.Stop()
	_ = t.r.closeEntry(context.Background(), t.ent, reason)
}

// Shutdown 关停重载器：置位关停标记，等待在途重载轮完成（受 ctx
// 约束；超时返回 ctx.Err()，此后可重试），随后立即关闭当前资源、
// 提前触发所有未到期的排空任务。
//
// 之后 Reload 返回 ErrShutdown，Current 返回最后资源（已关闭）。
// 关闭动作以 context.Background() 执行（ctx 仅约束等待）；关闭错误
// 原样透传、不重试（同时走事件与 warn 日志）。幂等：收尾完成后重复
// 调用返回 nil。
func (r *Reloader[T]) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.shutdownDone {
		r.mu.Unlock()
		return nil
	}
	r.shutdown = true
	for r.running {
		ch := r.done
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
		r.mu.Lock()
	}
	r.shutdownDone = true
	cur := r.ptr.Load()
	tasks := r.drains
	r.drains = map[uint64]*drainTask[T]{}
	r.mu.Unlock()

	var err error
	var gen uint64
	if cur != nil {
		gen = cur.gen
		if r.closer != nil && r.shutdownClosed.CompareAndSwap(false, true) {
			err = r.closeEntry(context.Background(), *cur, CloseShutdown)
		}
	}
	for _, t := range tasks {
		t.fire(CloseShutdown)
	}
	r.log(slog.LevelInfo, "reloader: shutdown",
		slog.Uint64(AttrGeneration, gen))
	return err
}

// closeEntry 关闭单个资源并分发事件与日志。closer 须非 nil。
func (r *Reloader[T]) closeEntry(ctx context.Context, e entry[T], reason CloseReason) error {
	err := r.closeSafe(ctx, e.res)
	r.dispatch(func(o Observer) {
		o.OnResourceClosed(ResourceClosedEvent{Generation: e.gen, Reason: reason, Err: err})
	})
	lvl := slog.LevelInfo
	if err != nil {
		lvl = slog.LevelWarn
	}
	r.log(lvl, "reloader: resource closed",
		slog.Uint64(AttrGeneration, e.gen),
		slog.String(AttrReason, string(reason)))
	return err
}

// Current 返回当前生效资源，直接使用其原生 API。热路径为单次原子读。
// Shutdown 前永不为零值；Shutdown 后返回最后资源（已关闭）——调用方
// 得到的是资源自身的关闭态错误而非 nil 解引用。
func (r *Reloader[T]) Current() T {
	return r.ptr.Load().res
}

// Snapshot 单次原子载入资源与世代，严格配对——分开调用 Current 与
// Generation 可能在两次调用间跨过一次重载。
func (r *Reloader[T]) Snapshot() (T, uint64) {
	e := r.ptr.Load()
	return e.res, e.gen
}

// Generation 原子读当前生效世代：首次构建为 1，每次成功重载加 1，
// 失败不消耗。
func (r *Reloader[T]) Generation() uint64 {
	return r.ptr.Load().gen
}

// dispatch 同步分发事件；单个观察者 panic 被 recover 并记日志
// （纯旁路，不得改变重载语义）。
func (r *Reloader[T]) dispatch(fn func(Observer)) {
	for _, o := range r.observers {
		func() {
			defer func() {
				if p := recover(); p != nil {
					r.log(slog.LevelError, "reloader: observer panic recovered",
						slog.String(observ.AttrErrorType, fmt.Sprintf("%v", p)))
				}
			}()
			fn(o)
		}()
	}
}

func (r *Reloader[T]) log(level slog.Level, msg string, attrs ...slog.Attr) {
	r.logger.Log(level, msg, attrs...)
}
