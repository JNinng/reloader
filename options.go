package reloader

import (
	"context"
	"fmt"
	"time"

	"github.com/jninng/observ"
)

// Option 是构造选项。与资源类型无关的选项直接实现本接口；
// 携带资源类型的选项（WithCloser）内部持有类型化闭包，在 New 中
// 经类型断言绑定到具体 T——与资源类型不匹配的 closer 选项会使
// New 返回错误（而非静默忽略）。
type Option interface{ option() }

type closerOption[T any] struct {
	fn func(ctx context.Context, res T) error
}

func (closerOption[T]) option() {}

// WithCloser 注入资源关闭函数，启用排空（Drain）：换下的旧资源经
// WithCloseDelay 宽限期后关闭。缺省 nil 为纯换新——不关闭、无排空，
// 适用于无泄漏风险的值类型资源（如配置快照）。T 由参数推断。
//
// 关闭函数一律以 context.Background() 执行——排空由独立 timer
// goroutine 触发、关停时无调用方 ctx 可透传，需要超时由关闭函数自行
// 包装；排空关闭的结果仅经 OnResourceClosed 事件与日志上报（失败
// warn、成功 info），不以返回值形式交付调用方。
func WithCloser[T any](close func(ctx context.Context, res T) error) Option {
	return closerOption[T]{fn: close}
}

type closeDelayOption time.Duration

func (closeDelayOption) option() {}

// WithCloseDelay 设置排空宽限期，缺省 DefaultCloseDelay，0 为换下后
// 立即（异步）关闭。宽限期内已持有旧资源的在途调用仍可使用它。
func WithCloseDelay(d time.Duration) Option { return closeDelayOption(d) }

type observerOption struct{ obs Observer }

func (observerOption) option() {}

// WithObserver 注入事件观察者，可多次注入（依序分发）。缺省无观察者。
func WithObserver(obs Observer) Option { return observerOption{obs} }

type meterOption struct{ m observ.Meter }

func (meterOption) option() {}

// WithMeter 注入指标出口，缺省 NoopMeter（零输出零开销）。
// 多个 Reloader 实例共用同一 Meter 时指标按 Meter 去重共享、
// 聚合为进程内库级口径（见 docs/adr/0003）。
func WithMeter(m observ.Meter) Option { return meterOption{m} }

type loggerOption struct{ l observ.Logger }

func (loggerOption) option() {}

// WithLogger 注入日志出口。缺省在构造期快照 observ.DefaultLogger()
// 并固定（快照语义，对齐 observ 接入手册）。
func WithLogger(l observ.Logger) Option { return loggerOption{l} }

// config 是 New 内部聚合的配置。
type config[T any] struct {
	closer    func(ctx context.Context, res T) error
	delay     time.Duration
	observers []Observer
	meter     observ.Meter
	logger    observ.Logger
}

func newConfig[T any](opts ...Option) (config[T], error) {
	cfg := config[T]{
		delay:  DefaultCloseDelay,
		meter:  observ.NoopMeter,
		logger: observ.DefaultLogger(),
	}
	for _, o := range opts {
		switch v := o.(type) {
		case closerOption[T]:
			cfg.closer = v.fn
		case closeDelayOption:
			cfg.delay = time.Duration(v)
		case observerOption:
			cfg.observers = append(cfg.observers, v.obs)
		case meterOption:
			cfg.meter = v.m
		case loggerOption:
			cfg.logger = v.l
		default:
			return cfg, fmt.Errorf("reloader: 未知或与资源类型不匹配的选项 %T", o)
		}
	}
	if cfg.meter == nil {
		cfg.meter = observ.NoopMeter
	}
	if cfg.logger == nil {
		cfg.logger = observ.NoopLogger
	}
	return cfg, nil
}
