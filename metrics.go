package reloader

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// 指标集（无 label、枚举拆名；计数 _total、耗时 _seconds）。多实例共用
// 同一 Meter 时按 Meter 去重共享，聚合为进程内库级口径；
// reloader_generation 为最近一次成功换新的世代（跨实例）。见 docs/adr/0003。
type metricSet struct {
	success    metric.Int64Counter
	fail       metric.Int64Counter
	coalesced  metric.Int64Counter
	seconds    metric.Float64Histogram
	generation metric.Int64Gauge
}

// reloadBuckets 一轮构建耗时的直方桶（秒）。
var reloadBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var (
	metricMu   sync.Mutex
	metricSets = map[metric.Meter]*metricSet{}
)

// newMetricSet 创建指标集。仪表创建出错时 otel 返回可用（noop）仪表，
// 错误可安全忽略——打点永不失败的语义由此保持。
func newMetricSet(m metric.Meter) *metricSet {
	success, _ := m.Int64Counter("reloader_reload_success_total",
		metric.WithDescription("成功完成的资源构建数（含首次构建与重载）"), metric.WithUnit("1"))
	fail, _ := m.Int64Counter("reloader_reload_fail_total",
		metric.WithDescription("构建失败的资源数（旧资源保留）"), metric.WithUnit("1"))
	coalesced, _ := m.Int64Counter("reloader_reload_coalesced_total",
		metric.WithDescription("被合并掉的重载请求数"), metric.WithUnit("1"))
	seconds, _ := m.Float64Histogram("reloader_reload_seconds",
		metric.WithDescription("一轮资源构建耗时"),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(reloadBuckets...))
	generation, _ := m.Int64Gauge("reloader_generation",
		metric.WithDescription("最近一次成功换新的资源世代"), metric.WithUnit("1"))
	return &metricSet{
		success:    success,
		fail:       fail,
		coalesced:  coalesced,
		seconds:    seconds,
		generation: generation,
	}
}

// metricsFor 返回该 Meter 的共享指标集；nil 返回 nil（整体跳过埋点）。
// Meter 动态类型不可比较时退化为每实例独立创建（ADR-0003 防御）。
func metricsFor(m metric.Meter) *metricSet {
	if m == nil {
		return nil
	}
	if set, ok := sharedSet(m); ok {
		return set
	}
	return newMetricSet(m) // 键不可比较：退化，本实例独立持有
}

// sharedSet 查共享缓存并写入；键不可比较（map 读写即 panic）时返回
// ok=false，由调用方退化为每实例直建。New* 的 panic 同样落入这里的
// recover，由退化直建原样重抛（工厂副作用因此执行两次）。
func sharedSet(m metric.Meter) (set *metricSet, ok bool) {
	defer func() {
		if recover() != nil {
			set, ok = nil, false
		}
	}()
	metricMu.Lock()
	defer metricMu.Unlock()
	if s, found := metricSets[m]; found {
		return s, true
	}
	s := newMetricSet(m)
	metricSets[m] = s
	return s, true
}

// observeBuild 记录一轮构建结果与耗时。ctx 仅作信息载体（trace 关联
// 由此生效），取消不构成丢点理由。
func (s *metricSet) observeBuild(ctx context.Context, err error, d time.Duration, gen uint64) {
	if s == nil {
		return
	}
	s.seconds.Record(ctx, d.Seconds())
	if err != nil {
		s.fail.Add(ctx, 1)
		return
	}
	s.success.Add(ctx, 1)
	s.generation.Record(ctx, int64(gen))
}

// observeCoalesced 记录一次被合并的重载请求。
func (s *metricSet) observeCoalesced(ctx context.Context) {
	if s == nil {
		return
	}
	s.coalesced.Add(ctx, 1)
}
