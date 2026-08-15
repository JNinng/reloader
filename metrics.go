package reloader

import (
	"sync"
	"time"

	"github.com/jninng/observ"
)

// 指标集（形态对齐 observ 规范：无 label、枚举拆名；计数 _total、
// 耗时 _seconds）。多实例共用同一 Meter 时按 Meter 去重共享，
// 聚合为进程内库级口径；reloader_generation 为最近一次成功换新的
// 世代（跨实例）。见 docs/adr/0003。
type metricSet struct {
	success    observ.Counter
	fail       observ.Counter
	coalesced  observ.Counter
	seconds    observ.Histogram
	generation observ.Gauge
}

// reloadBuckets 一轮构建耗时的直方桶（秒）。
var reloadBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var (
	metricMu   sync.Mutex
	metricSets = map[observ.Meter]*metricSet{}
)

func newMetricSet(m observ.Meter) *metricSet {
	return &metricSet{
		success:    m.NewCounter("reloader_reload_success_total", "成功完成的资源构建数（含首次构建与重载）"),
		fail:       m.NewCounter("reloader_reload_fail_total", "构建失败的资源数（旧资源保留）"),
		coalesced:  m.NewCounter("reloader_reload_coalesced_total", "被合并掉的重载请求数"),
		seconds:    m.NewHistogram("reloader_reload_seconds", "一轮资源构建耗时", reloadBuckets),
		generation: m.NewGauge("reloader_generation", "最近一次成功换新的资源世代"),
	}
}

// metricsFor 返回该 Meter 的共享指标集；nil 与 NoopMeter 返回 nil
// （整体跳过埋点）。Meter 动态类型不可比较时退化为每实例独立创建
// （ADR-0003 防御；仅 recover 映射键 panic，New* 的 panic 按契约
// 原样上抛）。
func metricsFor(m observ.Meter) (ms *metricSet) {
	if m == nil || m == observ.NoopMeter {
		return nil
	}
	metricMu.Lock()
	defer metricMu.Unlock()
	if set, ok := metricSets[m]; ok {
		return set
	}
	ms = newMetricSet(m)
	func() {
		defer func() { _ = recover() }() // 键不可比较：放弃共享，本实例独立持有
		metricSets[m] = ms
	}()
	return ms
}

// observeBuild 记录一轮构建结果与耗时。
func (s *metricSet) observeBuild(err error, d time.Duration, gen uint64) {
	if s == nil {
		return
	}
	s.seconds.Observe(d.Seconds())
	if err != nil {
		s.fail.Inc()
		return
	}
	s.success.Inc()
	s.generation.Set(float64(gen))
}

// observeCoalesced 记录一次被合并的重载请求。
func (s *metricSet) observeCoalesced() {
	if s == nil {
		return
	}
	s.coalesced.Inc()
}
