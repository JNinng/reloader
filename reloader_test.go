package reloader

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/observ"
)

// ---- 测试桩 ----

// fakeRes 假资源：id 即构建时的世代，关闭留痕且重复关闭报错。
type fakeRes struct {
	id     int
	closed atomic.Bool
}

func (f *fakeRes) Close() error {
	if f.closed.Swap(true) {
		return errors.New("fakeRes: already closed")
	}
	return nil
}

// buildLog 记录每次构建收到的世代参数与产出资源。
type buildLog struct {
	mu   sync.Mutex
	gens []uint64
	res  []*fakeRes
}

func (b *buildLog) record(gen uint64, r *fakeRes) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gens = append(b.gens, gen)
	b.res = append(b.res, r)
}

func (b *buildLog) snapshot() ([]uint64, []*fakeRes) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]uint64(nil), b.gens...), append([]*fakeRes(nil), b.res...)
}

// obsLog 记录 Observer 事件序列。
type obsRec struct {
	kind   string // start / end / closed
	gen    uint64
	reason CloseReason
	err    error
}

type obsLog struct {
	mu  sync.Mutex
	evs []obsRec
}

func (o *obsLog) add(e obsRec) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.evs = append(o.evs, e)
}

func (o *obsLog) snapshot() []obsRec {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]obsRec(nil), o.evs...)
}

func (o *obsLog) String() string {
	var b strings.Builder
	for _, e := range o.snapshot() {
		fmt.Fprintf(&b, "%s:%d", e.kind, e.gen)
		if e.kind == "closed" {
			fmt.Fprintf(&b, ":%s", e.reason)
		}
		b.WriteByte(' ')
	}
	return b.String()
}

// closedWhere 报告是否已观察到匹配的关闭事件：reason 为空匹配任意
// 原因；errOK 为 nil 跳过错误断言。
func (o *obsLog) closedWhere(gen uint64, reason CloseReason, errOK func(error) bool) bool {
	for _, e := range o.snapshot() {
		if e.kind != "closed" || e.gen != gen {
			continue
		}
		if reason != "" && e.reason != reason {
			continue
		}
		if errOK != nil && !errOK(e.err) {
			continue
		}
		return true
	}
	return false
}

func (o *obsLog) OnReloadStart(e ReloadStartEvent) {
	o.add(obsRec{kind: "start", gen: e.Generation})
}
func (o *obsLog) OnReloadEnd(e ReloadEndEvent) {
	o.add(obsRec{kind: "end", gen: e.Generation, err: e.Err})
}
func (o *obsLog) OnResourceClosed(e ResourceClosedEvent) {
	o.add(obsRec{kind: "closed", gen: e.Generation, reason: e.Reason, err: e.Err})
}

// panicObserver 三个回调全部 panic，用于验证分发点 recover。
type panicObserver struct{}

func (panicObserver) OnReloadStart(ReloadStartEvent) { panic("boom-start") }
func (panicObserver) OnReloadEnd(ReloadEndEvent)     { panic("boom-end") }
func (panicObserver) OnResourceClosed(ResourceClosedEvent) {
	panic("boom-closed")
}

// tMeter 实现 observ.Meter，带读回。
type tCounter struct {
	mu sync.Mutex
	v  float64
}

func (c *tCounter) Inc() { c.Add(1) }
func (c *tCounter) Add(v float64) {
	c.mu.Lock()
	c.v += v
	c.mu.Unlock()
}

type tGauge struct {
	mu sync.Mutex
	v  float64
}

func (g *tGauge) Set(v float64) { g.mu.Lock(); g.v = v; g.mu.Unlock() }
func (g *tGauge) Add(v float64) { g.mu.Lock(); g.v += v; g.mu.Unlock() }

type tHist struct {
	mu    sync.Mutex
	count int
}

func (h *tHist) Observe(float64) {
	h.mu.Lock()
	h.count++
	h.mu.Unlock()
}

type tMeter struct {
	mu       sync.Mutex
	counters map[string]*tCounter
	gauges   map[string]*tGauge
	hists    map[string]*tHist
}

func (m *tMeter) NewCounter(name, _ string) observ.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := &tCounter{}
	if m.counters == nil {
		m.counters = map[string]*tCounter{}
	}
	m.counters[name] = c
	return c
}

func (m *tMeter) NewGauge(name, _ string) observ.Gauge {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := &tGauge{}
	if m.gauges == nil {
		m.gauges = map[string]*tGauge{}
	}
	m.gauges[name] = g
	return g
}

func (m *tMeter) NewHistogram(name, _ string, _ []float64) observ.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := &tHist{}
	if m.hists == nil {
		m.hists = map[string]*tHist{}
	}
	m.hists[name] = h
	return h
}

func (m *tMeter) counter(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[name]; ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.v
	}
	return 0
}

func (m *tMeter) gauge(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g, ok := m.gauges[name]; ok {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.v
	}
	return 0
}

// badMeter 动态类型不可比较（含切片字段）的 Meter：验证共享缓存的
// 退化路径（ADR-0003：不可比较键退化为每实例直建而非 panic）。
type badMeter struct{ _ []int }

func (badMeter) NewCounter(name, _ string) observ.Counter                  { return &tCounter{} }
func (badMeter) NewGauge(name, _ string) observ.Gauge                      { return &tGauge{} }
func (badMeter) NewHistogram(name, _ string, _ []float64) observ.Histogram { return &tHist{} }

// waitFor 在期限内轮询条件。
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

// recordingBuild 返回记录世代的 build：res.id == gen，用于配对校验。
func recordingBuild(bl *buildLog) func(ctx context.Context, gen uint64) (*fakeRes, error) {
	return func(ctx context.Context, gen uint64) (*fakeRes, error) {
		res := &fakeRes{id: int(gen)}
		bl.record(gen, res)
		return res, nil
	}
}

// fakeCloser 关闭假资源。
func fakeCloser() func(ctx context.Context, res *fakeRes) error {
	return func(ctx context.Context, res *fakeRes) error { return res.Close() }
}

// ---- 构造与初始状态 ----

func TestNewInitialState(t *testing.T) {
	bl := &buildLog{}
	r, err := New(context.Background(), recordingBuild(bl))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if g := r.Generation(); g != 1 {
		t.Fatalf("generation = %d, want 1", g)
	}
	if cur := r.Current(); cur == nil || cur.id != 1 {
		t.Fatalf("current = %+v, want id 1", cur)
	}
	res, g := r.Snapshot()
	if g != 1 || res.id != 1 {
		t.Fatalf("snapshot = (%+v, %d), want (id 1, 1)", res, g)
	}
	gens, _ := bl.snapshot()
	if len(gens) != 1 || gens[0] != 1 {
		t.Fatalf("build gens = %v, want [1]", gens)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestNewBuildFail(t *testing.T) {
	boom := errors.New("cfg down")
	m := &tMeter{}
	_, err := New(context.Background(),
		func(ctx context.Context, gen uint64) (*fakeRes, error) { return nil, boom },
		WithMeter(m))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want 原样透传 boom", err)
	}
	if got := m.counter("reloader_reload_fail_total"); got != 1 {
		t.Fatalf("fail_total = %v, want 1", got)
	}
	if got := m.counter("reloader_reload_success_total"); got != 0 {
		t.Fatalf("success_total = %v, want 0", got)
	}
}

func TestNewNilBuild(t *testing.T) {
	if _, err := New[*fakeRes](context.Background(), nil); err == nil {
		t.Fatal("nil build 应返回错误")
	}
}

func TestNewTypeMismatchedCloser(t *testing.T) {
	// closer 绑定了别的资源类型：应报错而非静默忽略。
	_, err := New(context.Background(), recordingBuild(&buildLog{}),
		WithCloser(func(ctx context.Context, s *string) error { return nil }))
	if err == nil {
		t.Fatal("类型不匹配的 closer 选项应使 New 报错")
	}
}

// ---- 重载语义 ----

func TestReloadPureSwap(t *testing.T) {
	m := &tMeter{}
	r, err := New(context.Background(), recordingBuild(&buildLog{}), WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}
	old := r.Current()
	if err := r.Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if g := r.Generation(); g != 2 {
		t.Fatalf("generation = %d, want 2", g)
	}
	if cur := r.Current(); cur.id != 2 {
		t.Fatalf("current id = %d, want 2", cur.id)
	}
	if old.closed.Load() {
		t.Fatal("无 closer 时旧资源不应被关闭")
	}
	if got := m.counter("reloader_reload_success_total"); got != 2 { // 首建 + 1 次重载
		t.Fatalf("success_total = %v, want 2", got)
	}
	if got := m.gauge("reloader_generation"); got != 2 {
		t.Fatalf("generation gauge = %v, want 2", got)
	}
	_ = r.Shutdown(context.Background())
}

func TestReloadFailKeepsOldAndGenNotConsumed(t *testing.T) {
	boom := errors.New("build boom")
	m := &tMeter{}
	bl := &buildLog{}
	var fails atomic.Int32
	build := func(ctx context.Context, gen uint64) (*fakeRes, error) {
		if fails.Add(1) == 2 { // 第二次调用（首次重载）失败
			return nil, boom
		}
		return recordingBuild(bl)(ctx, gen)
	}
	r, err := New(context.Background(), build, WithMeter(m), WithCloser(fakeCloser()))
	if err != nil {
		t.Fatal(err)
	}
	cur := r.Current()

	err = r.Reload(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want 原样透传 boom", err)
	}
	if g := r.Generation(); g != 1 {
		t.Fatalf("失败后 generation = %d, want 1", g)
	}
	if r.Current() != cur {
		t.Fatal("失败后 current 应保持不变")
	}
	if cur.closed.Load() {
		t.Fatal("当前资源不应被关闭")
	}

	// 失败不消耗世代：下次成功仍为 2。
	if err := r.Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if g := r.Generation(); g != 2 {
		t.Fatalf("generation = %d, want 2（失败不消耗世代）", g)
	}
	gens, _ := bl.snapshot()
	if len(gens) != 2 || gens[0] != 1 || gens[1] != 2 {
		t.Fatalf("build gens = %v, want [1 2]", gens)
	}
	_ = r.Shutdown(context.Background())
}

func TestDrainClosesOld(t *testing.T) {
	obs := &obsLog{}
	r, err := New(context.Background(), recordingBuild(&buildLog{}),
		WithCloser(fakeCloser()),
		WithCloseDelay(20*time.Millisecond),
		WithObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	old := r.Current()
	if err := r.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 等事件本身（资源关闭先于事件分发，等 closed 标志会有窗口）。
	waitFor(t, 2*time.Second, func() bool { return obs.closedWhere(1, CloseDrain, nil) })
	if !old.closed.Load() {
		t.Fatal("事件已观察到但资源未关闭")
	}
}

func TestDrainZeroDelay(t *testing.T) {
	r, err := New(context.Background(), recordingBuild(&buildLog{}),
		WithCloser(fakeCloser()), WithCloseDelay(0))
	if err != nil {
		t.Fatal(err)
	}
	old := r.Current()
	if err := r.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return old.closed.Load() })
	_ = r.Shutdown(context.Background())
}

func TestBuildPanicRecovered(t *testing.T) {
	r, err := New(context.Background(), recordingBuild(&buildLog{}), WithCloser(fakeCloser()))
	if err != nil {
		t.Fatal(err)
	}
	cur := r.Current()
	r.build = func(ctx context.Context, gen uint64) (*fakeRes, error) { panic("boom") }
	err = r.Reload(context.Background())
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("err = %v, want 包含 panic 的错误", err)
	}
	if r.Generation() != 1 || r.Current() != cur {
		t.Fatal("panic 后应保留旧资源")
	}
	_ = r.Shutdown(context.Background())
}

func TestCloserPanicRecovered(t *testing.T) {
	obs := &obsLog{}
	r, err := New(context.Background(), recordingBuild(&buildLog{}),
		WithCloser(func(ctx context.Context, res *fakeRes) error { panic("close boom") }),
		WithCloseDelay(0),
		WithObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	old := r.Current()
	if err := r.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return obs.closedWhere(1, CloseDrain, func(err error) bool {
			return err != nil && strings.Contains(err.Error(), "panic")
		})
	})
	_ = old
	_ = r.Shutdown(context.Background())
}

// ---- 合并（coalesce） ----

func TestReloadCoalesce(t *testing.T) {
	m := &tMeter{}
	r, err := New(context.Background(), recordingBuild(&buildLog{}), WithMeter(m), WithCloser(fakeCloser()))
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	r.build = func(ctx context.Context, gen uint64) (*fakeRes, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return &fakeRes{id: int(gen)}, nil
	}

	errs := make(chan error, 4)
	go func() { errs <- r.Reload(context.Background()) }()
	<-entered // 第一轮 build 进行中

	for i := 0; i < 3; i++ {
		go func() { errs <- r.Reload(context.Background()) }()
	}
	// 等待三个合并请求全部登记完毕（登记与 coalesced 计数同临界区），
	// 否则迟登记的等待者会合法要求又一轮补跑。
	waitFor(t, 2*time.Second, func() bool {
		return m.counter("reloader_reload_coalesced_total") == 3
	})

	release <- struct{}{} // 第一轮完成 → 补跑第二轮
	<-entered
	release <- struct{}{} // 第二轮完成

	for i := 0; i < 4; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("reload err = %v, want nil", err)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("build 调用 %d 次, want 2（第一轮 + 一轮补跑）", got)
	}
	if got := m.counter("reloader_reload_coalesced_total"); got != 3 {
		t.Fatalf("coalesced_total = %v, want 3", got)
	}
	if got := m.counter("reloader_reload_success_total"); got != 3 { // 首建 + 2 轮
		t.Fatalf("success_total = %v, want 3", got)
	}
	_ = r.Shutdown(context.Background())
}

// ---- 关停 ----

func TestShutdownClosesCurrentAndFiresPendingDrains(t *testing.T) {
	obs := &obsLog{}
	r, err := New(context.Background(), recordingBuild(&buildLog{}),
		WithCloser(fakeCloser()),
		WithCloseDelay(10*time.Minute), // 未到期
		WithObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	old1 := r.Current()
	if err := r.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	cur2 := r.Current()

	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if !cur2.closed.Load() {
		t.Fatal("当前资源应立即关闭")
	}
	if !old1.closed.Load() {
		t.Fatal("未到期的排空任务应被提前触发")
	}
	if err := r.Reload(context.Background()); !errors.Is(err, ErrShutdown) {
		t.Fatalf("reload after shutdown = %v, want ErrShutdown", err)
	}
	if r.Generation() != 2 || r.Current() != cur2 {
		t.Fatal("关停后应保留最后资源与世代")
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("重复 Shutdown 应幂等返回 nil, got %v", err)
	}

	// 事件断言：两个 closed（各一次），无 drain 原因残留。
	closed := 0
	for _, e := range obs.snapshot() {
		if e.kind == "closed" {
			closed++
		}
	}
	if closed != 2 {
		t.Fatalf("closed 事件数 = %d, want 2：%s", closed, obs)
	}
}

func TestShutdownWaitsInFlightReload(t *testing.T) {
	r, err := New(context.Background(), recordingBuild(&buildLog{}), WithCloser(fakeCloser()))
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	r.build = func(ctx context.Context, gen uint64) (*fakeRes, error) {
		entered <- struct{}{}
		<-release
		return &fakeRes{id: int(gen)}, nil
	}
	reloadErr := make(chan error, 1)
	go func() { reloadErr <- r.Reload(context.Background()) }()
	<-entered

	shCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.Shutdown(shCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown err = %v, want DeadlineExceeded", err)
	}

	release <- struct{}{} // 放行在途重载
	waitFor(t, 2*time.Second, func() bool { return r.Generation() == 2 })
	if err := <-reloadErr; err != nil {
		t.Fatalf("在途重载应完成而非中断, err = %v", err)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("重试 shutdown: %v", err)
	}
	if !r.Current().closed.Load() {
		t.Fatal("当前资源应被关闭")
	}
}

func TestShutdownNoCloser(t *testing.T) {
	r, err := New(context.Background(), recordingBuild(&buildLog{}))
	if err != nil {
		t.Fatal(err)
	}
	cur := r.Current()
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if r.Current() != cur {
		t.Fatal("无 closer 时资源保持原样")
	}
	if err := r.Reload(context.Background()); !errors.Is(err, ErrShutdown) {
		t.Fatalf("reload = %v, want ErrShutdown", err)
	}
}

// ---- Observer ----

func TestObserverEventSequence(t *testing.T) {
	obs := &obsLog{}
	r, err := New(context.Background(), recordingBuild(&buildLog{}),
		WithCloser(fakeCloser()), WithCloseDelay(0), WithObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return obs.closedWhere(1, "", nil) })
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := "start:1 end:1 start:2 end:2 closed:1:drain closed:2:shutdown "
	if got := obs.String(); got != want {
		t.Fatalf("事件序列 = %q, want %q", got, want)
	}
}

func TestObserverPanicRecovered(t *testing.T) {
	r, err := New(context.Background(), recordingBuild(&buildLog{}),
		WithCloser(fakeCloser()), WithCloseDelay(0), WithObserver(panicObserver{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Reload(context.Background()); err != nil {
		t.Fatalf("observer panic 不应影响重载: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return r.Generation() == 2 })
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("observer panic 不应影响关停: %v", err)
	}
}

// ---- 指标共享（ADR-0003） ----

func TestMetricsSharedPerMeter(t *testing.T) {
	m1, m2 := &tMeter{}, &tMeter{}
	mk := func(m *tMeter) *Reloader[*fakeRes] {
		t.Helper()
		r, err := New(context.Background(), recordingBuild(&buildLog{}), WithMeter(m))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r1, r2, r3 := mk(m1), mk(m1), mk(m2) // r1/r2 共享 m1
	for _, r := range []*Reloader[*fakeRes]{r1, r2, r3} {
		if err := r.Reload(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := m1.counter("reloader_reload_success_total"); got != 4 { // 2 首建 + 2 重载
		t.Fatalf("m1 success_total = %v, want 4（跨实例聚合）", got)
	}
	if got := m2.counter("reloader_reload_success_total"); got != 2 {
		t.Fatalf("m2 success_total = %v, want 2（Meter 间隔离）", got)
	}
	if got := m1.gauge("reloader_generation"); got != 2 {
		t.Fatalf("m1 generation = %v, want 2", got)
	}
}

func TestMetricsNonComparableMeterDegrades(t *testing.T) {
	m := badMeter{}
	mk := func() *Reloader[*fakeRes] {
		t.Helper()
		r, err := New(context.Background(), recordingBuild(&buildLog{}), WithMeter(m))
		if err != nil {
			t.Fatalf("不可比较 Meter 不应使 New 报错: %v", err)
		}
		return r
	}
	r1, r2 := mk(), mk() // 各自独立直建：不共享、不 panic
	for _, r := range []*Reloader[*fakeRes]{r1, r2} {
		if err := r.Reload(context.Background()); err != nil {
			t.Fatal(err)
		}
		if g := r.Generation(); g != 2 {
			t.Fatalf("generation = %d, want 2", g)
		}
	}
}

// ---- 并发风暴 ----

func TestConcurrentReloadStorm(t *testing.T) {
	m := &tMeter{}
	obs := &obsLog{}
	bl := &buildLog{}
	r, err := New(context.Background(),
		func(ctx context.Context, gen uint64) (*fakeRes, error) {
			time.Sleep(time.Millisecond) // 拉长构建，制造合并窗口
			res := &fakeRes{id: int(gen)}
			bl.record(gen, res)
			return res, nil
		},
		WithCloser(fakeCloser()),
		WithCloseDelay(time.Millisecond),
		WithObserver(obs),
		WithMeter(m))
	if err != nil {
		t.Fatal(err)
	}

	// 配对锤：Snapshot 的资源 id 必须与世代一致。
	stop := make(chan struct{})
	var hammerWG sync.WaitGroup
	hammerWG.Add(1)
	go func() {
		defer hammerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if res, gen := r.Snapshot(); res.id != int(gen) {
					t.Errorf("snapshot 配对破坏: res.id=%d gen=%d", res.id, gen)
					return
				}
			}
		}
	}()

	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if err := r.Reload(context.Background()); err != nil {
					t.Errorf("reload: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	hammerWG.Wait()

	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	gens, res := bl.snapshot()
	if len(gens) == 0 || gens[0] != 1 {
		t.Fatalf("首次构建世代 = %v, want 1", gens)
	}
	for i := 1; i < len(gens); i++ {
		if gens[i] != gens[i-1]+1 {
			t.Fatalf("世代序列断裂: %v", gens)
		}
	}
	for _, rs := range res {
		if !rs.closed.Load() {
			t.Fatalf("资源 %d 在关停后未关闭", rs.id)
		}
	}
	closed := 0
	for _, e := range obs.snapshot() {
		if e.kind == "closed" {
			closed++
		}
	}
	if closed != len(res) {
		t.Fatalf("closed 事件 %d 个, want %d（每个资源恰一次）", closed, len(res))
	}
	if g := r.Generation(); g != uint64(len(gens)) {
		t.Fatalf("最终世代 = %d, want %d", g, len(gens))
	}
}
