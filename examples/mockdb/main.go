// Command mockdb 演示 reloader 管理一个 mock 数据库客户端的完整生命周期：
// 配置槽更新 + 触发重载、失败保旧、世代复用、排空宽限期、优雅关停。
//
// 运行：go run ./examples/mockdb
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/reloader"
)

// ---- mock 数据库客户端 ----

var errClosed = errors.New("mockdb: closed")

// mockDB 模拟需要热更新的目标资源（真实场景如 sql.DB / *redis.Client，
// 调用方经 Current 直接使用其原生 API）。
type mockDB struct {
	id     uint64 // 实例编号，即构建时分配到的世代
	dsn    string
	closed atomic.Bool
}

// Ping 模拟连接预检。
func (db *mockDB) Ping() error {
	if db.closed.Load() {
		return errClosed
	}
	return nil
}

// Query 模拟业务查询：返回值携带世代与 dsn，便于观察新旧切换。
func (db *mockDB) Query(table string) string {
	if db.closed.Load() {
		return fmt.Sprintf("ERR(%v) table=%s", errClosed, table)
	}
	return fmt.Sprintf("rows gen=%d dsn=%s table=%s", db.id, db.dsn, table)
}

// Close 幂等关闭。
func (db *mockDB) Close() error {
	if db.closed.Swap(true) {
		return errClosed
	}
	return nil
}

// ---- 模拟配置中心 ----

// dbConfig 模拟一份会热更的数据库配置。
type dbConfig struct {
	DSN string
}

// cfgSlot 配置槽：业务侧配置监听（如 redis pubsub 回调）写这里，
// 工厂闭包读这里——pull 语义。
var cfgSlot atomic.Value // dbConfig

// ---- 事件观察者 ----

// printObserver 打印生命周期事件，演示类型化 Observer 接入。
type printObserver struct{}

func (printObserver) OnReloadStart(e reloader.ReloadStartEvent) {
	fmt.Printf("[event] reload start   gen=%d\n", e.Generation)
}

func (printObserver) OnReloadEnd(e reloader.ReloadEndEvent) {
	if e.Err != nil {
		fmt.Printf("[event] reload fail    gen=%d err=%v\n", e.Generation, e.Err)
	} else {
		fmt.Printf("[event] reload ok      gen=%d (%.3fs)\n", e.Generation, e.Duration.Seconds())
	}
}

func (printObserver) OnResourceClosed(e reloader.ResourceClosedEvent) {
	fmt.Printf("[event] resource closed gen=%d reason=%s err=%v\n", e.Generation, e.Reason, e.Err)
}

func main() {
	ctx := context.Background()

	// 工厂：pull 最新配置构建 mock db；gen 为本次若成功将生效的世代，
	// 失败不消耗。契约：返回 nil error 时资源已可用（预检在工厂内完成）。
	build := func(ctx context.Context, gen uint64) (*mockDB, error) {
		cfg := cfgSlot.Load().(dbConfig)
		if cfg.DSN == "" {
			return nil, errors.New("mockdb: empty dsn")
		}
		time.Sleep(80 * time.Millisecond) // 模拟拨号 + 握手
		db := &mockDB{id: gen, dsn: cfg.DSN}
		if err := db.Ping(); err != nil {
			return nil, err
		}
		return db, nil
	}
	closer := func(ctx context.Context, db *mockDB) error {
		return db.Close()
	}

	cfgSlot.Store(dbConfig{DSN: "postgres://v1 host=10.0.0.1"})
	r, err := reloader.New(ctx, build,
		reloader.WithCloser(closer),
		reloader.WithCloseDelay(300*time.Millisecond), // 演示排空窗口
		reloader.WithObserver(printObserver{}),
		reloader.WithLogger(observ.NewSlogLogger(slog.Default())),
	)
	if err != nil {
		panic(err) // 首建失败 fail-fast
	}

	fmt.Println("== 1. 初始构建（世代 1），经 Current 直接使用原生 API ==")
	fmt.Println("   ", r.Current().Query("users"))

	fmt.Println("== 2. 模拟配置通知：写配置槽 + Reload（同步返回）==")
	cfgSlot.Store(dbConfig{DSN: "postgres://v2 host=10.0.0.2"})
	old := r.Current() // 预先持有旧句柄，演示排空
	if err := r.Reload(ctx); err != nil {
		panic(err)
	}
	res, gen := r.Snapshot() // 严格配对：资源与世代单次原子载入
	fmt.Printf("    current gen=%d: %s\n", gen, res.Query("orders"))
	fmt.Printf("    旧句柄(gen=%d) 宽限期内仍可用: %s\n", old.id, old.Query("orders"))

	fmt.Println("== 3. 排空到点后，旧句柄被关闭 ==")
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("    旧句柄(gen=%d) 排空后: %s\n", old.id, old.Query("orders"))

	fmt.Println("== 4. 构建失败：错误原样透传、旧资源保留、世代不消耗 ==")
	cfgSlot.Store(dbConfig{DSN: ""}) // 模拟坏配置
	if err := r.Reload(ctx); err != nil {
		fmt.Printf("    reload err: %v\n", err)
	}
	fmt.Printf("    世代仍为 %d: %s\n", r.Generation(), r.Current().Query("users"))

	fmt.Println("== 5. 恢复配置重试：复用同一世代号（3）==")
	cfgSlot.Store(dbConfig{DSN: "postgres://v3 host=10.0.0.3"})
	if err := r.Reload(ctx); err != nil {
		panic(err)
	}
	fmt.Printf("    世代 %d: %s\n", r.Generation(), r.Current().Query("users"))

	fmt.Println("== 6. 优雅关停：立即关闭当前资源（含未到期的排空）==")
	if err := r.Shutdown(ctx); err != nil {
		fmt.Printf("    shutdown err: %v\n", err)
	}
	fmt.Printf("    current(gen=%d) 关停后: %s\n", r.Generation(), r.Current().Query("users"))
	err = r.Reload(ctx)
	fmt.Printf("    reload after shutdown: %v (errors.Is ErrShutdown = %v)\n", err, errors.Is(err, reloader.ErrShutdown))
}
