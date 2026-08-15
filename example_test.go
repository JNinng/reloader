package reloader_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jninng/reloader"
)

// demoClient 模拟被热更新管理的目标资源（真实场景如 *redis.Client，
// 可直接使用其原生 API）。
type demoClient struct {
	addr string
	gen  uint64 // 资源自带世代，附加到业务日志用于新旧识别
}

func Example() {
	// 业务侧配置槽：配置监听（如 redis pubsub 通知）写这里。
	var addr atomic.Value
	addr.Store("10.0.0.1:6379")

	// 工厂闭包：pull 语义——自行读取最新配置构建资源。
	// gen 为本次若成功将生效的世代；真实场景在此完成拨号与 Ping 自验。
	build := func(ctx context.Context, gen uint64) (*demoClient, error) {
		a, _ := addr.Load().(string)
		if a == "" {
			return nil, errors.New("empty addr")
		}
		return &demoClient{addr: a, gen: gen}, nil
	}
	closeFn := func(ctx context.Context, c *demoClient) error {
		return nil // 真实场景：c.Close()，宽限期内旧客户端仍可用
	}

	r, err := reloader.New(context.Background(), build,
		reloader.WithCloser(closeFn),
		reloader.WithCloseDelay(reloader.DefaultCloseDelay))
	if err != nil {
		panic(err) // 首建失败 fail-fast
	}

	// 业务侧事件回调里：写配置槽 + 触发重载（同步返回结果）。
	addr.Store("10.0.0.2:6379")
	if err := r.Reload(context.Background()); err != nil {
		panic(err)
	}

	// 使用当前资源：直接调用原生 API；Snapshot 严格配对资源与世代。
	res, gen := r.Snapshot()
	fmt.Println(gen, res.addr, r.Generation() == gen)

	// 进程优雅退出时收尾：关闭当前资源并提前触发未到期的排空。
	_ = r.Shutdown(context.Background())
	// Output: 2 10.0.0.2:6379 true
}
