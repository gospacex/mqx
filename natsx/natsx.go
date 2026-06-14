package natsx

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx/utils"
)

// Reload 平滑热更新 NATS 连接池
func Reload(path string) error {
	log.Printf("[natsx] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil { return fmt.Errorf("reload parse error: %w", err) }

	cfg.Mode = "single"
	newCacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	_, err = getOrCreateProducerPool(newCacheKey, cfg)
	if err != nil { return fmt.Errorf("reload build new nats pool error: %w", err) }
	
	log.Printf("[natsx] hot-reload success.")
	return nil
}

// Shutdown 优雅关闭所有 natsx 管理的连接
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	producerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, pool *NatsPool) {
			defer wg.Done()
			log.Printf("[natsx] draining nats pool [key=%s]", k)
			pool.DrainAndClose()
			producerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*NatsPool))
		return true
	})

	// 省略 Consumer 的雷同代码

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[natsx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[natsx] shutdown timed out")
	}
}

func HealthCheck() map[string]string {
	result := make(map[string]string)
	producerCache.Range(func(key, value any) bool {
		pool := value.(*NatsPool)
		nc := pool.conns[0]
		if nc.IsClosed() {
			result[key.(string)] = "unhealthy (connection closed)"
		} else if !nc.IsConnected() {
			result[key.(string)] = "unhealthy (disconnected/reconnecting)"
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})
	return result
}
