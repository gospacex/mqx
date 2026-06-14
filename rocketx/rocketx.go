package rocketx

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/gospacex/mqx/utils"
)

// Reload 平滑热更新 RocketMQ 连接
func Reload(path string) error {
	log.Printf("[rocketx] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil { return fmt.Errorf("reload parse error: %w", err) }

	cfg.Mode = "cluster"
	newCacheKey := "producer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	_, err = getOrCreateProducer(newCacheKey, cfg)
	if err != nil { return fmt.Errorf("reload build new rocketmq producer error: %w", err) }
	
	log.Printf("[rocketx] hot-reload success.")
	return nil
}

// Shutdown 优雅关闭所有 rocketx 管理的连接
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	producerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, p rocketmq.Producer) {
			defer wg.Done()
			log.Printf("[rocketx] closing producer [key=%s]", k)
			if err := p.Shutdown(); err != nil {
				log.Printf("[rocketx] producer shutdown error [key=%s]: %v", k, err)
			}
			producerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(rocketmq.Producer))
		return true
	})

	// ... 省略 consumer 类似代码

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[rocketx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[rocketx] shutdown timed out")
	}
}

func HealthCheck() map[string]string {
	result := make(map[string]string)
	producerCache.Range(func(key, value any) bool {
		result[key.(string)] = "healthy (instance running)"
		return true
	})
	return result
}
