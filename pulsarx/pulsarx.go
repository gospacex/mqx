package pulsarx

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx/utils"
)

// Reload 平滑热更新 Pulsar 连接
func Reload(path string) error {
	log.Printf("[pulsarx] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil { return fmt.Errorf("reload parse error: %w", err) }

	cfg.Mode = "single"
	newCacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	_, err = getOrCreateProducer(newCacheKey, cfg)
	if err != nil { return fmt.Errorf("reload build new pulsar producer error: %w", err) }
	
	log.Printf("[pulsarx] hot-reload success.")
	return nil
}

// Shutdown 优雅关闭所有 pulsarx 管理的连接
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	// 先关 Producer / Consumer
	producerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, p pulsar.Producer) {
			defer wg.Done()
			log.Printf("[pulsarx] closing producer [key=%s]", k)
			p.Close()
			producerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(pulsar.Producer))
		return true
	})

	time.Sleep(100 * time.Millisecond)

	// 最后关 Client (底层 TCP)
	clientCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, c pulsar.Client) {
			defer wg.Done()
			log.Printf("[pulsarx] closing client connection [key=%s]", k)
			c.Close()
			clientCache.Delete(k)
			clientLocks.Delete(k)
		}(key.(string), value.(pulsar.Client))
		return true
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[pulsarx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[pulsarx] shutdown timed out")
	}
}

func HealthCheck() map[string]string {
	result := make(map[string]string)
	clientCache.Range(func(key, value any) bool {
		result[key.(string)] = "healthy (instance running)"
		return true
	})
	return result
}
