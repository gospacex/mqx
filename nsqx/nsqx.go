package nsqx

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx/utils"
	"github.com/nsqio/go-nsq"
)

// Reload 平滑热更新 NSQ 连接
func Reload(path string) error {
	log.Printf("[nsqx] hot-reloading config from %s ...", path)

	cfg, key, err := parseFileFromDisk(path)
	if err != nil { return fmt.Errorf("reload parse error: %w", err) }

	cfg.Mode = "single"
	newCacheKey := "producer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	_, err = getOrCreateProducer(newCacheKey, cfg)
	if err != nil { return fmt.Errorf("reload build new nsq producer error: %w", err) }
	
	log.Printf("[nsqx] hot-reload success.")
	return nil
}

// Shutdown 优雅关闭所有 nsqx 管理的连接
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	// 关闭所有 Producer
	producerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, p *nsq.Producer) {
			defer wg.Done()
			log.Printf("[nsqx] closing producer [key=%s]", k)
			p.Stop()
			producerCache.Delete(k)
			producerLocks.Delete(k)
		}(key.(string), value.(*nsq.Producer))
		return true
	})

	// 关闭所有 Consumer
	consumerCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, c *nsq.Consumer) {
			defer wg.Done()
			log.Printf("[nsqx] closing consumer [key=%s]", k)
			c.Stop()
			<-c.StopChan
			consumerCache.Delete(k)
			consumerLocks.Delete(k)
		}(key.(string), value.(*nsq.Consumer))
		return true
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[nsqx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[nsqx] shutdown timed out")
	}
}

func HealthCheck() map[string]string {
	result := make(map[string]string)

	producerCache.Range(func(key, value any) bool {
		p := value.(*nsq.Producer)
		if err := p.Ping(); err != nil {
			result[key.(string)] = "unhealthy: " + err.Error()
		} else {
			result[key.(string)] = "healthy"
		}
		return true
	})

	consumerCache.Range(func(key, value any) bool {
		result[key.(string)] = "healthy (running)"
		return true
	})

	return result
}
