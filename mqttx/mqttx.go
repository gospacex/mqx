package mqttx

import (
	"context"
	"fmt"
	"log"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Reload 平滑热更新 MQTT 连接
func Reload(path string) error {
	log.Printf("[mqttx] hot-reloading config from %s ...", path)

	cfg, _, err := parseFileFromDisk(path)
	if err != nil { return fmt.Errorf("reload parse error: %w", err) }

	cfg.Mode = "single"
	_, _, err = getOrCreateClient(cfg)
	if err != nil { return fmt.Errorf("reload build new mqtt client error: %w", err) }
	
	log.Printf("[mqttx] hot-reload success.")
	return nil
}

// Shutdown 优雅关闭所有 mqttx 管理的连接
func Shutdown(ctx context.Context) {
	var wg sync.WaitGroup

	clientCache.Range(func(key, value any) bool {
		wg.Add(1)
		go func(k string, client mqtt.Client) {
			defer wg.Done()
			log.Printf("[mqttx] disconnecting client [key=%s]", k)
			client.Disconnect(250)
			clientCache.Delete(k)
			clientLocks.Delete(k)
		}(key.(string), value.(mqtt.Client))
		return true
	})

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Println("[mqttx] all connections closed gracefully")
	case <-ctx.Done():
		log.Println("[mqttx] shutdown timed out")
	}
}

// HealthCheck 返回所有 MQTT 客户端的连接状态
func HealthCheck() map[string]string {
	result := make(map[string]string)
	clientCache.Range(func(key, value any) bool {
		client := value.(mqtt.Client)
		if client.IsConnectionOpen() {
			result[key.(string)] = "healthy (connected)"
		} else {
			result[key.(string)] = "unhealthy (disconnected)"
		}
		return true
	})
	return result
}
