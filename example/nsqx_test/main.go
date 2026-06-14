package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/mqx/nsqx"
	"github.com/gospacex/mqx/observability"
)

var shutdownTracing func(context.Context)

func main() {
	log.Println("=== MQX/Nsqx E2E 测试启动 ===")

	// 0. 读取 mq.yaml，根据 trace.enabled 决定是否启动 OTel 链路追踪。
	cfg, _, parseErr := nsqx.ParseFile("mq.yaml#nsq_cluster")
	if parseErr != nil {
		log.Printf("[main] failed to parse mq.yaml: %v (continuing with observability disabled)", parseErr)
		cfg = nil
	}
	if cfg != nil && cfg.Trace.Enabled {
		serviceName := cfg.Trace.ServiceName
		if serviceName == "" {
			serviceName = "nsqx-test"
		}
		endpoint := cfg.Trace.Endpoint
		if endpoint == "" {
			endpoint = "localhost:4318"
		}
		shutdownTracing, _ = observability.InitTracing(context.Background(), &observability.Config{
			Enabled:        true,
			JaegerEndpoint: endpoint,
			ServiceName:    serviceName,
			Insecure:       true,
			Headers:        cfg.Trace.Headers,
		})
	} else {
		log.Println("[main] trace disabled in config, OTel pipeline not started")
	}

	go runConsumerTest()
	time.Sleep(1 * time.Second)
	runProducerTest()

	log.Println("\n=== MQX 全局健康检查 ===")
	healthMap := nsqx.HealthCheck()
	for k, v := range healthMap {
		log.Printf("  -> %s : %s", k, v)
	}

	log.Println("\n=== [主程序挂起] 正在消费，请等待 3 秒或按 Ctrl+C 退出 ===")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-time.After(3 * time.Second):
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nsqx.Shutdown(ctx)
	if shutdownTracing != nil {
		shutdownTracing(ctx)
	}
	log.Println("=== MQX/Nsqx E2E 测试圆满结束 ===")
}
