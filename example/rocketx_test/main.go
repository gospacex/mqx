package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/rocketx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var shutdownTracing func(context.Context)

// initMetrics 启动 Prometheus metrics HTTP 端点
func initMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("[metrics] Prometheus 端点启动: http://%s/metrics", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[metrics] 服务器错误: %v", err)
		}
	}()
}

func main() {
	log.Println("=== MQX/Rocketx E2E 测试启动 ===")

	// 0. 读取 mq.yaml，根据 trace.enabled / metrics.enabled 决定是否启动可观测性。
	//    YAML 解析失败时降级为"全部关闭"模式，确保主流程不被打断。
	cfg, _, parseErr := rocketx.ParseFile("mq.yaml")
	if parseErr != nil {
		log.Printf("[main] failed to parse mq.yaml: %v (continuing with observability disabled)", parseErr)
		cfg = nil
	}

	// 0.1 trace：统一通过 observability.InitTracing 初始化。
	if cfg != nil && cfg.Trace.Enabled {
		serviceName := cfg.Trace.ServiceName
		if serviceName == "" {
			serviceName = "rocketx-test"
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

	// 0.2 metrics
	if cfg != nil && cfg.Metrics.Enabled {
		initMetrics("localhost:9091")
	} else {
		log.Println("[main] metrics disabled in config, Prometheus server not started")
	}

	go runConsumerTest()
	time.Sleep(1 * time.Second)
	runProducerTest()

	log.Println("\n=== MQX 全局健康检查 ===")
	healthMap := rocketx.HealthCheck()
	for k, v := range healthMap {
		log.Printf("  -> %s : %s", k, v)
	}

	log.Println("\n=== [主程序挂起] 正在消费，请等待 Consumer 处理完消息 ===")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-time.After(20 * time.Second):
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rocketx.Shutdown(ctx)

	// 关闭 TracerProvider
	if shutdownTracing != nil {
		shutdownTracing(ctx)
	}

	log.Println("=== MQX/Rocketx E2E 测试圆满结束 ===")
}
