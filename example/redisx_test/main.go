package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/redisx"
)

// main 作为程序的入口，先严格按 mq.yaml#redis_single 解析配置，再按配置
// 决定是否启动 OTel 调用链与 Prometheus 指标，最后驱动生产和消费，触发优雅关闭。
func main() {
	log.Println("=== MQX/Redisx E2E 测试启动 ===")

	// 0. 读取 mq.yaml，根据 trace.enabled 决定是否启动 OTel 链路追踪。
	//    注意：必须显式使用 "mq.yaml#redis_single" 指定 config key —— mq.yaml 中同时存在
	//    redis_single 和 redis_cluster 两个 key，不带 # 后缀时 ParseFile 会按非确定性
	//    map 顺序返回第一个，可能拿到没有 trace 段的 redis_cluster，导致
	//    trace 看上去"没上链"。
	cfg, configKey, parseErr := redisx.ParseFile("mq.yaml#redis_single")
	if parseErr != nil {
		log.Printf("[main] failed to parse mq.yaml: %v (continuing with observability disabled)", parseErr)
		cfg = nil
	}
	if cfg != nil {
		log.Printf("[main] config loaded key=%s driver=%s mode=%s trace.enabled=%v metrics.enabled=%v",
			configKey, cfg.Driver, cfg.Mode, cfg.Trace.Enabled, cfg.Metrics.Enabled)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. 按 cfg.Trace.* 启动 OTel 调用链；只有 trace.enabled=true 才真正接 OTLP。
	if cfg != nil && cfg.Trace.Enabled {
		// 端点从配置里读；与 protocol 解耦——observability 当前统一走 gRPC。
		endpoint := cfg.Trace.Endpoint
		if endpoint == "" {
			endpoint = "localhost:4317"
		}
		serviceName := cfg.Trace.ServiceName
		if serviceName == "" {
			serviceName = "redisx-test"
		}
		shutdown, err := observability.InitTracing(ctx, &observability.Config{
			Enabled:        true,
			ServiceName:    serviceName,
			JaegerEndpoint: endpoint,
		})
		if err != nil {
			log.Printf("[tracing] init warning: %v", err)
		} else {
			log.Printf("[tracing] initialized (service=%s, endpoint=%s)", serviceName, endpoint)
			defer shutdown(ctx)
		}
	} else {
		log.Println("[main] trace disabled in config, OTel pipeline not started")
	}

	// 2. 按 cfg.Metrics.Enabled 启动 Prometheus 指标 HTTP 服务
	metricsPort := 9091
	if cfg != nil && cfg.Metrics.Enabled {
		_ = observability.StartMetricsServer(&observability.Config{
			Enabled:     true,
			MetricsPort: metricsPort,
		})
		log.Printf("[metrics] server started on :%d (enabled by config)", metricsPort)
	} else {
		log.Println("[main] metrics disabled in config, /metrics server not started")
	}

	// 3. 异步启动消费者，开始持续监听 stream 并接收消息
	go runConsumerTest(cfg)

	// 给消费者一点时间去初始化连接并执行 XGROUP CREATE
	time.Sleep(1 * time.Second)

	// 4. 启动生产者，发送 stream 消息
	runProducerTest(cfg)

	// 5. 打印全局健康检查
	log.Println("\n=== MQX 全局健康检查 ===")
	healthMap := redisx.HealthCheck()
	for k, v := range healthMap {
		log.Printf("  -> %s : %s", k, v)
	}

	// 6. 捕获中断信号，等待观察消费结果，并触发优雅关闭
	log.Println("\n=== [主程序挂起] 正在消费，请等待 3 秒或按 Ctrl+C 退出 ===")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("收到中断信号，开始关闭...")
	case <-time.After(3 * time.Second):
		log.Println("等待时间到，自动触发关闭...")
	}

	// 创建 10s 超时的 Context
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// 框架优雅关闭：会关闭底层的 redis.Client 和 redis.ClusterClient
	redisx.Shutdown(shutdownCtx)
	_ = observability.ShutdownMetricsServer(shutdownCtx)
	log.Println("=== MQX/Redisx E2E 测试圆满结束 ===")
}
