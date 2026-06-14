package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/mqx/mqttx"
	"github.com/gospacex/mqx/observability"
)

// main 作为程序的入口，先严格按 mq.yaml#mqtt_single 解析配置，再按配置
// 决定是否启动 OTel 调用链与 Prometheus 指标，最后驱动生产和消费，触发优雅关闭。
func main() {
	log.Println("=== MQX/Mqttx E2E 测试启动 ===")

	// 0. 读取 mq.yaml，根据 trace.enabled 决定是否启动 OTel 链路追踪。
	//    注意：必须显式使用 "mq.yaml#mqtt_single" 指定 config key —— mq.yaml 中同时存在
	//    mqtt_single 和 mqtt_cluster 两个 key，不带 # 后缀时 ParseFile 会按非确定性
	//    map 顺序返回第一个，可能拿到没有 trace 段的 mqtt_cluster，导致
	//    trace 看上去"没上链"。
	cfg, configKey, parseErr := mqttx.ParseFile("mq.yaml#mqtt_single")
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

	// 0.1 trace: 只有 cfg.Trace.Enabled == true 时才走 observability.InitTracing
	//     (其内部在 Enabled=false 时直接返回 no-op cleanup，不会创建 OTLP exporter
	//     / TracerProvider，也不会动全局 TracerProvider)
	if cfg != nil && cfg.Trace.Enabled {
		endpoint := cfg.Trace.Endpoint
		if endpoint == "" {
			endpoint = "localhost:4317"
		}
		serviceName := cfg.Trace.ServiceName
		if serviceName == "" {
			serviceName = "mqttx-test"
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

	// 0.2 metrics: 严格按 cfg.Metrics.Enabled 控制
	if cfg != nil && cfg.Metrics.Enabled {
		_ = observability.StartMetricsServer(&observability.Config{
			Enabled:     true,
			MetricsPort: 9091,
		})
		log.Println("[main] metrics enabled in config, Prometheus server started on :9091")
	} else {
		log.Println("[main] metrics disabled in config, Prometheus server not started")
	}

	// 1. 异步启动消费者
	go runConsumerTest(cfg)

	// 给消费者一点时间去连接 broker 并完成订阅
	time.Sleep(1 * time.Second)

	// 2. 启动生产者
	runProducerTest(cfg)

	// 3. 打印全局健康检查
	log.Println("\n=== MQX 全局健康检查 ===")
	healthMap := mqttx.HealthCheck()
	for k, v := range healthMap {
		log.Printf("  -> %s : %s", k, v)
	}

	// 4. 捕获中断信号，等待观察消费结果，并触发优雅关闭
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

	// 框架优雅关闭
	mqttx.Shutdown(shutdownCtx)
	_ = observability.ShutdownMetricsServer(shutdownCtx)
	log.Println("=== MQX/Mqttx E2E 测试圆满结束 ===")
}
