package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/pulsarx"
	"go.opentelemetry.io/otel"
)

var shutdownTracing func(context.Context)

func main() {
	log.Println("=== MQX/Pulsarx E2E 测试启动 ===")

	// 0. 读取 mq.yaml，根据 trace.enabled 决定是否启动 OTel 链路追踪。
	//    注意：必须显式使用 "mq.yaml#pulsar_single" 指定 config key —— mq.yaml 中同时存在
	//    pulsar_single 和 pulsar_cluster 两个 key，不带 # 后缀时 ParseFile 会按非确定性
	//    map 顺序返回第一个，可能拿到没有 trace 段的 pulsar_cluster，导致
	//    trace 看上去"没上链"。
	cfg, configKey, parseErr := pulsarx.ParseFile("mq.yaml#pulsar_single")
	if parseErr != nil {
		log.Printf("[main] failed to parse mq.yaml: %v (continuing with observability disabled)", parseErr)
		cfg = nil
	}
	if cfg != nil {
		log.Printf("[main] config loaded key=%s driver=%s mode=%s trace.enabled=%v metrics.enabled=%v",
			configKey, cfg.Driver, cfg.Mode, cfg.Trace.Enabled, cfg.Metrics.Enabled)
	}

	// 0.1 trace：统一通过 observability.InitTracing 初始化。
	if cfg != nil && cfg.Trace.Enabled {
		serviceName := cfg.Trace.ServiceName
		if serviceName == "" {
			serviceName = "pulsarx-test"
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

		// 启动探针 span：跟 Pulsar 业务解耦的端到端连通性验证。
		// 看到 Jaeger 里有 otel.startup =》 OTel → Jaeger 链路通；业务 span 没出来
		// =》 是 Pulsar broker 问题。看不到 otel.startup =》 OTel 管道本身没通。
		ctx := context.Background()
		_, probe := otel.Tracer("boot").Start(ctx, "otel.startup")
		probe.End()
		log.Println("[otel] startup probe fired — check Jaeger for 'otel.startup' span")
	} else {
		log.Println("[main] trace disabled in config, OTel pipeline not started")
	}

	// 0.2 metrics
	if cfg != nil && cfg.Metrics.Enabled {
		log.Println("[main] metrics enabled in config, Prometheus endpoint would start here")
	} else {
		log.Println("[main] metrics disabled in config, Prometheus server not started")
	}

	go runConsumerTest()
	time.Sleep(1 * time.Second)
	runProducerTest()

	log.Println("\n=== [主程序挂起] 正在消费，请等待 3 秒或按 Ctrl+C 退出 ===")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-time.After(3 * time.Second):
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pulsarx.Shutdown(ctx)
	if shutdownTracing != nil {
		shutdownTracing(ctx)
	}
	log.Println("=== MQX/Pulsarx E2E 测试圆满结束 ===")
}
