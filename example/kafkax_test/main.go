package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/mqx/kafkax"
	"github.com/gospacex/mqx/observability"
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

// main 作为程序的入口，直接驱动生产和消费，最后执行优雅关闭。
func main() {
	log.Println("=== MQX/Kafkax E2E 测试启动 ===")

	// 0. 读取 mq.yaml，根据 trace.enabled / metrics.enabled 决定是否启动可观测性。
	//    YAML 解析失败时降级为"全部关闭"模式，确保主流程不被打断。
	cfg, _, err := kafkax.ParseFile("mq.yaml")
	if err != nil {
		log.Printf("[main] failed to parse mq.yaml: %v (continuing with observability disabled)", err)
		cfg = nil
	}

	// 0.1 trace：统一通过 observability.InitTracing 初始化。
	if cfg != nil && cfg.Trace.Enabled {
		shutdownTracing, err = observability.InitTracing(context.Background(), &observability.Config{
			Enabled:        true,
			JaegerEndpoint: cfg.Trace.Endpoint,
			ServiceName:    cfg.Trace.ServiceName,
			Insecure:       true,
			Headers:        cfg.Trace.Headers,
		})
		if err != nil {
			log.Printf("[main] init tracing error: %v", err)
		}
	} else {
		log.Println("[main] trace disabled in config, OTel pipeline not started")
	}

	// 0.2 metrics
	if cfg != nil && cfg.Metrics.Enabled {
		initMetrics("localhost:9091")
	} else {
		log.Println("[main] metrics disabled in config, Prometheus server not started")
	}

	// 读取测试模式：ORIGINAL 或 PROXY
	testMode := os.Getenv("TEST_MODE")
	if testMode == "" {
		testMode = "ORIGINAL"
	}

	log.Printf("[模式选择] TEST_MODE=%s", testMode)

	if testMode == "PROXY" {
		// ========== Proxy 模式 ==========
		log.Println("\n========== Proxy 模式测试 ==========")

		// 启动 TCP Proxy 服务器
		proxy := NewKafkaProxy("127.0.0.1:9094", "127.0.0.1:9092")
		if err := proxy.Start(); err != nil {
			log.Printf("[Proxy] 启动失败: %v", err)
			return
		}
		log.Println("[Proxy] TCP Proxy 已启动 (127.0.0.1:9094 -> 127.0.0.1:9092)")

		// 等待 Proxy 启动完成
		time.Sleep(500 * time.Millisecond)

		// 启动 Proxy 消费者（异步，持续接收）
		go RunProxyConsumerTest()

		// 等待消费者订阅完成
		time.Sleep(1 * time.Second)

		// 启动 Proxy 生产者
		RunProxyProducerTest()

		// 等待消息被消费
		log.Println("[主程序] 等待消息消费完成...")
		time.Sleep(3 * time.Second)

		log.Println("\n=== Proxy 模式调用链: kafka.produce_proxy -> kafka.consume_proxy ===")

	} else {
		// ========== 原始模式 ==========
		log.Println("\n========== 原始模式测试 ==========")

		// 1. 异步启动消费者，开始持续监听并接收消息
		go runConsumerTest()

		// 给消费者一点时间去初始化并完成 SubscribeTopics (防止发太快还没订阅上)
		time.Sleep(1 * time.Second)

		// 2. 启动生产者，发送消息
		runProducerTest()

		log.Println("\n=== 原始模式调用链: kafka.produce -> kafka.consume ===")
	}

	// 3. 打印全局健康检查
	log.Println("\n=== MQX 全局健康检查 ===")
	healthMap := kafkax.HealthCheck()
	for k, v := range healthMap {
		log.Printf("  -> %s : %s", k, v)
	}

	// 4. 捕获中断信号，等待观察消费结果，并触发优雅关闭
	log.Println("\n=== [主程序挂起] 正在消费，请等待 5 秒或按 Ctrl+C 退出 ===")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("收到中断信号，开始关闭...")
	case <-time.After(5 * time.Second):
		log.Println("等待时间到，自动触发关闭...")
	}

	// 创建 10s 超时的 Context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 框架优雅关闭：会触发 Producer.Flush 和 Consumer.Close
	kafkax.Shutdown(ctx)

	// 关闭 TracerProvider
	if shutdownTracing != nil {
		shutdownTracing(ctx)
	}

	log.Println("=== MQX/Kafkax E2E 测试圆满结束 ===")
}
