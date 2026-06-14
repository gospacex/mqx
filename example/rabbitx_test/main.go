package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/rabbitx"
)

// main 作为程序的入口，直接驱动生产和消费，最后执行优雅关闭。
func main() {
	log.Println("=== MQX/Rabbitx E2E 测试启动 ===")

	// 0. 读取 mq.yaml，根据 trace.enabled 决定是否启动 OTel 链路追踪。
	//    注意：必须显式使用 "mq.yaml#rabbit_single" 指定 config key —— mq.yaml 中同时存在
	//    rabbit_single 和 rabbit_cluster 两个 key，不带 # 后缀时 ParseFile 会按非确定性
	//    map 顺序返回第一个，可能拿到没有 service_name / endpoint 的 rabbit_cluster，导致
	//    trace 看上去"没上链"。
	cfg, _, parseErr := rabbitx.ParseFile("mq.yaml#rabbit_single")
	if parseErr != nil {
		log.Printf("[main] failed to parse mq.yaml: %v (continuing with observability disabled)", parseErr)
		cfg = nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg != nil && cfg.Trace.Enabled {
		endpoint := cfg.Trace.Endpoint
		if endpoint == "" {
			endpoint = "localhost:4317"
		}
		shutdown, err := observability.InitTracing(ctx, &observability.Config{
			Enabled:        true,
			ServiceName:    cfg.Trace.ServiceName,
			JaegerEndpoint: endpoint,
		})
		if err != nil {
			log.Printf("[tracing] init warning: %v", err)
		} else {
			log.Printf("[tracing] initialized (service=%s, endpoint=%s)", cfg.Trace.ServiceName, endpoint)
			defer shutdown(ctx)
		}
	} else {
		log.Println("[main] trace disabled in config, OTel pipeline not started")
	}

	// 1. 异步启动消费者，开始持续监听 queue 并接收消息
	go runConsumerTest()

	// 给消费者一点时间去初始化打开 Channel 并阻塞在 Consume
	time.Sleep(1 * time.Second)

	// 2. 启动生产者，打开 Channel 发送消息
	runProducerTest()

	// 3. 打印全局健康检查
	log.Println("\n=== MQX 全局健康检查 ===")
	healthMap := rabbitx.HealthCheck()
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

	// 框架优雅关闭：会关闭底层的 AMQP Connection
	rabbitx.Shutdown(shutdownCtx)
	log.Println("=== MQX/Rabbitx E2E 测试圆满结束 ===")
}
