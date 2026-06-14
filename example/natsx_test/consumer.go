package main

import (
	"context"
	"log"

	"github.com/gospacex/mqx/natsx"
	"github.com/nats-io/nats.go"
)

// traceEnabledSingle / traceEnabledCluster 通过读取各自 config key 的 trace.enabled
// 决定是否走 *Trace 函数;false 时走原生 nc.QueueSubscribe / js.QueueSubscribe。
var (
	traceEnabledSingle  = loadTraceFlag("mq.yaml#nats_single")
	traceEnabledCluster = loadTraceFlag("mq.yaml#nats_js_cluster")
)

func loadTraceFlag(path string) bool {
	cfg, _, err := natsx.ParseFile(path)
	if err != nil || cfg == nil {
		return false
	}
	return cfg.Trace.Enabled
}

func runConsumerTest() {
	log.Println("\n=== Natsx Consumer E2E 测试开始 ===")

	// Consumer 1: 单机 Core NATS 订阅 (按 traceEnabled 分支)
	log.Println("[Consumer 1] 获取单机 Core 消费者并挂载订阅...")
	c, err := natsx.C("mq.yaml#nats_single")
	if err == nil {
		nc := c.(*nats.Conn)
		ctx := context.Background()
		if traceEnabledSingle {
			// *Trace 内部已 extract trace context,handler 收到带 span 的 ctx。
			_, err = natsx.QueueSubscribeTrace(ctx, nc, "example.subject", "example-queue", func(ctx context.Context, msg *nats.Msg) {
				log.Printf("[Consumer 1] 收到消息: %s", string(msg.Data))
			})
		} else {
			// 原生路径:handler 不带 ctx;若业务侧仍要 extract,可在 handler 内手动调用
			// observability.ExtractNatsTrace 后再决定是否开 span。
			_, err = nc.QueueSubscribe("example.subject", "example-queue", func(msg *nats.Msg) {
				log.Printf("[Consumer 1] 收到消息: %s", string(msg.Data))
			})
		}
		if err != nil {
			log.Printf("[Consumer 1] 订阅失败: %v", err)
		} else {
			log.Printf("[Consumer 1] 订阅成功，开始监听... (trace=%v)", traceEnabledSingle)
		}
	} else {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	}

	// Consumer 2: 集群版 JetStream 订阅 (按 traceEnabled 分支)
	log.Println("\n[Consumer 2] 获取集群版 JetStream 消费者并挂载订阅...")
	c2, err := natsx.C("mq.yaml#nats_js_cluster")
	if err == nil {
		js := c2.(nats.JetStreamContext)
		ctx := context.Background()
		if traceEnabledCluster {
			// *Trace 内部已 extract trace context,handler 收到带 span 的 ctx。
			_, err = natsx.JSQueueSubscribeTrace(ctx, js, "orders.>", "orders-queue", func(ctx context.Context, msg *nats.Msg) {
				log.Printf("[Consumer 2] 收到消息: subject=%s data=%s", msg.Subject, string(msg.Data))
			})
		} else {
			// 原生路径:handler 不带 ctx。
			_, err = js.QueueSubscribe("orders.>", "orders-queue", func(msg *nats.Msg) {
				log.Printf("[Consumer 2] 收到消息: subject=%s data=%s", msg.Subject, string(msg.Data))
			})
		}
		if err != nil {
			log.Printf("[Consumer 2] 订阅失败: %v", err)
		} else {
			log.Printf("[Consumer 2] 订阅成功，开始监听... (trace=%v)", traceEnabledCluster)
		}
	} else {
		log.Printf("[Consumer 2] 跳过集群消费: 环境未就绪 (%v)", err)
	}
}