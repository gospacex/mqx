package main

import (
	"context"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx/pulsarx"
)

func runConsumerTest() {
	log.Println("\n=== Pulsarx Consumer E2E 测试开始 ===")

	// 读取 mq.yaml#pulsar_single，根据 trace.enabled 决定收发/Ack 路径。
	cfg, _, parseErr := pulsarx.ParseFile("mq.yaml#pulsar_single")
	if parseErr != nil {
		log.Printf("[runConsumerTest] failed to parse mq.yaml: %v (continuing with tracing disabled)", parseErr)
		cfg = nil
	}
	traceEnabled := cfg != nil && cfg.Trace.Enabled
	log.Printf("[runConsumerTest] trace.enabled=%v", traceEnabled)

	log.Println("[Consumer 1] 获取单机消费者并阻塞拉取...")
	c, err := pulsarx.C("mq.yaml#pulsar_single")

	if err == nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			for {
				var (
					msg pulsar.Message
					rerr error
				)
				if traceEnabled {
					msg, rerr = pulsarx.ReceiveTrace(ctx, c)
				} else {
					msg, rerr = c.Receive(ctx)
				}
				if rerr != nil {
					log.Printf("[Consumer 1] 拉取结束/异常: %v", rerr)
					return
				}
				log.Printf("[Consumer 1] 收到消息: %s", string(msg.Payload()))
				if traceEnabled {
					_ = pulsarx.AckTrace(ctx, c, msg)
				} else {
					c.Ack(msg)
				}
			}
		}()
	} else {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	}
}
