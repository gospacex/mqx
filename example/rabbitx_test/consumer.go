package main

import (
	"context"
	"log"
	"time"

	"github.com/gospacex/mqx/observability"
	"github.com/gospacex/mqx/rabbitx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func runConsumerTest() {
	log.Println("\n=== Rabbitx Consumer E2E 测试开始 ===")

	log.Println("[Consumer 1] 获取单机消费者并启动拉取...")
	conn, err := rabbitx.C("mq.yaml#rabbit_single")

	if err == nil {
		go func() {
			ch, chErr := conn.Channel()
			if chErr != nil {
				log.Printf("[Consumer 1] 打开 Channel 失败: %v", chErr)
				return
			}
			defer ch.Close()

			msgs, consumeErr := ch.Consume(
				"example.queue", // 由 mqx 自动声明好的 queue
				"test-consumer", // consumer name
				false,           // auto-ack
				false,
				false,
				false,
				nil,
			)
			if consumeErr != nil {
				log.Printf("[Consumer 1] 开始拉取失败: %v", consumeErr)
				return
			}

			log.Println("[Consumer 1] 开始监听消息通道 (最多阻塞 3 秒)...")
			timeout := time.After(3 * time.Second)

			tracer := otel.Tracer("rabbitx-consumer")

			for {
				select {
				case d, ok := <-msgs:
					if !ok {
						log.Println("[Consumer 1] 消息通道已关闭")
						return
					}

					// 提取 trace context 从 RabbitMQ headers
					ctx := observability.ExtractRabbitTrace(context.Background(), d.Headers)

					// 继续 trace span
					_, span := tracer.Start(ctx, "consume-message",
						trace.WithAttributes(
							attribute.String("queue", "example.queue"),
							attribute.Int64("delivery_tag", int64(d.DeliveryTag)),
						))
					defer span.End()

					log.Printf("[Consumer 1] 收到消息: %s (trace_id: %s)", string(d.Body), span.SpanContext().TraceID())
					// 手动确认
					_ = d.Ack(false)
				case <-timeout:
					log.Println("[Consumer 1] 模拟消费超时退出")
					return
				}
			}
		}()
	} else {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	}

	log.Println("\n[Consumer 2] 模拟获取集群版 Consumer (CPC)...")
	_, err = rabbitx.CPC("mq.yaml#rabbit_cluster")
	if err == nil {
		log.Printf("[Consumer 2] 集群版消费者就绪")
	} else {
		log.Printf("[Consumer 2] 跳过集群模拟: (%v)", err)
	}
}
