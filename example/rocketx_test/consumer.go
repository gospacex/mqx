package main

import (
	"context"
	"log"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/gospacex/mqx/rocketx"
)

func runConsumerTest() {
	log.Println("\n=== Rocketx Consumer E2E 测试开始 ===")

	// 顶部读取 cfg.Trace.Enabled，作为本次 trace 开关。
	cfg, _, parseErr := rocketx.ParseFile("mq.yaml")
	traceEnabled := false
	if parseErr == nil && cfg != nil {
		traceEnabled = cfg.Trace.Enabled
	}

	log.Println("[Consumer 1] 获取单机消费者并挂载处理函数 (延后 Start)...")
	c, err := rocketx.C("mq.yaml#rocketmq_single")

	if err == nil {
		// RocketMQ 要求必须先 Subscribe 再 Start
		err = c.Subscribe("example-topic", consumer.MessageSelector{}, func(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, msg := range ext {
				handleErr := rocketx.ConsumeTrace(ctx, msg, traceEnabled, func(ctx context.Context) error {
					log.Printf("[Consumer 1] 收到消息: %s", string(msg.Body))
					return nil
				})
				if handleErr != nil {
					log.Printf("[Consumer 1] 处理消息失败: %v", handleErr)
				}
			}
			return consumer.ConsumeSuccess, nil
		})

		if err != nil {
			log.Printf("[Consumer 1] 订阅失败: %v", err)
			return
		}

		err = c.Start()
		if err != nil {
			log.Printf("[Consumer 1] 启动消费者失败: %v", err)
		} else {
			log.Printf("[Consumer 1] 消费者启动成功，开始监听...")
		}
	} else {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	}

	log.Println("\n[Consumer 2] 模拟获取集群版 Consumer (CPC)...")
	_, err = rocketx.CPC("mq.yaml#rocketmq_cluster")
	if err == nil {
		log.Printf("[Consumer 2] 集群版消费者就绪")
	} else {
		log.Printf("[Consumer 2] 跳过集群模拟: (%v)", err)
	}
}