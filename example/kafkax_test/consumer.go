package main

import (
	"context"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx/kafkax"
)

func runConsumerTest() {
	log.Println("\n=== Kafkax Consumer E2E 测试开始 ===")

	// 读取配置，trace.enabled 决定是否启用 trace 提取
	cfg, _, err := kafkax.ParseFile("mq.yaml")
	traceEnabled := cfg != nil && cfg.Trace.Enabled

	log.Println("[Consumer 1] 获取单机消费者并启动拉取...")
	consumer, err := kafkax.C("mq.yaml#kafka_single")

	if err != nil {
		log.Printf("[Consumer 1] 跳过消费: 环境未就绪 (%v)", err)
	} else {
		// 起个短暂的协程模拟消费
		go func() {
			log.Println("[Consumer 1] 消费者协程启动，等待消息 (每次超时1秒，最多10次)...")

			// 读 10 次（10 秒），覆盖 Kafka 首次 rebalance 的 3 秒延迟
			for i := 0; i < 10; i++ {
				msg, err := kafkax.ConsumeTrace(context.Background(), consumer, time.Second*1, traceEnabled)
				if err != nil {
					if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
						log.Printf("[Consumer 1] 第 %d 次读取超时，继续等待...", i+1)
						continue
					}
					log.Printf("[Consumer 1] 消费者读取异常: %v", err)
					return
				}

				log.Printf("[Consumer 1] >>> 收到消息! key=%s value=%s", string(msg.Key), string(msg.Value))

				// 记录所有 headers 用于诊断
				log.Printf("[Consumer 1] >>> Headers 数量: %d", len(msg.Headers))
				for _, h := range msg.Headers {
					log.Printf("[Consumer 1] >>> Header: %s = %s", h.Key, string(h.Value))
				}

				// 我们在 config.go 里强制将 auto_commit 设成了 false
				// 业务端需要手动确认位移，这也是生产环境标配
				if _, commitErr := kafkax.CommitMessageTrace(context.Background(), consumer, msg, traceEnabled); commitErr != nil {
					log.Printf("[Consumer 1] Commit 失败: %v", commitErr)
				}
			}
			log.Println("[Consumer 1] 模拟消费循环结束。")
		}()
	}

	log.Println("\n[Consumer 2] 模拟获取集群版 Consumer (CPC)...")
	_, err = kafkax.CPC("mq.yaml#kafka_cluster")
	if err == nil {
		log.Printf("[Consumer 2] 集群版消费者就绪")
	} else {
		log.Printf("[Consumer 2] 跳过集群模拟: (%v)", err)
	}
}
