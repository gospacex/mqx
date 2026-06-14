package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx/kafkax"
)

func runProducerTest() {
	log.Println("=== Kafkax Producer E2E 测试开始 ===")

	// 读取配置，trace.enabled 决定是否启用 trace 注入
	cfg, _, err := kafkax.ParseFile("mq.yaml")
	traceEnabled := cfg != nil && cfg.Trace.Enabled

	// ---------------------------------------------------------
	// 场景 1: 并发防抖测试 (验证单例池)
	// ---------------------------------------------------------
	log.Println("[Producer 1] 并发触发 kafkax.P(kafka_single) x 100...")
	var wg sync.WaitGroup
	var kafkaConns sync.Map

	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			kp, err := kafkax.P("mq.yaml#kafka_single")
			if err != nil {
				kafkaConns.Store(fmt.Sprintf("err_%d", idx), err)
				return
			}
			kafkaConns.Store(fmt.Sprintf("conn_%d", idx), kp)
		}(i)
	}
	wg.Wait()
	log.Printf("[Producer 1] 并发初始化完毕。耗时: %v", time.Since(start))

	var firstPtr string
	isSingleton := true
	successCount := 0
	kafkaConns.Range(func(key, value any) bool {
		if v, ok := value.(*kafka.Producer); ok {
			successCount++
			ptrStr := fmt.Sprintf("%p", v)
			if firstPtr == "" {
				firstPtr = ptrStr
			} else if firstPtr != ptrStr {
				isSingleton = false
			}
		}
		return true
	})
	log.Printf("[Producer 1] 成功获取句柄数: %d/100", successCount)
	if successCount > 0 {
		log.Printf("[Producer 1] 严格单例模式: %v (指针地址: %s)", isSingleton, firstPtr)
	}

	// ---------------------------------------------------------
	// 场景 2: 发送原生消息并注入 Trace
	// ---------------------------------------------------------
	log.Println("\n[Producer 2] 获取单机生产者并发送原生消息...")
	producer, err := kafkax.P("mq.yaml#kafka_single")
	if err == nil {
		topic := "example-events"
		headers := []kafka.Header{{Key: "source", Value: []byte("kafkax-test")}}

		err = kafkax.ProduceTrace(context.Background(), producer, &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("key-001"),
			Value:          []byte(`{"msg": "hello mqx"}`),
			Headers:        headers,
		}, traceEnabled)

		if err != nil {
			log.Printf("[Producer 2] 发送消息失败: %v", err)
		} else {
			log.Printf("[Producer 2] 消息已投递到缓冲区，正在 Flush 到 Kafka...")
			// Produce 是异步调用，必须 Flush 确保消息真正到达 Kafka
			// 否则消费者在读取窗口内可能收不到消息
			left := producer.Flush(5000)
			if left > 0 {
				log.Printf("[Producer 2] Flush 完成，仍有 %d 条消息未送达", left)
			}
			log.Printf("[Producer 2] 消息已 Flush 到 Kafka (trace=%v)", traceEnabled)
		}
	} else {
		log.Printf("[Producer 2] 跳过发送: 环境未就绪 (%v)", err)
	}

	// ---------------------------------------------------------
	// 场景 3: 模拟集群对象配置直传 (POC)
	// ---------------------------------------------------------
	log.Println("\n[Producer 3] 模拟获取集群版 Producer (PPC)...")
	_, err = kafkax.PPC("mq.yaml#kafka_cluster")
	if err == nil {
		log.Printf("[Producer 3] 集群版生产者就绪")
	} else {
		log.Printf("[Producer 3] 跳过集群模拟: (%v)", err)
	}
}
