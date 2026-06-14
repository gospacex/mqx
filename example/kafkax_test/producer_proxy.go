package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ProxyProducer 通过 Kafka Proxy 发送消息，自动完成 trace 注入
// 原始 producer/consumer 零改动，仅替换连接地址和消息发送路径
type ProxyProducer struct {
	proxyAddr string
	producer  *kafka.Producer
	topic     string
}

func NewProxyProducer(proxyAddr, topic string) (*ProxyProducer, error) {
	log.Printf("[ProxyProducer] 连接到 Proxy: %s", proxyAddr)
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":  proxyAddr,
		"acks":              "all",
		"enable.idempotence": true,
	}

	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create proxy producer: %w", err)
	}

	log.Printf("[ProxyProducer] ProxyProducer 创建成功, topic=%s", topic)

	return &ProxyProducer{
		proxyAddr: proxyAddr,
		producer:  p,
		topic:     topic,
	}, nil
}

// SendWithTrace 通过 proxy 发送消息（trace 由 TCP Proxy 注入）
func (p *ProxyProducer) SendWithTrace(ctx context.Context, key, value []byte) error {
	log.Printf("[ProxyProducer] 发送消息: topic=%s, key=%s", p.topic, string(key))
	headers := []kafka.Header{
		{Key: "source", Value: []byte("proxy-producer")},
	}

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:     key,
		Value:   value,
		Headers: headers,
	}, nil)

	if err != nil {
		log.Printf("[ProxyProducer] 发送失败: %v", err)
		return fmt.Errorf("produce via proxy: %w", err)
	}

	p.producer.Flush(5000)
	log.Printf("[ProxyProducer] 发送成功: key=%s", string(key))
	return nil
}

func (p *ProxyProducer) Close() {
	p.producer.Flush(5000)
	p.producer.Close()
}

// RunProxyProducerTest 演示通过 Proxy 发送消息（非侵入 trace）
func RunProxyProducerTest() {
	log.Println("=== [Proxy Producer] 通过 Kafka Proxy 发送消息（自动 trace 注入）===")

	// 连接到本地 Proxy（而非直连 Kafka）
	proxyAddr := "127.0.0.1:9094"
	producer, err := NewProxyProducer(proxyAddr, "example-events")
	if err != nil {
		log.Printf("[Proxy Producer] 创建失败: %v（Proxy 未启动时正常）", err)
		return
	}
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("proxy-key-%d", i))
		value := []byte(fmt.Sprintf(`{"msg": "from proxy %d", "ts": %d}`, i, time.Now().Unix()))

		if err := producer.SendWithTrace(ctx, key, value); err != nil {
			log.Printf("[Proxy Producer] 发送失败: %v", err)
		} else {
			log.Printf("[Proxy Producer] 发送成功: key=%s", string(key))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
