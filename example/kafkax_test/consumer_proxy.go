package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx/kafkax"
)

// ProxyConsumer 通过 Kafka Proxy 接收消息，自动完成 trace 提取
// 原始 producer/consumer 零改动，仅替换连接地址和消息拉取路径
type ProxyConsumer struct {
	proxyAddr    string
	consumer     *kafka.Consumer
	topic        string
	groupID      string
	traceEnabled bool
}

func NewProxyConsumer(proxyAddr, topic, groupID string) (*ProxyConsumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":   proxyAddr,
		"group.id":            groupID,
		"enable.auto.commit":  false,
		"auto.offset.reset":   "earliest",
		"api.version.request": true,
		"socket.timeout.ms":   10000,
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create proxy consumer: %w", err)
	}

	if err := c.Subscribe(topic, nil); err != nil {
		c.Close()
		return nil, fmt.Errorf("subscribe topic: %w", err)
	}

	// 读取 mq.yaml 中的 trace.enabled 决定是否走 trace 链路
	mqCfg, _, _ := kafkax.ParseFile("mq.yaml")
	traceEnabled := mqCfg != nil && mqCfg.Trace.Enabled

	log.Printf("[ProxyConsumer] 已创建并订阅 topic=%s, group=%s, trace=%v", topic, groupID, traceEnabled)

	return &ProxyConsumer{
		proxyAddr:    proxyAddr,
		consumer:     c,
		topic:        topic,
		groupID:      groupID,
		traceEnabled: traceEnabled,
	}, nil
}

// ReadWithTrace 通过 proxy 拉取消息，走 kafkax.ConsumeTrace 自动处理 trace context
func (p *ProxyConsumer) ReadWithTrace(ctx context.Context, timeout time.Duration) (*kafka.Message, error) {
	msg, err := kafkax.ConsumeTrace(ctx, p.consumer, timeout, p.traceEnabled)
	if err != nil {
		return nil, fmt.Errorf("read via proxy: %w", err)
	}

	log.Printf("[ProxyConsumer] 收到消息: topic=%s, partition=%d, key=%s",
		p.topic, msg.TopicPartition.Partition, string(msg.Key))

	return msg, nil
}

// CommitMessage 手动提交位移
func (p *ProxyConsumer) CommitMessage(ctx context.Context, msg *kafka.Message) ([]kafka.TopicPartition, error) {
	return kafkax.CommitMessageTrace(ctx, p.consumer, msg, p.traceEnabled)
}

func (p *ProxyConsumer) Close() {
	p.consumer.Close()
}

// RunProxyConsumerTest 演示通过 Proxy 接收消息（自动 trace 提取）
func RunProxyConsumerTest() {
	log.Println("=== [Proxy Consumer] 通过 Kafka Proxy 接收消息（自动 trace 提取）===")

	proxyAddr := "127.0.0.1:9094"
	consumer, err := NewProxyConsumer(proxyAddr, "example-events", "proxy-consumer-group")
	if err != nil {
		log.Printf("[Proxy Consumer] 创建失败: %v（Proxy 未启动时正常）", err)
		return
	}
	defer consumer.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		msg, err := consumer.ReadWithTrace(ctx, 2*time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			log.Printf("[Proxy Consumer] 读取失败: %v", err)
			continue
		}

		log.Printf("[Proxy Consumer] 收到消息: key=%s value=%s", string(msg.Key), string(msg.Value))

		if _, err := consumer.CommitMessage(ctx, msg); err != nil {
			log.Printf("[Proxy Consumer] Commit 失败: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
