package kafkax

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ProduceTrace 发送消息，按 enabled 决定是否走 trace 链路。
// enabled=false 时直接调底层 API，等价于原始 Produce 调用。
// enabled=true 时启动 span、把当前 trace context 注入到 msg.Headers，
// 并打上 OTel messaging.* 语义属性。
func ProduceTrace(ctx context.Context, p *kafka.Producer, msg *kafka.Message, enabled bool) error {
	if !enabled {
		return p.Produce(msg, nil)
	}

	ctx, span := observability.StartSpan(ctx, "kafka.produce")
	defer span.End()

	// 注入 trace context 到消息 headers
	observability.InjectTrace(ctx, &msg.Headers)

	if msg.TopicPartition.Topic != nil {
		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", *msg.TopicPartition.Topic),
			attribute.String("messaging.operation", "publish"),
		)
	}

	if err := p.Produce(msg, nil); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// ConsumeTrace 拉取消息，按 enabled 决定是否走 trace 链路。
// enabled=true 时从消息 headers 提取上游 trace context 并创建子 span。
// 返回的 *kafka.Message 永远为原始消息（不被消费掉）。
func ConsumeTrace(ctx context.Context, c *kafka.Consumer, timeout time.Duration, enabled bool) (*kafka.Message, error) {
	msg, err := c.ReadMessage(timeout)
	if err != nil {
		return msg, err
	}

	if !enabled {
		return msg, nil
	}

	// 从消息 headers 提取上游 trace context
	parentCtx := observability.ExtractTrace(ctx, msg.Headers)

	_, span := observability.StartSpan(parentCtx, "kafka.consume")
	defer span.End()

	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.operation", "consume"),
	}
	if msg.TopicPartition.Topic != nil {
		attrs = append(attrs, attribute.String("messaging.destination", *msg.TopicPartition.Topic))
	}
	attrs = append(attrs,
		attribute.Int("messaging.kafka.partition", int(msg.TopicPartition.Partition)),
		attribute.String("messaging.message_id",
			fmt.Sprintf("%s-%d-%d", *msg.TopicPartition.Topic, msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset))),
	)
	span.SetAttributes(attrs...)

	return msg, nil
}

// CommitMessageTrace 提交位移，按 enabled 决定是否创建 span。
// enabled=false 时直接调底层 CommitMessage。
// enabled=true 时启动 span 并在提交出错时记录 error。
func CommitMessageTrace(ctx context.Context, c *kafka.Consumer, msg *kafka.Message, enabled bool) ([]kafka.TopicPartition, error) {
	if !enabled {
		return c.CommitMessage(msg)
	}

	_, span := observability.StartSpan(ctx, "kafka.commit",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation", "commit"),
		),
	)
	defer span.End()

	parts, err := c.CommitMessage(msg)
	if err != nil {
		span.RecordError(err)
	}
	return parts, err
}
