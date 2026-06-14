package pulsarx

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gospacex/mqx/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// propMapCarrier 基于 map[string]string 实现 propagation.TextMapCarrier
type propMapCarrier struct {
	props map[string]string
}

func (c *propMapCarrier) Get(key string) string {
	if c.props == nil {
		return ""
	}
	return c.props[key]
}

func (c *propMapCarrier) Set(key, value string) {
	if c.props == nil {
		c.props = make(map[string]string)
	}
	c.props[key] = value
}

func (c *propMapCarrier) Keys() []string {
	if c.props == nil {
		return nil
	}
	keys := make([]string, 0, len(c.props))
	for k := range c.props {
		keys = append(keys, k)
	}
	return keys
}

// SendTrace 发送消息，带 tracing，并将 trace context 注入消息属性
func SendTrace(ctx context.Context, producer pulsar.Producer, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	ctx, span := observability.StartSpan(ctx, "pulsarx.Send")
	defer span.End()

	// 注入 trace context 到消息属性
	carrier := &propMapCarrier{props: msg.Properties}
	propagator := observability.GetGlobalPropagator()
	propagator.Inject(ctx, carrier)
	msg.Properties = carrier.props

	result, err := producer.Send(ctx, msg)
	if err != nil {
		span.RecordError(err)
	}
	return result, err
}

// ReceiveTrace 接收消息，带 tracing，并从消息中提取 trace context
func ReceiveTrace(ctx context.Context, consumer pulsar.Consumer) (pulsar.Message, error) {
	ctx, span := observability.StartSpan(ctx, "pulsarx.Receive")
	defer span.End()

	msg, err := consumer.Receive(ctx)
	if err != nil {
		span.RecordError(err)
		return msg, err
	}

	// 从消息属性中提取 trace context，创建 consume 子 span
	if msg.Properties() != nil {
		carrier := &propMapCarrier{props: msg.Properties()}
		propagator := observability.GetGlobalPropagator()
		extractCtx := propagator.Extract(ctx, carrier)

		_, consumeSpan := observability.StartSpan(extractCtx, "pulsarx.consume",
			trace.WithAttributes(
				attribute.String("msg_id", fmt.Sprintf("%v", msg.ID())),
			),
		)
		consumeSpan.End()
	}

	return msg, nil
}

// AckTrace 确认消息，带 tracing
func AckTrace(ctx context.Context, consumer pulsar.Consumer, msg pulsar.Message) error {
	_, span := observability.StartSpan(ctx, "pulsarx.Ack",
		trace.WithAttributes(
			attribute.String("msg_id", fmt.Sprintf("%v", msg.ID())),
		),
	)
	defer span.End()

	consumer.Ack(msg)
	return nil
}