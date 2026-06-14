package nsqx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gospacex/mqx/observability"
	"github.com/nsqio/go-nsq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// tracedMessage 用于在 NSQ 消息体中嵌入 trace context
// NSQ 协议不原生支持消息 headers，因此采用 JSON 包裹方式传递 traceparent
type tracedMessage struct {
	Traceparent string `json:"tp"`
	Payload     []byte `json:"p"`
}

func marshalTraced(tp string, body []byte) ([]byte, error) {
	return json.Marshal(tracedMessage{
		Traceparent: tp,
		Payload:     body,
	})
}

func unmarshalTraced(data []byte) (string, []byte, bool) {
	var tm tracedMessage
	if err := json.Unmarshal(data, &tm); err != nil || tm.Traceparent == "" {
		return "", data, false
	}
	return tm.Traceparent, tm.Payload, true
}

// PublishTrace 发送消息，带 tracing。
// 自动将 trace context 嵌入消息 body（JSON 包裹），消费者使用 AddHandlerTrace 可自动解包。
func PublishTrace(ctx context.Context, producer *nsq.Producer, topic string, body []byte) error {
	ctx, span := observability.StartSpan(ctx, "nsqx.Publish",
		trace.WithAttributes(attribute.String("topic", topic)),
	)
	defer span.End()

	// 注入 trace context
	carrier := &messageCarrier{data: make(map[string]string)}
	propagator := observability.GetGlobalPropagator()
	propagator.Inject(ctx, carrier)

	wrapped, err := marshalTraced(carrier.data["traceparent"], body)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("nsqx.PublishTrace marshal: %w", err)
	}

	err = producer.Publish(topic, wrapped)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// messageCarrier 实现 propagation.TextMapCarrier，用于注入 trace context
type messageCarrier struct {
	data map[string]string
}

func (c *messageCarrier) Get(key string) string {
	if c.data == nil {
		return ""
	}
	return c.data[key]
}

func (c *messageCarrier) Set(key, value string) {
	if c.data == nil {
		c.data = make(map[string]string)
	}
	c.data[key] = value
}

func (c *messageCarrier) Keys() []string {
	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	return keys
}

// tracedHandler 包装 nsq.Handler，自动提取 trace context 并创建消费 span
type tracedHandler struct {
	ctx     context.Context
	handler nsq.HandlerFunc
}

func (h *tracedHandler) HandleMessage(msg *nsq.Message) error {
	extractCtx := h.ctx

	// 尝试解包 trace context
	if tp, payload, ok := unmarshalTraced(msg.Body); ok {
		carrier := &messageCarrier{data: map[string]string{"traceparent": tp}}
		propagator := observability.GetGlobalPropagator()
		extractCtx = propagator.Extract(h.ctx, carrier)
		msg.Body = payload
	}

	_, span := observability.StartSpan(extractCtx, "nsqx.consume",
		trace.WithAttributes(
			attribute.String("topic", msg.NSQDAddress),
		),
	)
	defer span.End()

	err := h.handler(msg)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// AddHandlerTrace 注册消息处理函数，带 tracing。
// 自动从消息 body 提取 trace context（由 PublishTrace 嵌入），形成 producer → consumer trace 链路。
func AddHandlerTrace(ctx context.Context, consumer *nsq.Consumer, handler nsq.HandlerFunc) {
	consumer.AddHandler(&tracedHandler{ctx: ctx, handler: handler})
}
