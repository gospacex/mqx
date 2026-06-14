package mqttx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gospacex/mqx/observability"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// headerCarrier 实现 propagation.TextMapCarrier 用于注入 trace context
type headerCarrier struct {
	data map[string]string
}

func (c *headerCarrier) Get(key string) string {
	return c.data[key]
}

func (c *headerCarrier) Set(key, value string) {
	c.data[key] = value
}

func (c *headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	return keys
}

// tracedMessage 用于在 MQTT 消息体中嵌入 trace context
// MQTT v3.1.1 协议不支持消息 headers，因此采用 JSON 包裹方式传递 traceparent
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

// payloadToBytes 将 interface{} payload 转换为 []byte
func payloadToBytes(payload interface{}) ([]byte, error) {
	switch v := payload.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

// unwrappedMessage 包装 mqtt.Message，返回解包后的原始 payload
type unwrappedMessage struct {
	mqtt.Message
	originalPayload []byte
}

func (m *unwrappedMessage) Payload() []byte {
	return m.originalPayload
}

// PublishTrace 发布消息，带 tracing。
// 自动将 trace context 嵌入消息 body（JSON 包裹），消费者使用 SubscribeTrace 可自动解包。
func PublishTrace(ctx context.Context, client mqtt.Client, topic string, qos byte, retained bool, payload interface{}) error {
	ctx, span := observability.StartSpan(ctx, "mqttx.Publish",
		trace.WithAttributes(
			attribute.String("topic", topic),
			attribute.Int("qos", int(qos)),
		),
	)
	defer span.End()

	// 将 payload 转换为 []byte
	body, err := payloadToBytes(payload)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("mqttx.PublishTrace payload marshal: %w", err)
	}

	// 注入 trace context
	carrier := &headerCarrier{data: make(map[string]string)}
	propagator := observability.GetGlobalPropagator()
	propagator.Inject(ctx, carrier)

	wrapped, err := marshalTraced(carrier.data["traceparent"], body)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("mqttx.PublishTrace marshal: %w", err)
	}

	token := client.Publish(topic, qos, retained, wrapped)
	token.Wait()
	if token.Error() != nil {
		span.RecordError(token.Error())
		return token.Error()
	}
	return nil
}

// SubscribeTrace 订阅主题，带 tracing。
// 自动从消息 body 提取 trace context（由 PublishTrace 嵌入），形成 producer → consumer trace 链路。
// handler 收到的 msg.Payload() 返回的是解包后的原始 payload。
func SubscribeTrace(ctx context.Context, client mqtt.Client, topic string, qos byte, handler mqtt.MessageHandler) error {
	ctx, span := observability.StartSpan(ctx, "mqttx.Subscribe",
		trace.WithAttributes(
			attribute.String("topic", topic),
			attribute.Int("qos", int(qos)),
		),
	)
	defer span.End()

	wrappedHandler := func(client mqtt.Client, msg mqtt.Message) {
		extractCtx := ctx

		// 尝试解包 trace context
		if tp, payload, ok := unmarshalTraced(msg.Payload()); ok {
			carrier := &headerCarrier{data: map[string]string{"traceparent": tp}}
			propagator := observability.GetGlobalPropagator()
			extractCtx = propagator.Extract(ctx, carrier)
			msg = &unwrappedMessage{Message: msg, originalPayload: payload}
		}

		_, consumeSpan := observability.StartSpan(extractCtx, "mqttx.consume",
			trace.WithAttributes(
				attribute.String("topic", msg.Topic()),
				attribute.Int("msg_id", int(msg.MessageID())),
			),
		)
		defer consumeSpan.End()

		handler(client, msg)
	}

	token := client.Subscribe(topic, qos, wrappedHandler)
	token.Wait()
	if token.Error() != nil {
		span.RecordError(token.Error())
		return token.Error()
	}
	return nil
}
