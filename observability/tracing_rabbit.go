package observability

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
)

// amqpHeaderCarrier 适配 OTel propagation 接口以读写 RabbitMQ Headers (amqp.Table)
type amqpHeaderCarrier struct {
	headers amqp.Table
}

func (c amqpHeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	if val, ok := c.headers[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (c amqpHeaderCarrier) Set(key string, value string) {
	if c.headers == nil {
		return // 理论上不会走到这里，因为外面会初始化
	}
	c.headers[key] = value
}

func (c amqpHeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, 0, len(c.headers))
	for k := range c.headers {
		keys = append(keys, k)
	}
	return keys
}

// InjectRabbitTrace 将 Context 中的 OTel TraceID 注入到 RabbitMQ amqp.Table 中
func InjectRabbitTrace(ctx context.Context, headers amqp.Table) {
	if headers == nil {
		return
	}
	carrier := amqpHeaderCarrier{headers: headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractRabbitTrace 从 RabbitMQ amqp.Table 中提取 OTel TraceID，生成新的 Context
func ExtractRabbitTrace(ctx context.Context, headers amqp.Table) context.Context {
	if headers == nil {
		return ctx
	}
	carrier := amqpHeaderCarrier{headers: headers}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
