package observability

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
)

// kafkaHeaderCarrier 适配 OTel propagation 接口以读写 Kafka Headers
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key string, value string) {
	if c.headers == nil {
		return
	}
	*c.headers = append(*c.headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

func (c kafkaHeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectTrace 将 Context 中的 OTel TraceID 注入到 Kafka Message Headers 中
func InjectTrace(ctx context.Context, headers *[]kafka.Header) {
	if headers == nil {
		return
	}
	carrier := kafkaHeaderCarrier{headers: headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractTrace 从 Kafka Message Headers 提取 OTel TraceID，生成新的 Context
func ExtractTrace(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := kafkaHeaderCarrier{headers: &headers}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
