module github.com/gospacex/mqx

go 1.19

require (
github.com/apache/pulsar-client-go v0.15.0
github.com/apache/rocketmq-client-go/v2 v2.1.2
github.com/confluentinc/confluent-kafka-go/v2 v2.4.0
github.com/eclipse/paho.mqtt.golang v1.4.3
github.com/nats-io/nats.go v1.34.0
github.com/nats-io/nkeys v0.4.7
github.com/nsqio/go-nsq v1.1.0
github.com/prometheus/client_golang v1.19.0
github.com/rabbitmq/amqp091-go v1.9.0
github.com/redis/go-redis/v9 v9.5.0
go.opentelemetry.io/otel v1.24.0
go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0
go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.24.0
go.opentelemetry.io/otel/sdk v1.24.0
go.opentelemetry.io/otel/trace v1.24.0
gopkg.in/yaml.v3 v3.0.1
stathat.com/c/consistent v1.0.0
)

replace stathat.com/c/consistent => github.com/stathat/consistent v1.0.0
