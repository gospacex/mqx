# MQX

MQX is a **Go SDK** that provides a unified connection-management layer for **8 message queue drivers**. It delivers fingerprint-based connection pooling, CAS-based connection avalanche prevention, graceful shutdown, hot-reload of broker configs, and OpenTelemetry + Prometheus observability — all while returning native SDK types (no wrapper types).

## Supported Drivers

| Driver | Backend SDK | Return Type |
|--------|------------|-------------|
| Kafka | confluentinc/confluent-kafka-go/v2 | `*kafka.Producer`, `*kafka.Consumer` |
| RabbitMQ | rabbitmq/amqp091-go | `*amqp.Connection` |
| Redis Streams | redis/go-redis/v9 | `*redis.Client`, `*redis.ClusterClient` |
| RocketMQ | apache/rocketmq-client-go/v2 | `rocketmq.Producer`, `rocketmq.PushConsumer` |
| Apache Pulsar | apache/pulsar-client-go | `pulsar.Producer`, `pulsar.Consumer` |
| NATS | nats-io/nats.go | `any` (conn multiplexing) |
| NSQ | nsqio/go-nsq | `*nsq.Producer`, `*nsq.Consumer` |
| MQTT | eclipse/paho.mqtt.golang | `mqtt.Client` |

## API Surface

Each driver exposes a consistent 16-function API (10 producer + 10 consumer constructors with `Must` variants):

### Producer
| Function | Meaning |
|----------|---------|
| `P(path)` | Producer from YAML |
| `PPS(path)` | Producer Single from YAML |
| `PPC(path)` | Producer Cluster from YAML |
| `POS(cfg)` | Producer Single from Config object |
| `POC(cfg)` | Producer Cluster from Config object |
| `MustP/PPS/PPC/POS/POC` | Panic-on-error variants |

### Consumer
| Function | Meaning |
|----------|---------|
| `C(path)` | Consumer from YAML |
| `CPS(path)` | Consumer Single from YAML |
| `CPC(path)` | Consumer Cluster from YAML |
| `COS(cfg)` | Consumer Single from Config object |
| `COC(cfg)` | Consumer Cluster from Config object |
| `MustC/CPS/CPC/COS/COC` | Panic-on-error variants |

### Lifecycle (all drivers)
| Method | Description |
|--------|-------------|
| `Shutdown(ctx)` | Graceful shutdown with timeout |
| `Reload(path)` | Hot-reload broker config |
| `HealthCheck()` | Return driver health status |

## Quick Start

```go
package main

import (
    "context"
    "github.com/gospacex/mqx/kafkax"
)

func main() {
    p, err := kafkax.PPS("config.yaml")
    if err != nil {
        panic(err)
    }
    // p is a *kafka.Producer — use it directly
    defer p.Shutdown(context.Background())
}
```

## Configuration

MQX uses a unified YAML config model supporting all 8 drivers:

```yaml
kafka:
  brokers:
    - localhost:9092
  producer:
    topic: my-topic
    timeout: 10s
    batch_size: 100
  consumer:
    group_id: my-group
    topic: my-topic
```

## Typed Errors

MQX provides typed error codes (17 categories) with driver/topic/group context:

- `ConfigError`, `ConnectionError`, `TLSError`, `SendError`, `ConsumeError`
- `AckError`, `PoolError`, `TimeoutError`, `ShutdownError`
- Compatible with `errors.Is()` / `errors.As()`

## Observability

- **Tracing**: OpenTelemetry with pluggable exporters (gRPC, HTTP, Kafka topic, Redis stream)
- **Metrics**: Prometheus counters for produce/consume success and errors
- **Native metrics**: Queue length, in-flight messages, pool stats per driver

```go
import "github.com/gospacex/mqx/observability"

cfg := observability.DefaultConfig()
tp, err := observability.InitTracing(cfg)
defer tp.Shutdown(context.Background())
```

## Project Status

MQX is in **active development** (v0.0.4). The API is functional across all 8 drivers with:

- ✅ Full producer + consumer constructors for all 8 drivers
- ✅ Graceful shutdown and hot-reload
- ✅ Typed error system
- ✅ OpenTelemetry tracing integration
- ✅ Prometheus metrics
- ✅ Config fingerprint-based connection pooling
- ❌ **Limited test coverage** (unit tests for config and observability only; no driver integration tests)
- ❌ **No runnable examples** (example directory removed)
- ❌ **No CI/CD pipeline**

## License

MIT
