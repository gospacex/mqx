# MQX

MQX 是一个 **Go SDK**，为 **8 种消息队列驱动**提供统一的连接管理层。功能包括基于指纹的连接池、CAS 连接雪崩防护、优雅关闭、配置热加载，以及 OpenTelemetry + Prometheus 可观测性——同时返回原生 SDK 类型（无包装类型）。

## 支持的驱动

| 驱动 | 后端 SDK | 返回类型 |
|------|----------|----------|
| Kafka | confluentinc/confluent-kafka-go/v2 | `*kafka.Producer`, `*kafka.Consumer` |
| RabbitMQ | rabbitmq/amqp091-go | `*amqp.Connection` |
| Redis Streams | redis/go-redis/v9 | `*redis.Client`, `*redis.ClusterClient` |
| RocketMQ | apache/rocketmq-client-go/v2 | `rocketmq.Producer`, `rocketmq.PushConsumer` |
| Apache Pulsar | apache/pulsar-client-go | `pulsar.Producer`, `pulsar.Consumer` |
| NATS | nats-io/nats.go | `any`（连接复用） |
| NSQ | nsqio/go-nsq | `*nsq.Producer`, `*nsq.Consumer` |
| MQTT | eclipse/paho.mqtt.golang | `mqtt.Client` |

## API 接口

每个驱动提供一致的 16 个函数接口（含 panic 变体）：

### 生产者
| 函数 | 说明 |
|------|------|
| `P(path)` | 从 YAML 创建生产者 |
| `PPS(path)` | 从 YAML 创建单节点生产者 |
| `PPC(path)` | 从 YAML 创建集群生产者 |
| `POS(cfg)` | 从 Config 对象创建单节点生产者 |
| `POC(cfg)` | 从 Config 对象创建集群生产者 |
| `MustP/PPS/PPC/POS/POC` | 错误时 panic 的变体 |

### 消费者
| 函数 | 说明 |
|------|------|
| `C(path)` | 从 YAML 创建消费者 |
| `CPS(path)` | 从 YAML 创建单节点消费者 |
| `CPC(path)` | 从 YAML 创建集群消费者 |
| `COS(cfg)` | 从 Config 对象创建单节点消费者 |
| `COC(cfg)` | 从 Config 对象创建集群消费者 |
| `MustC/CPS/CPC/COS/COC` | 错误时 panic 的变体 |

### 生命周期（所有驱动）
| 方法 | 说明 |
|------|------|
| `Shutdown(ctx)` | 带超时的优雅关闭 |
| `Reload(path)` | 配置热加载 |
| `HealthCheck()` | 返回驱动健康状态 |

## 快速开始

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
    // p 是 *kafka.Producer — 直接使用
    defer p.Shutdown(context.Background())
}
```

## 配置

MQX 使用统一的 YAML 配置模型，支持所有 8 种驱动：

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

## 类型化错误

MQX 提供 17 种类型化错误码，附带 driver/topic/group 上下文：

- `ConfigError`, `ConnectionError`, `TLSError`, `SendError`, `ConsumeError`
- `AckError`, `PoolError`, `TimeoutError`, `ShutdownError`
- 兼容 `errors.Is()` / `errors.As()`

## 可观测性

- **链路追踪**：OpenTelemetry，可插拔导出器（gRPC、HTTP、Kafka topic、Redis stream）
- **指标**：Prometheus 生产/消费成功和错误计数器
- **原生指标**：队列长度、飞行中消息数、连接池状态

```go
import "github.com/gospacex/mqx/observability"

cfg := observability.DefaultConfig()
tp, err := observability.InitTracing(cfg)
defer tp.Shutdown(context.Background())
```

## 项目状态

MQX 处于**活跃开发阶段**（v0.0.4）。所有 8 个驱动的 API 已可用：

- ✅ 全部 8 个驱动的生产者和消费者构造器
- ✅ 优雅关闭和配置热加载
- ✅ 类型化错误系统
- ✅ OpenTelemetry 链路追踪集成
- ✅ Prometheus 指标
- ✅ 基于配置指纹的连接池
- ❌ **测试覆盖率有限**（仅有 config 和 observability 的单元测试，无驱动集成测试）
- ❌ **无可运行示例**（example 目录已移除）
- ❌ **无 CI/CD 流水线**

## 许可

MIT
