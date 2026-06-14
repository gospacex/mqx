<div align="center">

  <h1>MQX</h1>
  <p><b>One Line of Code, Production Ready.</b></p>
  <p>专为 Go 语言打造的<strong>企业级消息队列基建与生命周期管理底座</strong>。</p>

  [![Go Version](https://img.shields.io/badge/go-1.26-blue.svg)](https://golang.org/dl/)
  [![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
  [![Production Ready](https://img.shields.io/badge/Tier_1-Production_Ready-orange.svg)]()
  [![Drivers](https://img.shields.io/badge/MQ%20Drivers-8-blueviolet.svg)]()
  [![Trace%20Backends](https://img.shields.io/badge/Trace%20Backends-3-9cf.svg)]()
</div>

<br/>

一行命令返回 8 大主流消息队列（**Kafka / RabbitMQ / RocketMQ / Redis / Pulsar / NATS / NSQ / MQTT**）的官方 SDK 强类型指针。
MQX **不代理、不封装、不阉割**任何消息收发逻辑，所有防并发雪崩建连、TLS 鉴权、集群容灾防抖、拓扑自动声明、零侵入可观测性、内存无损优雅关闭由底座吸收，调用方拿到的是 100% 原生 SDK。

---

## 🌟 核心特征

- **🔥 多路复用连接池**：YAML 配 `instance_pool_size: N`，底层以 `atomic` 轮询 N 个独立原生 Socket，单连接内存锁竞争骤降
- **🔥 无损热更新**：密码过期 / 集群迁移时 `kafkax.Reload("mq.yaml")` 原子置换长连接池，旧池后台平滑排空，**业务 0 感知**
- **👁️ 零侵入可观测性**：按 `cfg.Trace.Enabled` 开关，OTel 链路 + Prometheus 旁路拦截底层 C 引擎指标（In-Flight / Queue Length / RTT）
- **🛡️ 极致防抖**：基于配置切片字典序哈希 + CAS 双检锁，防范万级并发启动时的连接雪崩
- **🧱 绝对类型安全**：每包 `PPS/PPC/CPS/CPC` 矩阵直接返回具体 SDK 指针（如 `*redis.Client` / `*kafka.Producer`），告别 `interface{}` 断言
- **🛠️ 自动拓扑基建**：RabbitMQ 自动声明 Exchange / Queue / DLX；Redis Stream 自动建游标

---

## 🛡️ 极致防抖：按配置指纹的多池复用

`utils.ConfigFingerprint` + 各 driver 的 `getOrCreateProducerPool` 共同构成 mqx 的核心防雪崩架构——**不同入参构建不同指纹，不同指纹键控不同池**。既不靠单例（避免业务串扰），也不靠盲建连（避免万级并发时的 broker 端雪崩）。

### 1. 指纹计算（`utils/fingerprint.go:14-40`）

```go
func ConfigFingerprint(cfg *mqx.Config) string {
    if cfg == nil { return "" }

    addrs := make([]string, len(cfg.Addrs))
    copy(addrs, cfg.Addrs); sort.Strings(addrs)        // 切片排序防抖

    topics := make([]string, len(cfg.Consumer.Topics))
    copy(topics, cfg.Consumer.Topics); sort.Strings(topics)

    raw := fmt.Sprintf("%s|%s|%v|%s|%s|%s|%v",
        cfg.Driver, cfg.Mode, addrs,                   // 驱动 + 拓扑
        cfg.Auth.Username,                             // 鉴权
        cfg.Producer.Topic,                            // 业务路由
        cfg.Consumer.Group, topics,                    // 消费组
    )
    h := sha256.Sum256([]byte(raw))
    return hex.EncodeToString(h[:16])                   // 32 hex 指纹
}
```

`Driver | Mode | Addrs(排序) | User | Producer.Topic | Consumer.Group | Topics(排序)` 任一字段不同 → 不同指纹 → 不同池。

### 2. 为什么不是单例

| 场景 | 单例的后果 | 指纹池的效果 |
| :--- | :--- | :--- |
| 两个业务连 `kafka-a:9092` / `kafka-b:9092` | 互相抢锁、addr 错位、串数据 | 各自拿自己的池，零串扰 |
| 同 broker 不同 SASL 用户（生产/消费分离） | 鉴权互相覆盖 | 指纹含 username，分池 |
| `mq.yaml#prod` 与 `mq.yaml#staging` 共存 | 后启动者覆盖前者，前者全断 | 拓扑不同即不同指纹，并存 |
| 1w goroutine 同帧拉连 | `init()` 单 `Dial()` 被打成雪崩 | CAS 双检：只建 1 次，其余复用 |

### 3. CAS 双检锁防雪崩

```text
Goroutine #1 ─┐                    sync.Map[fingerprint]
Goroutine #2 ─┤   Load(fingerprint) ── miss ──┐
   ...        │                                 │
Goroutine N ─┘                                   ▼
                                       LoadOrStore(fingerprint, build())
                                            │
                                ┌───────────┴───────────┐
                                ▼                       ▼
                          抢到 store 权            看到已存在
                                │                       │
                                ▼                       ▼
                          建 N 路 socket           return existing
                                │                       │
                                ▼                       ▼
                          Store 完成 ◄──────────────────┘
                                │
                                ▼
                          return new pool
                          (atomic.AddUint64
                           % size 轮询)
```

N 路并发只产生 1 次 broker 握手，单连接内存锁竞争骤降。

### 4. 与其他特性的协同

- **多路复用**：池内 `atomic.AddUint64(&counter, 1) % size` 轮询 N 个独立原生 socket
- **无损热更新**：`Reload("mq.yaml")` → 新 cfg → 新指纹 → 新池启动 → 老池后台 `Close()` 排空 → 业务 0 感知
- **类型安全**：每包 `PPS/PPC/CPS/CPC` 16 字真言 API 拿到的就是 `*kafka.Producer` / `*redis.Client` 原生 SDK 指针

### 5. 为什么需要 N 路连接：MQ 协议层的硬限制

`instance_pool_size: N` **不是"性能调优开关"**——8 个 MQ 协议都对单 connection 有硬性约束，单连接在很多真实场景下根本"不够用"：

| 引擎 | 单 connection 的硬限制 | 多连接的必要性 |
| :--- | :--- | :--- |
| **RabbitMQ** | 单 connection 最多 2048 channel；channel **非线程安全**，跨 goroutine 必须独占 | 跨 goroutine 并发 publish 时 N connection 才能彻底避开 channel 锁 |
| **Kafka** | broker 按 connection 限流；单 TCP 流吞吐有上限 | 多 broker / 多 partition 高吞吐场景下，N connection 摊薄 socket 抖动与单流拥塞 |
| **RocketMQ** | Producer / Consumer 强绑定 connection，跨实例不共享 | 生产/消费分离部署；同进程多 producer 隔离故障域 |
| **NATS** | 单 connection 同时承担 pub/sub/jetstream | reconnect 风暴期间，备用 connection 接管避免业务被阻塞 |
| **Pulsar** | Producer/Reader 各自持锁 | 多 topic 高扇出场景下，单 connection 锁竞争激烈 |
| **NSQ** | 单 producer 内有发送缓冲 | 生产/消费分别独立 pool 避免互相阻塞 |
| **MQTT** | 单 client 持有 keepalive 心跳 | 业务长任务卡住心跳时，备用 connection 接管保活 |
| **Redis Stream** | 单 connection 串行命令 | 高 QPS XADD 时，pipeline 之外的并发无解 |

#### ⚠️ 不适用场景：gorm / 缓存 / HTTP 客户端

`*sql.DB`（gorm 底层）/ `*redis.Client`（go-redis）/ `*http.Client` **都自带连接池**（`SetMaxOpenConns` / `PoolSize` / `Transport.MaxIdleConnsPerHost`），它们已经内部 round-robin。**在外面再包一层 N 路轮询 = 4× 连接数 + 内存膨胀 + 锁竞争**，是反模式。正确做法是**调 SDK 自带池参数**，而不是在外面再套一层。

**本池只解决"channel / partition / 单 connection 协议锁"导致的真问题**——这个限制是 MQ 协议层带来的，gorm / 缓存 / HTTP 客户端的 SDK 已经自行消化。

### 6. 源码锚点

- 指纹算法：`utils/fingerprint.go:14` `ConfigFingerprint`
- 池注册表：各 driver 内部 `sync.Map` + `getOrCreateProducerPool`，全 8 驱动 18 处调用
- 池原子轮询：`rabbitx/pool.go:67-72` `ConnectionPool.Get`（`atomic.AddUint64` 模 size 轮询），kafkax 同源
- 池多路复用必要性论证：见 §5

---

## 📦 8 大引擎矩阵

| MQ 引擎 | 引入子包 | 底层驱动 SDK | 框架返回的原生类型 |
| :--- | :--- | :--- | :--- |
| **Kafka** | `mqx/kafkax` | `confluent-kafka-go/v2` | `*kafka.Producer` / `*kafka.Consumer` |
| **RabbitMQ** | `mqx/rabbitx` | `amqp091-go` | `*amqp091.Connection` |
| **Redis** | `mqx/redisx` | `go-redis/v9` | `*redis.Client` / `*redis.ClusterClient` |
| **RocketMQ** | `mqx/rocketx` | `rocketmq-client-go/v2` | `rocketmq.Producer` / `rocketmq.PushConsumer` |
| **Pulsar** | `mqx/pulsarx` | `pulsar-client-go` | `pulsar.Producer` / `pulsar.Consumer` |
| **NATS / JS** | `mqx/natsx` | `nats.go` | `*nats.Conn` / `nats.JetStreamContext` |
| **NSQ** | `mqx/nsqx` | `go-nsq` | `*nsq.Producer` / `*nsq.Consumer` |
| **MQTT** | `mqx/mqttx` | `paho.mqtt.golang` | `mqtt.Client` |

每个 driver 都已挂载 `*Trace` 追踪函数（`kafkax.ProduceTrace` / `redisx.XAddTraceWithMaxLen` / `pulsarx.SendTrace` / `mqttx.PublishTrace` / `natsx.PublishTrace` / `nsqx.PublishTrace` / `rabbitx.PublishWithContextTrace` / `rocketx.SendSyncTrace`），由 `cfg.Trace.Enabled` 开关。

---

## ⚡ 极速上手 (Quick Start)

### 1. 编写 `mq.yaml`

单机 / 集群统一通过 `mqx.Config` 树状模型描述：

```yaml
# mq.yaml
kafka_prod:
  driver: kafka
  mode: cluster
  addrs: ["broker1:9092", "broker2:9092"]
  instance_pool_size: 5   # 开启 5 路复用连接池
  auth: { username: "admin", password: "pwd" }
  producer:
    topic: "order.events"
  trace:
    enabled: true
    exporter: jaeger      # jaeger | redis_stream | kafka_topic

rabbit_dev:
  driver: rabbitmq
  mode: single
  addrs: ["127.0.0.1:5672"]
  dlq:
    enabled: true
    topic: "dev.dlq"      # 自动声明死信拓扑
```

### 2. 一行代码获取强类型指针

MQX 的 **16 字真言 API**：`P(Producer)/C(Consumer)` × `S(Single)/C(Cluster)`：

```go
import (
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/gospacex/mqx/kafkax"
    "github.com/gospacex/mqx/rabbitx"
)

// 1. Kafka 集群版 Producer（返回底层 5 路轮询池里的一个原生指针）
kp, err := kafkax.PPC("mq.yaml#kafka_prod")
if err != nil { log.Fatal(err) }
kp.Produce(&kafka.Message{TopicPartition: ..., Value: []byte("hello")}, nil)

// 2. RabbitMQ 单机版 Consumer
rc, err := rabbitx.CPS("mq.yaml#rabbit_dev")
if err != nil { log.Fatal(err) }
ch, _ := rc.Channel()
ch.Consume("...", true, ...)
```

> 💡 **生产提示**：`main.go` 启动期推荐 `MustPPC` 强硬接口，fail-fast 早暴露；运行时推荐带 `error` 返回的 `PPC` 配合降级落库。

### 3. 16 字真言 API 全表

| 场景 | 通过 YAML 路径加载 | 通过 `mqx.Config` 直传 | 含意 |
| :--- | :--- | :--- | :--- |
| **单机生产者** | `P(path)` / `PPS(path)` | `POS(obj)` | PPS = Producer Single |
| **集群生产者** | `PPC(path)` | `POC(obj)` | PPC = Producer Cluster |
| **单机消费者** | `C(path)` / `CPS(path)` | `COS(obj)` | CPS = Consumer Single |
| **集群消费者** | `CPC(path)` | `COC(obj)` | CPC = Consumer Cluster |

每个函数都提供 `Must*` 强硬版本（panic on error）与正常返回 error 版本。

---

## 👁️ 零侵入可观测性

`observability` 包封装 OpenTelemetry tracer + Prometheus metrics HTTP 服务，三种 trace backend 可热切换。

### 3 种 trace backend

```go
import "github.com/gospacex/mqx/observability"

cleanup, err := observability.InitTracing(ctx, &observability.Config{
    Enabled:        true,
    ServiceName:    "order-svc",
    JaegerEndpoint: "localhost:4317",

    // 三选一，默认为 jaeger（向后兼容）
    Backend: "jaeger",        // OTLP gRPC → jaeger-collector
    // Backend: "redis_stream",  // 自研 exporter → Redis Stream（参见 observability/exporter/redisstream）
    // Backend: "kafka_topic",   // 自研 exporter → Kafka topic（参见 observability/exporter/kafkatopic）

    // 当 Backend=redis_stream 时必填
    RedisClient: redisClient, RedisStream: "trace:order-svc",
    // 当 Backend=kafka_topic 时必填
    KafkaProducer: kafkaProducer, KafkaTopic: "trace-spans",
})
defer cleanup(ctx)
```

| Backend | 适用 | 配套 exporter |
| :--- | :--- | :--- |
| **`jaeger`** | 标准 OTLP gRPC 链路 | OTel 官方 `otlptracegrpc` |
| **`redis_stream`** | 已有 Redis 集群，无独立 trace infra | `observability/exporter/redisstream`（自研） |
| **`kafka_topic`** | 复用业务 Kafka 作为 trace 通道 | `observability/exporter/kafkatopic`（自研） |

> 自研两个 exporter 与 driver **完全解耦**：8 个 driver 都不会 import 任何 exporter，backend 切换完全由 `cfg.Trace.Backend` / env-var 决定。
> 详见 `observability/server.go:27-32` 的 backend 常量定义和 `observability/exporter/{redisstream,kafkatopic}/exporter.go`。

### Prometheus 指标

```go
srv := observability.StartMetricsServer(&observability.Config{Enabled: true, MetricsPort: 9091})
defer observability.ShutdownMetricsServer(ctx)
// 访问 http://localhost:9091/metrics
// 关键指标: mqx_native_in_flight_requests, mqx_native_queue_length, mqx_native_rtt_seconds
```

---

## 🌍 `${env:VAR}` 配置占位符

`observability.ExpandEnvVars` 解析 OTel confmap 兼容的 `${env:VAR}` 与 `${env:VAR:-default}`，未匹配保留原字面量。配合 yaml 可一行切换多套环境：

```yaml
trace:
  enabled: true
  exporter: ${env:MQ_TRACE_BACKEND:-jaeger}
  endpoint: ${env:JAEGER_ENDPOINT:-localhost:4317}
  stream:   ${env:TRACE_REDIS_STREAM:-trace:redisx:single}
  topic:    ${env:TRACE_KAFKA_TOPIC:-trace-spans}
addrs:
  - ${env:KAFKA_BROKER:-localhost:9092}
```

源码：`observability/server.go:50-66`（`ExpandEnvVars`）。设计目的：让一份 `mq.yaml` 在 e2e 测试里通过 `os.Setenv` 切换 6 种组合（3 backend × 2 topology），详见 `example/*_test/e2e_test.go`。

---

## 🛑 优雅关闭 (Graceful Shutdown)

收到 `SIGTERM` / `SIGINT` 时一行代码确保内存积压排空与消费者安全离线：

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

// 各 driver 内部并发排空 Producer pool + 关闭 Consumer
kafkax.Shutdown(ctx)
redisx.Shutdown(ctx)
rabbitx.Shutdown(ctx)
// ... 其余 5 个 driver 同理

// 最后关闭 OTel TracerProvider（force flush + 2s 等待 + shutdown）
if cleanup != nil { cleanup(ctx) }
observability.ShutdownMetricsServer(ctx)
```

实现细节（`kafkax/kafkax.go`、`redisx/redisx.go` 等）：
- **Kafka**：`Flush(timeoutMs)` 等待在途消息推完 + `Consumer.Close()`
- **Redis**：`client.Close()` 等连接池排空
- **NATS**：`Drain()` 语义
- **NSQ**：`StopChan` 通知

---

## 🧪 E2E 测试体系

### 矩阵：3 backend × 2 topology = 6 组合 / driver

`example/<driver>_test/e2e_test.go`（8 份）按矩阵产出 48 个 e2e test func，模式 C（一份 yaml 切 6 组合）：

```go
func TestKafkax_Jaeger_Single(t *testing.T)      { runKafkaxE2E(t, "jaeger", "single") }
func TestKafkax_Jaeger_Cluster(t *testing.T)     { runKafkaxE2E(t, "jaeger", "cluster") }
func TestKafkax_RedisStream_Single(t *testing.T)  { runKafkaxE2E(t, "redis_stream", "single") }
func TestKafkax_RedisStream_Cluster(t *testing.T) { runKafkaxE2E(t, "redis_stream", "cluster") }
func TestKafkax_KafkaTopic_Single(t *testing.T)   { runKafkaxE2E(t, "kafka_topic", "single") }
func TestKafkax_KafkaTopic_Cluster(t *testing.T)  { runKafkaxE2E(t, "kafka_topic", "cluster") }
```

> 6 func 全部单行 `runKafkaxE2E(t, backend, topology)` 收口（`example/kafkax_test/e2e_test.go:21-45`）。
> 7 driver 平移为后续 `expand-e2e-trace-depths-7-drivers` 变更（设计见 `openspec/changes/archive/2026-06-12-expand-kafkax-e2e-trace-depths/design.md` 的 Migration Plan v2.B）。

### 4 阶 trace 深度（kafkax 已落地，规格 `openspec/specs/e2e-trace-depths/spec.md`）

| 深度 | 验证项 | 共享断言器 |
| :---: | :--- | :--- |
| **depth-1** | 单次 round-trip 后 trace backend 出现该 trace_id 的 span | `assert.AssertSpanInBackendWithTimeout` |
| **depth-2** | span 含 `messaging.system` / `messaging.destination` 等 OTel 语义属性 | `assert.AssertSpanFields` + `SpanExpect` |
| **depth-3** | context 跨进程传播（jaeger 严格 ParentSpanID 匹配，redis/kafka 宽松 TraceID 匹配） | `assert.AssertTraceContext` / `AssertTraceContextLoose` |
| **depth-4a** | backend 已 shutdown 时 producer 仍能写 | `observability.Shutdown` + `assert.ProduceOnce` |
| **depth-4b** | 100 并发 round-trip，consumer 收 ≥ 90 条 | `assert.ProduceConsumeConcurrent` |
| **depth-4c** | `nil` / empty / 1MB 异常 payload 不 panic | `assert.ProduceOnce` 3 payload |

### `example/assert` 共享断言层

`example/assert/{trace,trace_helpers}.go` 是 e2e 通用基础设施：

| 入口 | 作用 |
| :--- | :--- |
| `NewSpanID(t)` | 生成 16-hex span id |
| `FetchSpansByTraceID(t, backend, traceID)` | 按 backend 分支拉 spans |
| `AssertSpanInBackendWithTimeout` | 30s 轮询断言 trace_id 出现 |
| `AssertSpanFields(t, backend, traceID, []SpanExpect)` | 断言 span 字段（`Kind==""` 跳过 Kind 校验） |
| `AssertTraceContext` / `AssertTraceContextLoose` | 严格/宽松 context 断言 |
| `ProduceConsume` / `ProduceConsumeWithSpanID` | 单 round-trip，3 值返回 `(payload, spanID, traceID)` |
| `ProduceConsumeConcurrent(t, N)` | N 并发 round-trip |
| `ProduceOnce` | 4a/4c 单次异常 payload |

3 backend 验证强度差异矩阵（详见 `example/assert/trace.go:50-66`）：jaeger 严格支持 Kind / ParentSpanID；redis_stream / kafka_topic 自研 exporter 不保留这两个字段，自动降级到 TraceID 宽松匹配。

### 跑测试

```bash
# 编译
go build ./...

# 单 driver 6 组合全跑
go test -race -v -count=1 ./example/kafkax_test/

# 8 driver 全跑
go test -race -v -count=1 ./example/.../

# observability 包自身单测（包含 ExpandEnvVars、InitTracing 装配等）
go test -race ./observability/...
```

无 docker 环境时，broker 不可达的子测试会按 `t.Skipf` 优雅跳过，不会 FAIL。

---

## 📁 代码组织

```
mqx/
├── config.go                    # 顶层 mqx.Config + 8 driver 专属 config
├── observability/               # 零侵入 OTel + Prometheus 装配
│   ├── server.go                # InitTracing / Shutdown / ExpandEnvVars
│   ├── tracer.go                # 公共 tracer 包装
│   ├── native_metrics.go        # 旁路拦截底层 SDK 指标
│   └── exporter/
│       ├── redisstream/         # 自研：span → Redis Stream (XAdd)
│       └── kafkatopic/          # 自研：span → Kafka topic (Produce)
├── kafkax/   rabbitx/   redisx/   rocketx/
├── pulsarx/  natsx/    nsqx/     mqttx/         # 8 个 driver 子包
├── example/                      # E2E 示例 + 测试
│   ├── assert/                   # 共享 e2e 断言层（trace, trace_helpers）
│   ├── kafkax_test/  redisx_test/  ...           # 8 driver e2e
│   └── *_test/main.go producer.go consumer.go mq.yaml
├── test/docker/                  # 8 driver × 单/集 拓扑的 docker-compose 模板
├── openspec/                     # OpenSpec 规格 + 变更档案
│   ├── specs/                    # 已落地的 spec（e2e-trace-depths 等）
│   └── changes/                  # active 变更
│       └── archive/              # 已归档变更（design.md / close-issues.md 等）
├── docs/                         # 设计文档与白皮书
└── utils/                        # 内部工具（ConfigFingerprint 等）
```

---

## 📖 官方文档矩阵

- ⚙️ **配置手册**: [docs/config.md](docs/config.md)
- 🚀 **架构黑科技**: [docs/enterprise_features.md](docs/enterprise_features.md)
- 🧠 **设计辩论**: [docs/design_debate.md](docs/design_debate.md)
- 🛡️ **质量验收**: [docs/ACCEPTANCE_REPORT.md](docs/ACCEPTANCE_REPORT.md)
- 🛰️ **trace exporter 路由**: [docs/trace_exporter_routing.md](docs/trace_exporter_routing.md)
- 📚 **专属指南**: [Kafkax](docs/spec_kafkax.md) · [Rabbitx](docs/spec_rabbitx.md) · [Redisx](docs/spec_redisx.md) · [Rocketx](docs/spec_rocketx.md) · [Pulsarx](docs/spec_pulsarx.md) · [Natsx](docs/spec_natsx.md) · [Nsqx](docs/spec_nsqx.md) · [Mqttx](docs/spec_mqttx.md)
- 📋 **已落地规格**: [openspec/specs/](openspec/specs/) — 如 [e2e-trace-depths](openspec/specs/e2e-trace-depths/spec.md)

---

## 🛠️ 开发与规范

- Go 1.26+，所有 PR 必跑 `go test -race ./...` + `go build ./...`
- 单一文件 ≤ 500 行硬约束
- 禁止 `fmt.Sprintf` 拼接生成配置文件，统一用 `embed` / `text/template`
- 变更流程遵循 OpenSpec + SDDflow（spec → plan → build → close → archive）
- 归档的变更保留在 `openspec/changes/archive/<date>-<name>/`，含 `proposal.md` / `design.md` / `tasks.md` / `close-issues.md` 全套

---

*Built with ❤️ for Production-Ready Go Microservices.*
