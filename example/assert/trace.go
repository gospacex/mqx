// Package assert 提供 8 driver × 6 组合 e2e 测试共享的辅助函数。
//
// 设计原则：
//   - 不抽象 driver 各自的 Produce/Consume 签名（差异太大，强行抽象会变 leaky）
//     → 各 e2e_test.go 自行 import driver 包做 send/recv
//   - 抽象 trace_id 生成、backend 轮询、docker-compose 生命周期
//   - docker 不可达 / compose 文件缺失 → t.Skip，不 Fail
//
// # SOP：跑 e2e 前的手工准备
//
// 跑 e2e 前用户须按 backend 启动对应共享容器；assert.StartStack 只起自己
// 的 trace compose（redis/kafka）和 driver 集群 compose，jaeger 与 single
// 拓扑的 driver broker 由 tutorial 共享 compose 提前提供。
//
//	# 1. 跑 jaeger backend 组合（Single + Cluster 都用）
//	docker compose -f /Users/hyx/work/gowork/src/lego/tutorial/docker-compose.yml \
//	    up -d jaeger
//	# Cluster 拓扑还需 down tutorial 的对应业务 broker（避免端口冲突）：
//	docker compose -f /Users/hyx/work/gowork/src/lego/tutorial/docker-compose.yml \
//	    stop kafka redis rabbitmq pulsar emqx nsqlookupd nsqd \
//	        rocketmq-nameserver rocketmq-broker
//
//	# 2. 跑 redis_stream backend 组合
//	docker compose -f /Users/hyx/work/gowork/src/lego/tutorial/docker-compose.yml \
//	    up -d kafka rabbitmq pulsar emqx nsqlookupd nsqd \
//	        rocketmq-nameserver rocketmq-broker
//	# cachex-redis 端口 6379 与 mqx-trace-redis 撞，需先 down：
//	docker compose -f /Users/hyx/work/gowork/src/lego/tutorial/docker-compose.yml \
//	    stop redis
//	docker compose -f test/docker/trace/redis.yaml up -d --wait
//
//	# 3. 跑 kafka_topic backend 组合
//	docker compose -f /Users/hyx/work/gowork/src/lego/tutorial/docker-compose.yml \
//	    up -d redis rabbitmq pulsar emqx nsqlookupd nsqd \
//	        rocketmq-nameserver rocketmq-broker
//	# tutorial 的 kafka 端口 9092 与 mqx-trace-kafka 撞，trace backend 用
//	# host port 19092 解耦，但业务 broker 仍要 9092，Cluster 拓扑启
//	# test/docker/<driver>/cluster.yaml 替代；Single 拓扑用 tutorial.kafka 即可
//	docker compose -f test/docker/trace/kafka.yaml up -d --wait
//
//	# 4. 跑测试
//	go test -v -count=1 ./example/<driver>_test/                       # 6 组合全跑
//	go test -v -count=1 ./example/<driver>_test/ -run Test<Driver>_Jaeger_Cluster   # 单跑一条
//
// 端口冲突一览（host port）：
//   - 16686/4317/4318：tutorial.jaeger 与 test/docker/trace/jaeger.yaml 互斥（jaeger backend 由 tutorial 提供，不二次启）
//   - 6379：tutorial.cachex-redis 与 test/docker/trace/redis.yaml 互斥（trace/redis 起前先 down tutorial.redis）
//   - 9092：tutorial.kafka 与 test/docker/trace/kafka.yaml 互斥（trace/kafka 已用 host port 19092 解耦，但 Cluster 拓扑仍需 down tutorial.kafka）
//
// # 3 backend 验证强度差异矩阵
//
// 由于 2 个自定义 SpanExporter（observability/exporter/redisstream、kafkatopic）
// 的 spanRecord 不写 Kind / ParentSpanID / Status 字段（读源码确认），本包断言
// 函数按 backend 走两条路径：
//
//	| 深度      | jaeger          | redis_stream     | kafka_topic       |
//	|-----------|-----------------|------------------|-------------------|
//	| depth-1   | 严格（HTTP API）| 严格（XRange）   | 严格（consumer）  |
//	| depth-2   | 严格（Kind 校验）| 降级（Kind 跳过）| 降级（Kind 跳过）  |
//	| depth-3   | 严格（Parent）   | 降级（TraceID）  | 降级（TraceID）   |
//	| depth-4a  | 严格            | 严格            | 严格              |
//	| depth-4b  | 严格            | 严格            | 严格              |
//	| depth-4c  | 严格            | 严格            | 严格              |
//
// 改写 2 个 exporter 的 spanRecord 写入 Kind/ParentSpanID/Status 属于 v2.A 路径
// （见 design.md R10 与 Migration Plan 阶段 3），不在本次范围。
package assert

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

const (
	BackendJaeger      = "jaeger"
	BackendRedisStream = "redis_stream"
	BackendKafkaTopic  = "kafka_topic"

	TopologySingle  = "single"
	TopologyCluster = "cluster"

	defaultAssertTimeout = 15 * time.Second
	pollInterval         = 500 * time.Millisecond
)

// repoRoot 计算 example/<driver>_test 相对仓库根的路径。
// 约定：e2e_test.go 位于 example/<driver>_test/，test/docker 在 repo 根。
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root, err := filepath.Abs(filepath.Join(wd, "..", ".."))
	if err != nil {
		t.Fatalf("abs repo root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Skipf("repo root not found from %q (expected ../..): %v", wd, err)
	}
	return root
}

// StartStack 启动 driver 拓扑对应的 docker-compose + 对应 trace backend 容器。
//
// driver    = "redisx" | "kafkax" | ... (无 _test 后缀)
// topology  = "single" | "cluster"
// backend   = "jaeger" | "redis_stream" | "kafka_topic"
//
// single 拓扑假定 driver broker 已经由共享的 tutorial docker-compose
// （lepo/tutorial/docker-compose.yml）起好（redis / kafka / nsqd / pulsar /
// rabbitmq / emqx / rocketmq-nameserver+broker 等），因此本函数在 single
// 时**跳过** driver 容器启停 —— 共享 compose 是 single 模式的真源。
// cluster 拓扑仍走 test/docker/<driver>/cluster.yaml 拉起专属 broker 集群。
//
// 若 docker 不可用 → t.Skip；若 compose 文件缺失 → t.Skip。
// t.Cleanup 自动 down -v。
func StartStack(t *testing.T, driver, topology, backend string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available, skipping e2e")
	}
	root := repoRoot(t)

	// 1. trace backend 容器（redis / kafka）—— 三个独立 compose
	//
	// backend=jaeger 时跳过启停：tutorial 共享的 jaeger 容器已绑 16686/4317/4318，
	// 二次 up 必然端口冲突；测试代码默认信任 localhost:16686/4317 可用，由 SOP
	// 保证 tutorial 已 up（见包注释 "SOP" 段）。
	if backend == BackendJaeger {
		t.Logf("[start-stack] backend=jaeger: 跳过 trace compose 启停 (依赖外部 jaeger 容器共享，例如 tutorial/docker-compose.yml)")
	} else {
		traceCompose := filepath.Join(root, "test", "docker", "trace", backend+".yaml")
		runComposeOrSkip(t, traceCompose, "up", "-d", "--wait")
		t.Cleanup(func() { runCompose(t, traceCompose, "down", "-v") })
	}

	// 2. driver 拓扑容器 —— single 跳过（共享 compose 已在 tutorial 端起好）
	if topology == TopologySingle {
		t.Logf("[start-stack] topology=single: 跳过 driver 容器启停 (driver=%s 由共享 tutorial compose 提供)", driver)
		return
	}
	driverCompose := filepath.Join(root, "test", "docker", driver, topology+".yaml")
	runComposeOrSkip(t, driverCompose, "up", "-d", "--wait")
	t.Cleanup(func() { runCompose(t, driverCompose, "down", "-v") })
}

func runComposeOrSkip(t *testing.T, file string, args ...string) {
	t.Helper()
	if _, err := os.Stat(file); err != nil {
		t.Skipf("compose file %s not found: %v", file, err)
	}
	runCompose(t, file, args...)
}

func runCompose(t *testing.T, file string, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", file}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v failed: %v\n%s", args, err, out)
	}
}

// NewTraceID 返回 16 字节随机 trace.TraceID。
// e2e 测试用它在 producer 端 StartSpan(..., trace.WithTraceID(want)) 注入已知 ID，
// 然后在 AssertSpanInBackend 里用同一 ID 去 backend 查找。
func NewTraceID(t *testing.T) trace.TraceID {
	t.Helper()
	var tid trace.TraceID
	if _, err := io.ReadFull(rand.Reader, tid[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return tid
}

// TraceIDHex 返回 16 字节 trace.TraceID 的 32 字符 hex 表示。
func TraceIDHex(tid trace.TraceID) string {
	return tid.String()
}

// AssertSpanInBackend 轮询 backend 直到找到匹配 want 的 span 或超时。
//
// backend 决定查询端点：
//   - jaeger:        GET http://localhost:16686/api/traces/<hex>
//   - redis_stream:  XRange trace:<driver>:<topology>
//   - kafka_topic:   起临时 consumer 拉 trace-spans-<driver> topic
func AssertSpanInBackend(t *testing.T, ctx context.Context, backend, driver, topology string, want trace.TraceID) {
	AssertSpanInBackendWithTimeout(t, ctx, backend, driver, topology, want, defaultAssertTimeout)
}

// AssertSpanInBackendWithTimeout 同 AssertSpanInBackend，可自定义超时（用于慢启动 broker）。
func AssertSpanInBackendWithTimeout(t *testing.T, ctx context.Context, backend, driver, topology string, want trace.TraceID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		found, err := queryBackend(ctx, backend, driver, topology, want)
		if err != nil {
			lastErr = err
		} else if found {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("[assert] trace_id=%s not found in backend=%s driver=%s topology=%s within %s (last_err=%v)",
		want, backend, driver, topology, timeout, lastErr)
}

func queryBackend(ctx context.Context, backend, driver, topology string, want trace.TraceID) (bool, error) {
	switch backend {
	case BackendJaeger:
		return queryJaeger(ctx, want)
	case BackendRedisStream:
		return queryRedisStream(ctx, driver, topology, want)
	case BackendKafkaTopic:
		return queryKafkaTopic(ctx, driver, topology, want)
	}
	return false, fmt.Errorf("unknown backend: %s", backend)
}

func queryJaeger(ctx context.Context, want trace.TraceID) (bool, error) {
	url := fmt.Sprintf("http://localhost:16686/api/traces/%s", want.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// 连不上 jaeger 不算 hard error，让上层轮询
		return false, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	var body struct {
		Data []struct {
			TraceID string `json:"traceID"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false, fmt.Errorf("decode jaeger response: %w", err)
	}
	for _, d := range body.Data {
		if d.TraceID == want.String() {
			return true, nil
		}
	}
	return false, nil
}

func queryRedisStream(ctx context.Context, driver, topology string, want trace.TraceID) (bool, error) {
	stream := fmt.Sprintf("trace:%s:%s", driver, topology)
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()
	res, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return false, nil
	}
	for _, msg := range res {
		payload, ok := msg.Values["span"].(string)
		if !ok {
			continue
		}
		var rec struct {
			TraceID string `json:"trace_id"`
		}
		if err := json.Unmarshal([]byte(payload), &rec); err != nil {
			continue
		}
		if rec.TraceID == want.String() {
			return true, nil
		}
	}
	return false, nil
}

func queryKafkaTopic(ctx context.Context, driver, topology string, want trace.TraceID) (bool, error) {
	topic := fmt.Sprintf("trace-spans-%s", driver)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:19092",
		"group.id":          fmt.Sprintf("assert-trace-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return false, nil
	}
	defer func() { _ = consumer.Close() }()
	if err := consumer.Subscribe(topic, nil); err != nil {
		return false, nil
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for readCtx.Err() == nil {
		msg, err := consumer.ReadMessage(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var rec struct {
			TraceID string `json:"trace_id"`
		}
		if err := json.Unmarshal(msg.Value, &rec); err != nil {
			continue
		}
		if rec.TraceID == want.String() {
			return true, nil
		}
	}
	return false, nil
}

// SpanRecord 是 trace backend span 的统一抽象。
// jaeger backend 字段全填；redis_stream / kafka_topic 自定义 exporter
// 不写 Kind / ParentSpanID / Status，因此这两个 backend 下这三个字段
// 必为空字符串。调用方按 backend 决定是否校验这些字段。
type SpanRecord struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	Kind         string
	Attributes   map[string]string
	StartTime    time.Time
	Status       string
}

// SpanExpect 描述 AssertSpanFields 期望匹配的 span 字段。
// Kind 为空字符串 → 跳过 Kind 校验（redis/kafka backend 必传 ""）。
// Attributes 走子集匹配：期望 attrs 必须出现且值相等，多余 attrs 忽略。
type SpanExpect struct {
	Name       string
	Kind       string // "" = skip
	Attributes map[string]string
}

// AssertSpanFields 断言 spans 中至少有一个 SpanRecord 满足 want 的 name+attributes。
// want.Kind 为空时跳过 Kind 校验（redis/kafka backend 必传空）。
func AssertSpanFields(t *testing.T, spans []SpanRecord, want SpanExpect) {
	t.Helper()
	for _, s := range spans {
		if s.Name != want.Name {
			continue
		}
		if want.Kind != "" && s.Kind != want.Kind {
			continue
		}
		matched := true
		for k, v := range want.Attributes {
			if s.Attributes[k] != v {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	t.Fatalf("AssertSpanFields: no span matches name=%q attrs=%v in %d fetched spans",
		want.Name, want.Attributes, len(spans))
}

// AssertTraceContext 严格断言：spans 中存在 consume 端 span，其 ParentSpanID
// 等于 wantProducerSpanID。仅 jaeger backend 可用（其他 backend 不写 ParentSpanID）。
func AssertTraceContext(t *testing.T, spans []SpanRecord, traceID, wantProducerSpanID string) {
	t.Helper()
	for _, s := range spans {
		if s.ParentSpanID == wantProducerSpanID {
			return
		}
	}
	t.Fatalf("AssertTraceContext: no span has ParentSpanID=%q in %d fetched spans (traceID=%s)",
		wantProducerSpanID, len(spans), traceID)
}

// AssertTraceContextLoose 降级断言：spans 中至少有一条 TraceID 等于 want。
// 用于 redis/kafka backend（自定义 exporter 不写 ParentSpanID）。
func AssertTraceContextLoose(t *testing.T, spans []SpanRecord, traceID string) {
	t.Helper()
	for _, s := range spans {
		if s.TraceID == traceID {
			return
		}
	}
	t.Fatalf("AssertTraceContextLoose: no span has TraceID=%q in %d fetched spans",
		traceID, len(spans))
}
