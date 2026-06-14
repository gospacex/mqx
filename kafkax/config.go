package kafkax

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gospacex/mqx"
	"gopkg.in/yaml.v3"
)

// activeConfigCache 保存基于 path 的最新解析快照，供热更新机制使用
var activeConfigCache sync.Map // key: path, val: *mqx.Config

// ParseFile 从 YAML 文件读取配置。若有热更新快照则直接返回内存快照，避免频繁 IO
func ParseFile(path string) (*mqx.Config, string, error) {
	if val, ok := activeConfigCache.Load(path); ok {
		cfg := val.(*mqx.Config)
		_, configKey := splitPath(path)
		if configKey == "" { configKey = "default" }
		return cfg, configKey, nil
	}

	return parseFileFromDisk(path)
}

func parseFileFromDisk(path string) (*mqx.Config, string, error) {
	filePath, configKey := splitPath(path)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("kafkax: read config file: %w", err)
	}

	var all map[string]*mqx.Config
	if err := yaml.Unmarshal(data, &all); err != nil {
		var single mqx.Config
		if err2 := yaml.Unmarshal(data, &single); err2 != nil {
			return nil, "", fmt.Errorf("kafkax: parse yaml: %w", err)
		}
		if configKey == "" { configKey = "default" }
		
		// 加入热更新快照池
		activeConfigCache.Store(path, &single)
		return &single, configKey, nil
	}

	if configKey == "" {
		for k, v := range all {
			activeConfigCache.Store(path, v)
			return v, k, nil
		}
		return nil, "", fmt.Errorf("kafkax: empty config file")
	}

	cfg, ok := all[configKey]
	if !ok {
		return nil, "", fmt.Errorf("kafkax: config key %q not found", configKey)
	}
	
	activeConfigCache.Store(path, cfg)
	return cfg, configKey, nil
}

func splitPath(path string) (string, string) {
	if i := strings.LastIndex(path, "#"); i > 0 {
		return path[:i], path[i+1:]
	}
	return path, ""
}

func buildProducerConfigMap(cfg *mqx.Config) (*kafka.ConfigMap, error) {
	if cfg.Kafka == nil { cfg.Kafka = &mqx.KafkaConfig{} }
	kc := cfg.Kafka

	if cfg.Producer.Acks == "" { cfg.Producer.Acks = "all" }
	if !cfg.Producer.Idempotent { cfg.Producer.Idempotent = true }

	cm := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(cfg.Addrs, ","),
		"acks":               cfg.Producer.Acks,
		"enable.idempotence": cfg.Producer.Idempotent,
	}

	// 【重磅功能】开启底层原生 Statistics 上报引擎
	if cfg.Metrics.Enabled {
		_ = cm.SetKey("statistics.interval.ms", 5000) // 每 5 秒底层上报一次原生 JSON Stats
	}

	if cfg.Retry.MaxRetries > 0 { _ = cm.SetKey("retries", cfg.Retry.MaxRetries) }
	if cfg.Retry.InitBackoff > 0 { _ = cm.SetKey("retry.backoff.ms", int(cfg.Retry.InitBackoff.Milliseconds())) }
	if kc.MaxInFlightPerConn > 0 { _ = cm.SetKey("max.in.flight.requests.per.connection", kc.MaxInFlightPerConn) }
	if cfg.Producer.BatchSize > 0 { _ = cm.SetKey("batch.size", cfg.Producer.BatchSize) }
	if cfg.Producer.LingerMs > 0 { _ = cm.SetKey("linger.ms", cfg.Producer.LingerMs) }
	if cfg.Producer.Timeout > 0 { _ = cm.SetKey("message.timeout.ms", int(cfg.Producer.Timeout.Milliseconds())) }
	if cfg.Producer.Compression != "" { _ = cm.SetKey("compression.type", cfg.Producer.Compression) }

	applySecurity(cm, cfg, kc)
	return cm, nil
}

func buildConsumerConfigMap(cfg *mqx.Config) (*kafka.ConfigMap, error) {
	if cfg.Kafka == nil { cfg.Kafka = &mqx.KafkaConfig{} }
	kc := cfg.Kafka

	cm := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Addrs, ","),
		"group.id":          cfg.Consumer.Group,
		"enable.auto.commit": cfg.Consumer.AutoCommit,
	}

	_ = cm.SetKey("auto.offset.reset", func() string {
		if cfg.Consumer.StartOffset != "" {
			return cfg.Consumer.StartOffset
		}
		return "earliest"
	}())
	if cfg.Consumer.SessionTimeout > 0 {
		_ = cm.SetKey("session.timeout.ms", int(cfg.Consumer.SessionTimeout.Milliseconds()))
	}
	if cfg.Consumer.MaxPollCount > 0 {
		_ = cm.SetKey("max.poll.records", cfg.Consumer.MaxPollCount)
	}
	if kc.FetchMinBytes > 0 { _ = cm.SetKey("fetch.min.bytes", kc.FetchMinBytes) }
	if kc.FetchMaxBytes > 0 { _ = cm.SetKey("fetch.max.bytes", kc.FetchMaxBytes) }

	applySecurity(cm, cfg, kc)
	return cm, nil
}

func applySecurity(cm *kafka.ConfigMap, cfg *mqx.Config, kc *mqx.KafkaConfig) {
	if kc.SecurityProtocol != "" { _ = cm.SetKey("security.protocol", kc.SecurityProtocol) }
	if cfg.Auth.Username != "" {
		mech := kc.SASLMechanism
		if mech == "" { mech = "PLAIN" }
		_ = cm.SetKey("sasl.mechanisms", mech)
		_ = cm.SetKey("sasl.username", cfg.Auth.Username)
		_ = cm.SetKey("sasl.password", cfg.Auth.Password)
	}
}
