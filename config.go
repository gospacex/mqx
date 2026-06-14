package mqx

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"time"
	"github.com/gospacex/mqx/config"
)

// Config 顶层统一配置
type Config struct {
	Driver string   `yaml:"driver"`
	Mode   string   `yaml:"mode"`
	Addrs  []string `yaml:"addrs"`

	// 【新增】实例连接池大小，默认为 1。配置 >1 时底层将开启 Round-Robin 轮询多路复用
	InstancePoolSize int `yaml:"instance_pool_size"`

	Auth     AuthConfig     `yaml:"auth"`
	TLS      TLSConfig      `yaml:"tls"`
	Producer ProducerConfig `yaml:"producer"`
	Consumer ConsumerConfig `yaml:"consumer"`
	Retry    RetryConfig    `yaml:"retry"`
	DLQ      DLQConfig      `yaml:"dlq"`
	Shutdown ShutdownConfig `yaml:"shutdown"`

	Trace   config.TracingConfig `yaml:"trace"`
	Metrics MetricsConfig        `yaml:"metrics"`

	Kafka    *KafkaConfig    `yaml:"kafka,omitempty"`
	RabbitMQ *RabbitMQConfig `yaml:"rabbitmq,omitempty"`
	RocketMQ *RocketMQConfig `yaml:"rocketmq,omitempty"`
	Pulsar   *PulsarConfig   `yaml:"pulsar,omitempty"`
	NATS     *NATSConfig     `yaml:"nats,omitempty"`
	Redis    *RedisConfig    `yaml:"redis,omitempty"`
	NSQ      *NSQConfig      `yaml:"nsq,omitempty"`
	MQTT     *MQTTConfig     `yaml:"mqtt,omitempty"`
}

type AuthConfig struct {
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Mechanism string `yaml:"mechanism"`
	Token     string `yaml:"token"`
}

type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	CAFile     string `yaml:"ca_file"`
	SkipVerify bool   `yaml:"skip_verify"`
}

func (t *TLSConfig) BuildTLS() (*tls.Config, error) {
	if !t.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: t.SkipVerify,
	}

	// 加载客户端证书（双向认证）
	if t.CertFile != "" && t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, TLSError("failed to load client certificate", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// 加载 CA 证书（验证服务器）
	if t.CAFile != "" {
		caCert, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, TLSError("failed to read CA file", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, TLSError("failed to parse CA certificate", nil)
		}
		tlsConfig.RootCAs = caPool
	}

	return tlsConfig, nil
}

type ProducerConfig struct {
	Topic       string        `yaml:"topic"`
	Acks        string        `yaml:"acks"`
	Idempotent  bool          `yaml:"idempotent"`
	BatchSize   int           `yaml:"batch_size"`
	LingerMs    int           `yaml:"linger_ms"`
	Compression string        `yaml:"compression"`
	Timeout     time.Duration `yaml:"timeout"`
	OrderedKey  bool          `yaml:"ordered_key"`
}

type ConsumerConfig struct {
	Topics         []string      `yaml:"topics"`
	Group          string        `yaml:"group"`
	AutoCommit     bool          `yaml:"auto_commit"`
	MaxPollCount   int           `yaml:"max_poll_count"`
	Concurrency    int           `yaml:"concurrency"`
	MaxInFlight    int           `yaml:"max_in_flight"`
	StartOffset    string        `yaml:"start_offset"`
	SessionTimeout time.Duration `yaml:"session_timeout"`
}

type RetryConfig struct {
	MaxRetries  int           `yaml:"max_retries"`
	InitBackoff time.Duration `yaml:"init_backoff"`
	MaxBackoff  time.Duration `yaml:"max_backoff"`
	Multiplier  float64       `yaml:"multiplier"`
}

type DLQConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Topic      string `yaml:"topic"`
	MaxRetries int    `yaml:"max_retries"`
}

type ShutdownConfig struct {
	Timeout time.Duration `yaml:"timeout"`
}

type MetricsConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Exporter string `yaml:"exporter"`
}

// ... 驱动专属配置略 (保持不变) ...
type KafkaConfig struct {
	SecurityProtocol   string `yaml:"security_protocol"`
	SASLMechanism      string `yaml:"sasl_mechanism"`
	PartitionStrategy  string `yaml:"partition_strategy"`
	MaxInFlightPerConn int    `yaml:"max_in_flight_per_conn"`
	FetchMinBytes      int    `yaml:"fetch_min_bytes"`
	FetchMaxBytes      int    `yaml:"fetch_max_bytes"`
	SchemaRegistry     string `yaml:"schema_registry"`
}
type RabbitMQConfig struct {
	VHost          string `yaml:"vhost"`
	Exchange      string `yaml:"exchange"`
	ExchangeType  string `yaml:"exchange_type"`
	Queue         string `yaml:"queue"`
	RoutingKey    string `yaml:"routing_key"`
	Durable       bool   `yaml:"durable"`
	AutoDelete    bool   `yaml:"auto_delete"`
	DLXExchange   string `yaml:"dlx_exchange"`
	DLXRoutingKey string `yaml:"dlx_routing_key"`
	Prefetch      int    `yaml:"prefetch"`
	Confirm       bool   `yaml:"confirm"`
}
type RocketMQConfig struct {
	NameServer        []string      `yaml:"name_server"`
	GroupName string        `yaml:"group_name"`
	InstanceName      string        `yaml:"instance_name"`
	Namespace string        `yaml:"namespace"`
	SendTimeout       time.Duration `yaml:"send_timeout"`
	RetryOnSendFail   int           `yaml:"retry_on_send_fail"`
	ConsumeFromWhere  string        `yaml:"consume_from_where"`
	ConsumeOrderly    bool          `yaml:"consume_orderly"`
	MaxReconsumeTimes int32 `yaml:"max_reconsume_times"`
	AccessKey         string        `yaml:"access_key"`
	SecretKey         string        `yaml:"secret_key"`
	TransactionEnable bool `yaml:"transaction_enable"`
}
type PulsarConfig struct {
	OperationTimeout    time.Duration `yaml:"operation_timeout"`
	ConnectionTimeout   time.Duration `yaml:"connection_timeout"`
	ReceiverQueueSize   int           `yaml:"receiver_queue_size"`
	AckTimeout          time.Duration `yaml:"ack_timeout"`
	NackRedeliveryDelay time.Duration `yaml:"nack_redelivery_delay"`
	SubscriptionType    string        `yaml:"subscription_type"`
	DeadLetterTopic     string        `yaml:"dead_letter_topic"`
	DeadLetterMaxRetry  uint32        `yaml:"dead_letter_max_retry"`
	MaxPendingMessages  int           `yaml:"max_pending_messages"`
	EnableBatching      bool          `yaml:"enable_batching"`
}
type NATSConfig struct {
	Name            string        `yaml:"name"`
	CredsFile       string        `yaml:"creds_file"`
	NKeyFile        string        `yaml:"nkey_file"`
	MaxReconnects   int           `yaml:"max_reconnects"`
	ReconnectWait   time.Duration `yaml:"reconnect_wait"`
	PingInterval    time.Duration `yaml:"ping_interval"`
	JetStream       bool          `yaml:"jetstream"`
	StreamName      string        `yaml:"stream_name"`
	StreamSubjects  []string      `yaml:"stream_subjects"`
	StreamStorage   string        `yaml:"stream_storage"`
	StreamReplicas  int           `yaml:"stream_replicas"`
}
type RedisConfig struct {
	DB           int `yaml:"db"`
	PoolSize     int `yaml:"pool_size"`
	MinIdleConns int `yaml:"min_idle_conns"`
	MaxLen int64 `yaml:"max_len"`
}
type NSQConfig struct {
	Channel           string        `yaml:"channel"`
	MaxInFlight       int           `yaml:"max_in_flight"`
	MaxAttempts       int           `yaml:"max_attempts"`
	MsgTimeout        time.Duration `yaml:"msg_timeout"`
	DialTimeout       time.Duration `yaml:"dial_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	OutputBufferSize  int64         `yaml:"output_buffer_size"`
	NsqdAddr          string        `yaml:"nsqd_addr"`
}
type MQTTConfig struct {
	ClientID             string        `yaml:"client_id"`
	CleanSession         bool          `yaml:"clean_session"`
	OrderMatters         bool          `yaml:"order_matters"`
	ResumeSubs           bool          `yaml:"resume_subs"`
	AutoReconnect        bool          `yaml:"auto_reconnect"`
	KeepAlive            time.Duration `yaml:"keep_alive"`
	PingTimeout          time.Duration `yaml:"ping_timeout"`
	ConnectTimeout       time.Duration `yaml:"connect_timeout"`
	MaxReconnectInterval time.Duration `yaml:"max_reconnect_interval"`
	WillEnabled          bool          `yaml:"will_enabled"`
	WillTopic            string        `yaml:"will_topic"`
	WillPayload          string        `yaml:"will_payload"`
	WillQoS              byte          `yaml:"will_qos"`
	WillRetained         bool          `yaml:"will_retained"`
}
