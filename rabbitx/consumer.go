package rabbitx

import (
	"fmt"
	"log"
	"sync"

	"github.com/gospacex/mqx"
	"github.com/gospacex/mqx/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	consumerCache sync.Map // key → *amqp.Connection
	consumerLocks sync.Map // key → *sync.Mutex
)

// =====================================================================
// 单机消费者 (Single Consumer)
// =====================================================================

func C(path string) (*amqp.Connection, error) { return CPS(path) }

func CPS(path string) (*amqp.Connection, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("rabbitx.CPS: %w", err)
	}
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COS(cfg mqx.Config) (*amqp.Connection, error) {
	cfg.Mode = "single"
	cacheKey := "consumer:single:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustC(path string) *amqp.Connection { return MustCPS(path) }

func MustCPS(path string) *amqp.Connection {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("rabbitx MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) *amqp.Connection {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("rabbitx MustCOS failure: %w", err))
	}
	return c
}

// =====================================================================
// 集群消费者 (Cluster Consumer)
// =====================================================================

func CPC(path string) (*amqp.Connection, error) {
	cfg, key, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("rabbitx.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + key + ":" + utils.ConfigFingerprint(cfg)
	return getOrCreateConsumer(cacheKey, cfg)
}

func COC(cfg mqx.Config) (*amqp.Connection, error) {
	cfg.Mode = "cluster"
	cacheKey := "consumer:cluster:" + utils.ConfigFingerprint(&cfg)
	return getOrCreateConsumer(cacheKey, &cfg)
}

func MustCPC(path string) *amqp.Connection {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("rabbitx MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) *amqp.Connection {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("rabbitx MustCOC failure: %w", err))
	}
	return c
}

// =====================================================================
// 核心逻辑 (Double-Checked Locking)
// =====================================================================

func getOrCreateConsumer(cacheKey string, cfg *mqx.Config) (*amqp.Connection, error) {
	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(*amqp.Connection), nil
	}

	if cfg.Driver != "" && cfg.Driver != "rabbitmq" {
		return nil, fmt.Errorf("rabbitx: driver mismatch, expected rabbitmq but got %s", cfg.Driver)
	}

	lockVal, _ := consumerLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mu := lockVal.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	if val, ok := consumerCache.Load(cacheKey); ok {
		return val.(*amqp.Connection), nil
	}

	uri, err := BuildAMQPURI(cfg)
	if err != nil {
		return nil, fmt.Errorf("build amqp uri: %w", err)
	}

	tlsConfig, err := cfg.TLS.BuildTLS()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}

	var conn *amqp.Connection
	if tlsConfig != nil {
		conn, err = amqp.DialTLS(uri, tlsConfig)
	} else {
		conn, err = amqp.Dial(uri)
	}

	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	// 初始化消费侧拓扑 (与生产侧相同，确保队列和死信的创建一致性)
	ch, err := conn.Channel()
	if err == nil {
		if setupErr := SetupTopology(ch, cfg); setupErr != nil {
			log.Printf("[rabbitx] topology setup warning: %v", setupErr)
		}
		_ = ch.Close()
	}

	// 挂载连接关闭监控
	go func() {
		errChan := conn.NotifyClose(make(chan *amqp.Error))
		if e := <-errChan; e != nil {
			log.Printf("[rabbitx] consumer connection closed unexpectedly [key=%s]: %v", cacheKey, e)
			consumerCache.Delete(cacheKey)
		}
	}()

	consumerCache.Store(cacheKey, conn)
	log.Printf("[rabbitx] consumer connection ready [key=%s]", cacheKey)

	return conn, nil
}
