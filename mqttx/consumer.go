package mqttx

import (
	"fmt"
	"github.com/gospacex/mqx"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// =====================================================================
// 消费者 (Single / Cluster) -> 返回 mqtt.Client
// =====================================================================

func C(path string) (mqtt.Client, error) { return CPS(path) }

func CPS(path string) (mqtt.Client, error) {
	cfg, _, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("mqttx.CPS: %w", err)
	}
	cfg.Mode = "single"
	client, _, err := getOrCreateClient(cfg)
	return client, err
}

func COS(cfg mqx.Config) (mqtt.Client, error) {
	cfg.Mode = "single"
	client, _, err := getOrCreateClient(&cfg)
	return client, err
}

func MustC(path string) mqtt.Client { return MustCPS(path) }

func MustCPS(path string) mqtt.Client {
	c, err := CPS(path)
	if err != nil {
		panic(fmt.Errorf("mqttx MustCPS failure: %w", err))
	}
	return c
}

func MustCOS(cfg mqx.Config) mqtt.Client {
	c, err := COS(cfg)
	if err != nil {
		panic(fmt.Errorf("mqttx MustCOS failure: %w", err))
	}
	return c
}

func CPC(path string) (mqtt.Client, error) {
	cfg, _, err := ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("mqttx.CPC: %w", err)
	}
	cfg.Mode = "cluster"
	client, _, err := getOrCreateClient(cfg)
	return client, err
}

func COC(cfg mqx.Config) (mqtt.Client, error) {
	cfg.Mode = "cluster"
	client, _, err := getOrCreateClient(&cfg)
	return client, err
}

func MustCPC(path string) mqtt.Client {
	c, err := CPC(path)
	if err != nil {
		panic(fmt.Errorf("mqttx MustCPC failure: %w", err))
	}
	return c
}

func MustCOC(cfg mqx.Config) mqtt.Client {
	c, err := COC(cfg)
	if err != nil {
		panic(fmt.Errorf("mqttx MustCOC failure: %w", err))
	}
	return c
}
