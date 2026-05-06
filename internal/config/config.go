package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	SQS     SQSConfig     `yaml:"sqs"`
	DynamoDB DynamoDBConfig `yaml:"dynamodb"`
	Proxy   ProxyConfig   `yaml:"proxy"`
}

type SQSConfig struct {
	Standard StandardQueueConfig `yaml:"standard"`
	FIFO     FIFOQueueConfig     `yaml:"fifo"`
	DefaultQueueLimit int64    `yaml:"default_queue_limit"`
}

type StandardQueueConfig struct {
	RateLimit int64 `yaml:"rate_limit"`
	BurstLimit int64 `yaml:"burst_limit"`
}

type FIFOQueueConfig struct {
	TPSLimit         int64 `yaml:"tps_limit"`
	ReceiveTPSLimit  int64 `yaml:"receive_tps_limit"`
	DeleteTPSLimit   int64 `yaml:"delete_tps_limit"`
	BurstLimit       int64 `yaml:"burst_limit"`
	BatchTPSLimit    int64 `yaml:"batch_tps_limit"`
}

type DynamoDBConfig struct {
	OnDemand  OnDemandConfig  `yaml:"on_demand"`
	Provisioned ProvisionedConfig `yaml:"provisioned"`
	Default   DynamoDBDefaultConfig `yaml:"default"`
}

type OnDemandConfig struct {
	InitialReadRPS     int64 `yaml:"initial_read_rps"`
	InitialWriteRPS    int64 `yaml:"initial_write_rps"`
	AccountMaxReadRPS  int64 `yaml:"account_max_read_rps"`
	AccountMaxWriteRPS int64 `yaml:"account_max_write_rps"`
	MaxTableRPS        int64 `yaml:"max_table_rps"`
}

type ProvisionedConfig struct {
	PerTableMaxRCU int64 `yaml:"per_table_max_rcu"`
	PerTableMaxWCU int64 `yaml:"per_table_max_wcu"`
	AccountMaxRCU  int64 `yaml:"account_max_rcu"`
	AccountMaxWCU  int64 `yaml:"account_max_wcu"`
	MinCapacity    int64 `yaml:"min_capacity"`
}

type DynamoDBDefaultConfig struct {
	TableMaxRCU int64 `yaml:"table_max_rcu"`
	TableMaxWCU int64 `yaml:"table_max_wcu"`
}

type ProxyConfig struct {
	Host         string        `yaml:"host"`
	Port         int           `yaml:"port"`
	UpstreamURL  string        `yaml:"upstream_url"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

func (p ProxyConfig) Address() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	applyEnvOverrides(&cfg)

	return &cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("QUOTA_SQS_FIFO_TPS"); v != "" {
		if val := parseInt64(v); val > 0 {
			cfg.SQS.FIFO.TPSLimit = val
		}
	}
	if v := os.Getenv("QUOTA_SQS_FIFO_RECEIVE_TPS"); v != "" {
		if val := parseInt64(v); val > 0 {
			cfg.SQS.FIFO.ReceiveTPSLimit = val
		}
	}
	if v := os.Getenv("QUOTA_SQS_FIFO_DELETE_TPS"); v != "" {
		if val := parseInt64(v); val > 0 {
			cfg.SQS.FIFO.DeleteTPSLimit = val
		}
	}
	if v := os.Getenv("QUOTA_SQS_FIFO_BATCH_TPS"); v != "" {
		if val := parseInt64(v); val > 0 {
			cfg.SQS.FIFO.BatchTPSLimit = val
		}
	}
	if v := os.Getenv("QUOTA_DYNAMODB_ONDEMAND_MAX_RPS"); v != "" {
		if val := parseInt64(v); val > 0 {
			cfg.DynamoDB.OnDemand.MaxTableRPS = val
		}
	}
	if v := os.Getenv("PROXY_PORT"); v != "" {
		if val := parseInt(v); val > 0 {
			cfg.Proxy.Port = val
		}
	}
	if v := os.Getenv("UPSTREAM_URL"); v != "" {
		cfg.Proxy.UpstreamURL = v
	}
}

func parseInt64(s string) int64 {
	var n int64
	fmt.Sscanf(s, "%d", &n)
	return n
}

func parseInt(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}