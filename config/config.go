package config

import (
	"strings"
	"time"

	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
)

type Cassandra struct {
	// Hosts is a list of Cassandra node addresses.
	Hosts []string `yaml:"hosts"`

	// Keyspace is the target Cassandra keyspace.
	Keyspace string `yaml:"keyspace"`

	// Username / Password for authentication.
	Username string `yaml:"username"`
	Password string `yaml:"password"`

	// Consistency level: ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, LOCAL_ONE.
	Consistency string `yaml:"consistency"`

	// SerialConsistency: SERIAL or LOCAL_SERIAL (for lightweight transactions).
	SerialConsistency string `yaml:"serialConsistency"`

	// Compressor: "snappy" or "lz4". Empty means no compression.
	Compressor string `yaml:"compressor"`

	// SSL configuration.
	SSL struct {
		Enable             bool `yaml:"enable"`
		InsecureSkipVerify bool `yaml:"insecureSkipVerify"`
	} `yaml:"ssl"`

	// RetryPolicy controls exponential backoff retries.
	RetryPolicy struct {
		NumRetries    int           `yaml:"numRetries"`
		MinRetryDelay time.Duration `yaml:"minRetryDelay"`
		MaxRetryDelay time.Duration `yaml:"maxRetryDelay"`
	} `yaml:"retryPolicy"`

	// HostSelectionPolicy: "token_aware" (default) or "round_robin".
	HostSelectionPolicy string `yaml:"hostSelectionPolicy"`

	// Connection settings.
	NumConns          int           `yaml:"numConns"`
	ConnectTimeout    time.Duration `yaml:"connectTimeout"`
	Timeout           time.Duration `yaml:"timeout"`
	KeepAlive         time.Duration `yaml:"keepAlive"`
	MaxPreparedStmts  int           `yaml:"maxPreparedStmts"`
	MaxRoutingKeyInfo int           `yaml:"maxRoutingKeyInfo"`
	PageSize          int           `yaml:"pageSize"`

	// Batch settings.
	BatchSizeLimit      int           `yaml:"batchSizeLimit"`
	BatchTickerDuration time.Duration `yaml:"batchTickerDuration"`
}

type Config struct {
	Cassandra Cassandra        `yaml:"cassandra"`
	CDC       cdcConfig.Config `yaml:",inline" mapstructure:",squash"`
}

func (c *Config) ApplyDefaults() {
	c.Cassandra.setDefaults()
}

func (cs *Cassandra) setDefaults() {
	cs.setConsistencyDefault()
	cs.setBatchDefaults()
	cs.setConnectionDefaults()
	cs.setRetryDefaults()
}

func (cs *Cassandra) setConsistencyDefault() {
	valid := map[string]bool{
		"ANY": true, "ONE": true, "TWO": true, "THREE": true,
		"QUORUM": true, "ALL": true, "LOCAL_QUORUM": true,
		"EACH_QUORUM": true, "LOCAL_ONE": true,
	}
	c := strings.TrimSpace(strings.ToUpper(cs.Consistency))
	if c == "" || !valid[c] {
		cs.Consistency = "QUORUM"
	} else {
		cs.Consistency = c
	}
}

func (cs *Cassandra) setBatchDefaults() {
	if cs.BatchSizeLimit <= 0 {
		cs.BatchSizeLimit = 1000
	}
	if cs.BatchTickerDuration <= 0 {
		cs.BatchTickerDuration = 10 * time.Second
	}
	hp := strings.TrimSpace(strings.ToLower(cs.HostSelectionPolicy))
	if hp != "round_robin" && hp != "token_aware" {
		cs.HostSelectionPolicy = "token_aware"
	} else {
		cs.HostSelectionPolicy = hp
	}
}

func (cs *Cassandra) setConnectionDefaults() {
	if cs.NumConns <= 0 {
		cs.NumConns = 2
	}
	if cs.ConnectTimeout <= 0 {
		cs.ConnectTimeout = 5 * time.Second
	}
	if cs.Timeout <= 0 {
		cs.Timeout = 5 * time.Second
	}
	if cs.KeepAlive <= 0 {
		cs.KeepAlive = 30 * time.Second
	}
	if cs.MaxPreparedStmts <= 0 {
		cs.MaxPreparedStmts = 1000
	}
	if cs.PageSize <= 0 {
		cs.PageSize = 5000
	}
}

func (cs *Cassandra) setRetryDefaults() {
	if cs.RetryPolicy.NumRetries <= 0 {
		cs.RetryPolicy.NumRetries = 3
	}
	if cs.RetryPolicy.MinRetryDelay <= 0 {
		cs.RetryPolicy.MinRetryDelay = 100 * time.Millisecond
	}
	if cs.RetryPolicy.MaxRetryDelay <= 0 {
		cs.RetryPolicy.MaxRetryDelay = 1 * time.Second
	}
}
