package cassandra

import (
	"crypto/tls"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/apache/cassandra-gocql-driver/v2/snappy"

	"github.com/Trendyol/go-mongo-cdc-cassandra/config"
)

// NewSession creates a Cassandra session from the connector config.
//
//nolint:funlen
func NewSession(cfg config.Cassandra) (Session, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Keyspace = cfg.Keyspace

	cluster.Consistency = resolveConsistency(cfg.Consistency)

	if cfg.SerialConsistency != "" {
		switch cfg.SerialConsistency {
		case "SERIAL":
			cluster.SerialConsistency = gocql.Serial
		case "LOCAL_SERIAL":
			cluster.SerialConsistency = gocql.LocalSerial
		default:
			cluster.SerialConsistency = gocql.Serial
		}
	}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cfg.Username,
		Password: cfg.Password,
	}

	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	if cfg.KeepAlive > 0 {
		cluster.SocketKeepalive = cfg.KeepAlive
	}
	if cfg.NumConns > 0 {
		cluster.NumConns = cfg.NumConns
	}
	if cfg.MaxPreparedStmts > 0 {
		cluster.MaxPreparedStmts = cfg.MaxPreparedStmts
	}
	if cfg.MaxRoutingKeyInfo > 0 {
		cluster.MaxRoutingKeyInfo = cfg.MaxRoutingKeyInfo
	}
	if cfg.PageSize > 0 {
		cluster.PageSize = cfg.PageSize
	}

	if cfg.RetryPolicy.NumRetries > 0 {
		cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
			NumRetries: cfg.RetryPolicy.NumRetries,
			Min:        cfg.RetryPolicy.MinRetryDelay,
			Max:        cfg.RetryPolicy.MaxRetryDelay,
		}
	}

	switch cfg.Compressor {
	case "snappy":
		cluster.Compressor = &snappy.SnappyCompressor{}
	case "lz4":
		cluster.Compressor = &lz4.LZ4Compressor{}
	}

	if cfg.SSL.Enable {
		cluster.SslOpts = &gocql.SslOptions{
			Config: &tls.Config{
				InsecureSkipVerify: cfg.SSL.InsecureSkipVerify, //nolint:gosec // user-controlled config
			},
		}
	}

	switch cfg.HostSelectionPolicy {
	case "round_robin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	default:
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(), gocql.ShuffleReplicas(),
		)
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return NewGocqlSessionAdapter(session), nil
}

func resolveConsistency(s string) gocql.Consistency {
	switch s {
	case "ANY":
		return gocql.Any
	case "ONE":
		return gocql.One
	case "TWO":
		return gocql.Two
	case "THREE":
		return gocql.Three
	case "ALL":
		return gocql.All
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum
	case "EACH_QUORUM":
		return gocql.EachQuorum
	case "LOCAL_ONE":
		return gocql.LocalOne
	default:
		return gocql.Quorum
	}
}
