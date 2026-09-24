# Go Mongo CDC Cassandra

MongoDB Change Data Capture (CDC) connector for Apache Cassandra. This library
provides real-time data synchronization from MongoDB to Cassandra using MongoDB
Change Streams.

It builds on top of [`go-mongo-cdc`](https://github.com/Trendyol/go-mongo-cdc)
for MongoDB CDC and adds a configurable Cassandra sink layer powered by
[gocql](https://github.com/apache/cassandra-gocql-driver).

## Features

- **Near real-time synchronization** from MongoDB to Cassandra using MongoDB Change Streams.
- **Custom mapping** via pluggable mapper functions that can emit one or many Cassandra actions per change event.
- **Batch processing controls** such as maximum batch size and batch ticker durations for efficient writes.
- **Insert, Update, Delete, Upsert** operations — Cassandra INSERT is an upsert by default.
- **Raw CQL escape hatch**: emit arbitrary CQL statements from a mapper through the same batching and checkpoint pipeline.
- **Batch-internal dedup**: newer changes to the same row within a batch overwrite older ones, reducing write amplification.
- **Configurable consistency levels** (ANY, ONE, QUORUM, ALL, LOCAL_QUORUM, etc.).
- **Compression** support (Snappy, LZ4).
- **SSL/TLS** and **authentication** support.
- **Exponential backoff retry** policy.
- **Prometheus metrics** exposed on the same endpoint as the underlying CDC layer.
- **Easily manageable configurations** via YAML files or Go structs.

## Installation

```
go get github.com/Trendyol/go-mongo-cdc-cassandra
```

## Quick Start

### Basic Usage (Default Mapper)

The default mapper writes each document into a `documents` table
(`id TEXT PRIMARY KEY`, `data TEXT`). Create the table first:

```cql
CREATE TABLE mykeyspace.documents (
    id   TEXT PRIMARY KEY,
    data TEXT
);
```

```go
package main

import (
    "context"

    cdccassandra "github.com/Trendyol/go-mongo-cdc-cassandra"
)

func main() {
    connector, err := cdccassandra.NewConnectorBuilder("config.yml").Build()
    if err != nil {
        panic(err)
    }
    defer connector.Close()

    connector.Start(context.Background())
}
```

### Custom Mapper

```go
func mapper(event mongodb.Event) []cassandra.Action {
    id := event.DocumentID.(string)
    if event.IsMutated {
        var u User
        _ = jsoniter.Unmarshal(event.Value, &u)
        return []cassandra.Action{
            cassandra.NewUpsertAction("users", map[string]any{
                "id": id, "name": u.Name, "email": u.Email,
            }),
        }
    }
    return []cassandra.Action{
        cassandra.NewDeleteAction("users", map[string]any{"id": id}),
    }
}

connector, err := cdccassandra.NewConnectorBuilder(cfg).
    SetMapper(mapper).
    Build()
```

### Raw CQL Actions

```go
cassandra.NewRawAction("UPDATE users SET login_count = login_count + 1 WHERE id = ?", id)
```

## Configuration

### Example `config.yml`

```yaml
cassandra:
  hosts:
    - "localhost:9042"
  keyspace: "mykeyspace"
  consistency: "QUORUM"
  batchSizeLimit: 1000
  batchTickerDuration: 10s
  numConns: 2

mongodb:
  connection:
    uri: "localhost:27017"
    database: "exampleDB"
    collection: "exampleCollection"

metric:
  port: 8080

checkpoint:
  tokenSaveInterval: 10s
  changeStreamSaveCount: 500

partition:
  totalPartition: 1
  consumerGroup: "cassandra"

logger:
  logLevel: "info"

gracefulShutdownTimeout: 5s
```

### Cassandra Configuration

| Variable                          | Type          | Required | Default      | Description                                                    |
|-----------------------------------|---------------|----------|--------------|----------------------------------------------------------------|
| `cassandra.hosts`                 | []string      | yes      | -            | Cassandra node addresses.                                       |
| `cassandra.keyspace`              | string        | yes      | -            | Target keyspace.                                                |
| `cassandra.username`              | string        | no       | -            | Authentication username.                                        |
| `cassandra.password`              | string        | no       | -            | Authentication password.                                        |
| `cassandra.consistency`           | string        | no       | QUORUM       | Consistency level.                                              |
| `cassandra.serialConsistency`     | string        | no       | -            | Serial consistency (SERIAL / LOCAL_SERIAL).                     |
| `cassandra.compressor`            | string        | no       | -            | Compression: `snappy` or `lz4`.                                 |
| `cassandra.hostSelectionPolicy`   | string        | no       | token_aware  | Host selection: `token_aware` or `round_robin`.                 |
| `cassandra.numConns`              | int           | no       | 2            | Connections per host.                                           |
| `cassandra.connectTimeout`        | time.Duration | no       | 5s           | Connection timeout.                                             |
| `cassandra.timeout`               | time.Duration | no       | 5s           | Query timeout.                                                  |
| `cassandra.keepAlive`             | time.Duration | no       | 30s          | Socket keep-alive interval.                                     |
| `cassandra.maxPreparedStmts`      | int           | no       | 1000         | Max prepared statements cache size.                             |
| `cassandra.maxRoutingKeyInfo`     | int           | no       | -            | Max routing key info cache size.                                |
| `cassandra.pageSize`              | int           | no       | 5000         | Default page size for queries.                                  |
| `cassandra.batchSizeLimit`        | int           | no       | 1000         | Max actions per batch before flush.                             |
| `cassandra.batchTickerDuration`   | time.Duration | no       | 10s          | Auto-flush interval for long-waiting messages.                  |
| `cassandra.retryPolicy.numRetries`    | int       | no       | 3            | Max retries with exponential backoff.                           |
| `cassandra.retryPolicy.minRetryDelay` | time.Duration | no   | 100ms        | Minimum retry delay.                                            |
| `cassandra.retryPolicy.maxRetryDelay` | time.Duration | no   | 1s           | Maximum retry delay.                                            |
| `cassandra.ssl.enable`            | bool          | no       | false        | Enable TLS.                                                     |
| `cassandra.ssl.insecureSkipVerify`| bool          | no       | false        | Skip TLS certificate verification.                              |

### MongoDB / CDC Configuration

Check out [go-mongo-cdc](https://github.com/Trendyol/go-mongo-cdc#configuration).

> **Note:** The checkpoint `type` is forced to `manual` internally. Checkpoints
> are committed only after a batch is successfully written to Cassandra, ensuring
> at-least-once delivery.

## How It Works

1. `go-mongo-cdc` streams change events from MongoDB (with partitioning, bootstrap and checkpointing).
2. Each event is converted into a `mongodb.Event` and passed to the mapper.
3. The mapper returns `cassandra.Action`s (Insert/Upsert/Update/Delete or Raw CQL).
4. Actions are buffered; within a batch, newer changes to the same row overwrite older ones.
5. On flush, each action is executed as a CQL statement against Cassandra.
6. After a successful flush, the CDC checkpoint is committed.

## Exposed Metrics

| Metric Name | Type | Description | Labels |
|---|---|---|---|
| `go_mongo_cdc_cassandra_cassandra_connector_action_total_current` | Counter | Count of Cassandra actions | `action_type`, `result`, `table_name` |
| `go_mongo_cdc_cassandra_cassandra_connector_latency_ms_current` | Gauge | Time to add events to the batch (ms) | N/A |
| `go_mongo_cdc_cassandra_cassandra_connector_batch_process_latency_ms_current` | Gauge | Time to process a batch against Cassandra (ms) | N/A |

CDC-related metrics are also exposed by the underlying change data capture layer
on the same Prometheus endpoint.

## Examples

- [Simple Example](example/simple) - Default mapper writing to a generic `documents` table.
- [Struct Config](example/struct-config) - Programmatic config with a custom mapper.

## Contributing

Go Mongo CDC Cassandra is always open for direct contributions. For more information please check our [Contribution Guideline document](CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).
