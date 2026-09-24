package main

import (
	"context"
	"time"

	jsoniter "github.com/json-iterator/go"

	cdccassandra "github.com/Trendyol/go-mongo-cdc-cassandra"
	"github.com/Trendyol/go-mongo-cdc-cassandra/cassandra"
	"github.com/Trendyol/go-mongo-cdc-cassandra/config"
	"github.com/Trendyol/go-mongo-cdc-cassandra/mongodb"

	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
)

type User struct {
	ID    string `json:"_id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func mapper(event mongodb.Event) []cassandra.Action {
	if event.IsMutated {
		var u User
		if err := jsoniter.Unmarshal(event.Value, &u); err != nil {
			return nil
		}
		u.ID = event.DocumentID.(string)
		return []cassandra.Action{
			cassandra.NewUpsertAction("users", map[string]any{
				"id":    u.ID,
				"name":  u.Name,
				"email": u.Email,
			}),
		}
	}

	return []cassandra.Action{
		cassandra.NewDeleteAction("users", map[string]any{
			"id": event.DocumentID.(string),
		}),
	}
}

func main() {
	cfg := config.Config{
		Cassandra: config.Cassandra{
			Hosts:               []string{"localhost:9042"},
			Keyspace:            "mykeyspace",
			Consistency:         "QUORUM",
			BatchSizeLimit:      1000,
			BatchTickerDuration: 10 * time.Second,
		},
		CDC: cdcConfig.Config{
			MongoDB: cdcConfig.MongoDB{
				Connection: cdcConfig.Connection{
					URI:        "localhost:27017",
					Database:   "exampleDB",
					Collection: "users",
				},
			},
			Checkpoint: cdcConfig.CheckpointConfig{
				TokenSaveInterval:       10 * time.Second,
				BootstrapQueryBatchSize: 5000,
				BootstrapSaveInterval:   10 * time.Second,
			},
			Partition: cdcConfig.PartitionConfig{
				TotalPartition: 1,
				ConsumerGroup:  "cassandra",
			},
			Logger: cdcConfig.LoggerConfig{LogLevel: "info"},
		},
	}

	connector, err := cdccassandra.NewConnectorBuilder(cfg).
		SetMapper(mapper).
		Build()
	if err != nil {
		panic(err)
	}
	defer connector.Close()

	connector.Start(context.Background())
}
