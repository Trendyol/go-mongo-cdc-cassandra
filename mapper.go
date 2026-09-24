package cdccassandra

import (
	"github.com/Trendyol/go-mongo-cdc-cassandra/cassandra"
	"github.com/Trendyol/go-mongo-cdc-cassandra/mongodb"
)

// Mapper converts a MongoDB change event into zero or more Cassandra actions.
type Mapper func(event mongodb.Event) []cassandra.Action

// DefaultMapper writes the full document as key + JSON value into a default
// table. Override it with SetMapper for production use.
func DefaultMapper(event mongodb.Event) []cassandra.Action {
	id := documentIDToString(event.DocumentID)

	if event.IsMutated {
		return []cassandra.Action{
			cassandra.NewUpsertAction("documents", map[string]any{
				"id":   id,
				"data": string(event.Value),
			}),
		}
	}

	return []cassandra.Action{
		cassandra.NewDeleteAction("documents", map[string]any{
			"id": id,
		}),
	}
}
