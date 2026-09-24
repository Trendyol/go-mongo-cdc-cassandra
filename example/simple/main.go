package main

import (
	"context"

	cdccassandra "github.com/Trendyol/go-mongo-cdc-cassandra"
)

// The simple example uses the DefaultMapper, which writes every document into a
// "documents" table (id TEXT, data TEXT) as an upsert.
//
// Create the table before running:
//
//	CREATE TABLE mykeyspace.documents (
//	    id   TEXT PRIMARY KEY,
//	    data TEXT
//	);
func main() {
	connector, err := cdccassandra.NewConnectorBuilder("config.yml").Build()
	if err != nil {
		panic(err)
	}
	defer connector.Close()

	connector.Start(context.Background())
}
