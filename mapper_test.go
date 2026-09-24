package cdccassandra

import (
	"testing"

	"github.com/Trendyol/go-mongo-cdc-cassandra/cassandra"
	"github.com/Trendyol/go-mongo-cdc-cassandra/mongodb"
)

func TestDefaultMapperMutate(t *testing.T) {
	event := mongodb.Event{
		IsMutated:  true,
		DocumentID: "abc",
		Value:      []byte(`{"name":"foo"}`),
	}

	actions := DefaultMapper(event)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Operation != cassandra.Upsert {
		t.Fatalf("expected upsert, got %s", actions[0].Operation)
	}
	if actions[0].Table != "documents" {
		t.Fatalf("expected table documents, got %s", actions[0].Table)
	}
	if actions[0].Document["id"] != "abc" {
		t.Fatalf("expected id abc, got %v", actions[0].Document["id"])
	}
}

func TestDefaultMapperDelete(t *testing.T) {
	event := mongodb.Event{
		IsDeleted:  true,
		DocumentID: "xyz",
	}

	actions := DefaultMapper(event)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Operation != cassandra.Delete {
		t.Fatalf("expected delete, got %s", actions[0].Operation)
	}
	if actions[0].Filter["id"] != "xyz" {
		t.Fatalf("expected filter id xyz, got %v", actions[0].Filter["id"])
	}
}

func TestDocumentIDToString(t *testing.T) {
	if got := documentIDToString("id1"); got != "id1" {
		t.Fatalf("expected id1, got %s", got)
	}
	if got := documentIDToString(42); got != "42" {
		t.Fatalf("expected 42, got %s", got)
	}
	if got := documentIDToString(int64(7)); got != "7" {
		t.Fatalf("expected 7, got %s", got)
	}
}
