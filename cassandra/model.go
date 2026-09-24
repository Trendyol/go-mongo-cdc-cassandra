package cassandra

// OperationType is the CQL operation to execute.
type OperationType string

const (
	Insert OperationType = "insert"
	Update OperationType = "update"
	Delete OperationType = "delete"
	Upsert OperationType = "upsert" // INSERT (Cassandra INSERT is an upsert by default)
)

// Action describes a single change to be applied to Cassandra.
type Action struct {
	// Table is the target Cassandra table (without keyspace prefix).
	Table string

	// Operation is the CQL operation type.
	Operation OperationType

	// Document holds column→value pairs for Insert/Upsert/Update.
	Document map[string]any

	// Filter holds column→value pairs for the WHERE clause of Update/Delete.
	Filter map[string]any

	// RawQuery is an arbitrary CQL statement. When set, Document/Filter/Table
	// are ignored and the query is executed verbatim.
	RawQuery string

	// RawArgs are the bound arguments for RawQuery.
	RawArgs []any
}

// NewUpsertAction creates an INSERT (upsert) action.
func NewUpsertAction(table string, document map[string]any) Action {
	return Action{Table: table, Operation: Upsert, Document: document}
}

// NewInsertAction creates an INSERT action.
func NewInsertAction(table string, document map[string]any) Action {
	return Action{Table: table, Operation: Insert, Document: document}
}

// NewUpdateAction creates an UPDATE action.
func NewUpdateAction(table string, document, filter map[string]any) Action {
	return Action{Table: table, Operation: Update, Document: document, Filter: filter}
}

// NewDeleteAction creates a DELETE action.
func NewDeleteAction(table string, filter map[string]any) Action {
	return Action{Table: table, Operation: Delete, Filter: filter}
}

// NewRawAction creates an action that executes an arbitrary CQL statement.
func NewRawAction(query string, args ...any) Action {
	return Action{RawQuery: query, RawArgs: args}
}
