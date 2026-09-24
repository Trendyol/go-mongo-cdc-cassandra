package cassandra

import gocql "github.com/apache/cassandra-gocql-driver/v2"

// Session abstracts gocql.Session for testability.
type Session interface {
	Query(stmt string, values ...any) Query
	Close()
}

// Query abstracts gocql.Query.
type Query interface {
	Exec() error
}

// GocqlSessionAdapter wraps a real gocql.Session.
type GocqlSessionAdapter struct {
	*gocql.Session
}

func NewGocqlSessionAdapter(session *gocql.Session) *GocqlSessionAdapter {
	return &GocqlSessionAdapter{Session: session}
}

func (s *GocqlSessionAdapter) Query(stmt string, values ...any) Query {
	return &GocqlQueryAdapter{q: s.Session.Query(stmt, values...)}
}

type GocqlQueryAdapter struct {
	q *gocql.Query
}

func (q *GocqlQueryAdapter) Exec() error {
	return q.q.Exec()
}
