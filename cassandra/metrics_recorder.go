package cassandra

// MetricsRecorder records Cassandra connector metrics.
type MetricsRecorder interface {
	RecordActionSuccess(tableName string, operationType string)
	RecordActionError(tableName string, operationType string)
	RecordProcessLatency(latencyMs int64)
	RecordBatchProcessLatency(latencyMs int64)
}
