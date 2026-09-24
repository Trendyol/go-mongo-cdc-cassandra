package metric

import (
	"github.com/Trendyol/go-mongo-cdc-cassandra/cassandra"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const Name = "go_mongo_cdc_cassandra"

var (
	actionCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(Name, "cassandra_connector_action_total", "current"),
			Help: "Cassandra connector action counter",
		},
		[]string{"action_type", "result", "table_name"},
	)

	processLatencyGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(Name, "cassandra_connector_latency_ms", "current"),
			Help: "Cassandra connector latency ms",
		},
	)

	batchProcessLatencyGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(Name, "cassandra_connector_batch_process_latency_ms", "current"),
			Help: "Cassandra connector batch process latency ms",
		},
	)
)

type PrometheusMetricsRecorder struct{}

func NewMetricsRecorder() cassandra.MetricsRecorder {
	return &PrometheusMetricsRecorder{}
}

func (m *PrometheusMetricsRecorder) RecordActionSuccess(tableName, operationType string) {
	actionCounter.WithLabelValues(operationType, "success", tableName).Inc()
}

func (m *PrometheusMetricsRecorder) RecordActionError(tableName, operationType string) {
	actionCounter.WithLabelValues(operationType, "error", tableName).Inc()
}

func (m *PrometheusMetricsRecorder) RecordProcessLatency(latencyMs int64) {
	processLatencyGauge.Set(float64(latencyMs))
}

func (m *PrometheusMetricsRecorder) RecordBatchProcessLatency(latencyMs int64) {
	batchProcessLatencyGauge.Set(float64(latencyMs))
}
