package cassandra

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-mongo-cdc-cassandra/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/stream"
)

type Bulk struct {
	session                      Session
	metricsRecorder              MetricsRecorder
	keyspace                     string
	cdcCheckpointCommit          func()
	cdcCheckpointCommitBootstrap func(partitionID int)
	batchTicker                  *time.Ticker
	rebalancingPartitions        map[int]bool
	batch                        []BatchItem
	batchKeys                    map[string]int
	rawKeyCounter                atomic.Uint64
	batchIndex                   int
	batchSize                    int
	batchSizeLimit               int
	batchTickerDuration          time.Duration
	flushLock                    sync.Mutex
}

type BatchItem struct {
	Action      *Action
	PartitionID int
	IsBootstrap bool
}

func NewBulk(
	cfg *config.Config,
	cdcCheckpointCommit func(),
	cdcCheckpointCommitBootstrap func(partitionID int),
	session Session,
	metricsRecorder MetricsRecorder,
) *Bulk {
	return &Bulk{
		session:                      session,
		metricsRecorder:              metricsRecorder,
		keyspace:                     cfg.Cassandra.Keyspace,
		cdcCheckpointCommit:          cdcCheckpointCommit,
		cdcCheckpointCommitBootstrap: cdcCheckpointCommitBootstrap,
		batchTicker:                  time.NewTicker(cfg.Cassandra.BatchTickerDuration),
		batchTickerDuration:          cfg.Cassandra.BatchTickerDuration,
		batchSizeLimit:               cfg.Cassandra.BatchSizeLimit,
		batchKeys:                    make(map[string]int, cfg.Cassandra.BatchSizeLimit),
		rebalancingPartitions:        make(map[int]bool),
	}
}

func (b *Bulk) StartBulk() {
	for range b.batchTicker.C {
		b.flushMessages()
	}
}

func (b *Bulk) AddActions(
	ctx *stream.ListenerContext,
	eventTime time.Time,
	actions []Action,
	isLastChunk bool,
	partitionID int,
	isBootstrap bool,
) {
	b.flushLock.Lock()
	if b.rebalancingPartitions[partitionID] {
		logger.Log.Debug("could not add new action to batch while partition is rebalancing - partitionId: %d", partitionID)
		b.flushLock.Unlock()
		return
	}

	for i := range actions {
		action := actions[i]
		key := b.getActionKey(action)

		if batchIndex, ok := b.batchKeys[key]; ok {
			b.batch[batchIndex] = BatchItem{
				Action: &actions[i], PartitionID: partitionID, IsBootstrap: isBootstrap,
			}
		} else {
			b.batch = append(b.batch, BatchItem{
				Action: &actions[i], PartitionID: partitionID, IsBootstrap: isBootstrap,
			})
			b.batchKeys[key] = b.batchIndex
			b.batchIndex++
			b.batchSize++
		}
	}

	if isLastChunk {
		ctx.Ack()
	}

	shouldFlush := b.batchSize >= b.batchSizeLimit
	b.flushLock.Unlock()

	if isLastChunk {
		b.metricsRecorder.RecordProcessLatency(time.Since(eventTime).Milliseconds())
	}
	if shouldFlush {
		b.flushMessages()
	}
}

func (b *Bulk) Close() {
	b.batchTicker.Stop()
	b.flushMessages()
}

func (b *Bulk) PreparePartitionRebalancing(partitionID int) {
	b.flushLock.Lock()
	b.rebalancingPartitions[partitionID] = true
	b.flushLock.Unlock()
}

func (b *Bulk) EndPartitionRebalancing(partitionID int) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	delete(b.rebalancingPartitions, partitionID)
}

func (b *Bulk) FlushMessages() {
	b.flushMessages()
}

func (b *Bulk) flushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if len(b.batch) > 0 {
		startedTime := time.Now()

		for _, item := range b.batch {
			if err := b.executeAction(item.Action); err != nil {
				logger.Log.Error("error while executing cassandra action, err: %v", err)
				panic(err)
			}
		}

		b.metricsRecorder.RecordBatchProcessLatency(time.Since(startedTime).Milliseconds())
		b.batchTicker.Reset(b.batchTickerDuration)

		bootstrapPartitions := make(map[int]bool)
		for _, item := range b.batch {
			if item.IsBootstrap {
				bootstrapPartitions[item.PartitionID] = true
			}
		}

		b.batch = b.batch[:0]
		b.batchKeys = make(map[string]int, b.batchSizeLimit)
		b.batchIndex = 0
		b.batchSize = 0

		b.cdcCheckpointCommit()
		for pid := range bootstrapPartitions {
			b.cdcCheckpointCommitBootstrap(pid)
		}
	}
}

func (b *Bulk) executeAction(action *Action) error {
	if action.RawQuery != "" {
		tableName := action.Table
		if tableName == "" {
			tableName = "(raw)"
		}
		err := b.session.Query(action.RawQuery, action.RawArgs...).Exec()
		if err == nil {
			b.metricsRecorder.RecordActionSuccess(tableName, "raw")
		} else {
			b.metricsRecorder.RecordActionError(tableName, "raw")
		}
		return err
	}

	var err error
	switch action.Operation {
	case Insert, Upsert:
		if len(action.Document) == 0 {
			logger.Log.Warn("skipping insert/upsert with empty document for table: %s", action.Table)
			return nil
		}
		err = b.insert(action)
	case Update:
		if len(action.Document) == 0 || len(action.Filter) == 0 {
			logger.Log.Warn("skipping update with empty document or filter for table: %s", action.Table)
			return nil
		}
		err = b.update(action)
	case Delete:
		if len(action.Filter) == 0 {
			logger.Log.Warn("skipping delete with empty filter for table: %s", action.Table)
			return nil
		}
		err = b.delete(action)
	default:
		err = fmt.Errorf("unsupported operation type: %s", action.Operation)
	}

	tableName := action.Table
	opName := string(action.Operation)
	if err == nil {
		b.metricsRecorder.RecordActionSuccess(tableName, opName)
	} else {
		b.metricsRecorder.RecordActionError(tableName, opName)
	}
	return err
}

func (b *Bulk) insert(action *Action) error {
	columns := sortedKeys(action.Document)
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		b.keyspace, action.Table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	values := make([]any, 0, len(columns))
	for _, col := range columns {
		values = append(values, action.Document[col])
	}

	return b.session.Query(query, values...).Exec()
}

func (b *Bulk) update(action *Action) error {
	docColumns := sortedKeys(action.Document)
	setParts := make([]string, len(docColumns))
	for i, k := range docColumns {
		setParts[i] = fmt.Sprintf("%s = ?", k)
	}

	filterColumns := sortedKeys(action.Filter)
	whereParts := make([]string, len(filterColumns))
	for i, k := range filterColumns {
		whereParts[i] = fmt.Sprintf("%s = ?", k)
	}

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		b.keyspace, action.Table,
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	)

	values := make([]any, 0, len(docColumns)+len(filterColumns))
	for _, col := range docColumns {
		values = append(values, action.Document[col])
	}
	for _, col := range filterColumns {
		values = append(values, action.Filter[col])
	}

	return b.session.Query(query, values...).Exec()
}

func (b *Bulk) delete(action *Action) error {
	filterColumns := sortedKeys(action.Filter)
	whereParts := make([]string, len(filterColumns))
	for i, k := range filterColumns {
		whereParts[i] = fmt.Sprintf("%s = ?", k)
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		b.keyspace, action.Table,
		strings.Join(whereParts, " AND "),
	)

	values := make([]any, 0, len(filterColumns))
	for _, col := range filterColumns {
		values = append(values, action.Filter[col])
	}

	return b.session.Query(query, values...).Exec()
}

func (b *Bulk) getActionKey(action Action) string {
	if action.RawQuery != "" {
		return fmt.Sprintf("raw:%d", b.rawKeyCounter.Add(1))
	}

	table := action.Table
	switch action.Operation {
	case Insert, Upsert:
		pk := primaryKeyFromDoc(action.Document)
		return fmt.Sprintf("upsert:%s:%s", table, pk)
	case Update:
		pk := primaryKeyFromFilter(action.Filter)
		return fmt.Sprintf("update:%s:%s", table, pk)
	case Delete:
		pk := primaryKeyFromFilter(action.Filter)
		return fmt.Sprintf("delete:%s:%s", table, pk)
	default:
		return fmt.Sprintf("raw:%d", b.rawKeyCounter.Add(1))
	}
}

func primaryKeyFromDoc(doc map[string]any) string {
	keys := sortedKeys(doc)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%v", doc[k]))
	}
	return strings.Join(parts, "|")
}

func primaryKeyFromFilter(filter map[string]any) string {
	return primaryKeyFromDoc(filter)
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
