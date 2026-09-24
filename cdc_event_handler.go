package cdccassandra

import "github.com/Trendyol/go-mongo-cdc-cassandra/cassandra"

type CdcEventHandler struct {
	bulk *cassandra.Bulk
}

func (h *CdcEventHandler) BeforePartitionStop(partitionID int) {
	h.bulk.PreparePartitionRebalancing(partitionID)
}

func (h *CdcEventHandler) AfterPartitionStop(partitionID int) {
	h.bulk.FlushMessages()
	h.bulk.EndPartitionRebalancing(partitionID)
}

func (h *CdcEventHandler) BeforePartitionStart(_ int) {}

func (h *CdcEventHandler) AfterPartitionStart(_ int) {}
