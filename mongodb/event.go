package mongodb

import "time"

// Event represents a single MongoDB change event that is passed to the mapper.
type Event struct {
	EventTime      time.Time
	CollectionName string
	DocumentID     any
	Value          []byte
	FullDocument   map[string]any
	IsDeleted      bool
	IsMutated      bool
	PartitionID    int
}

func NewDeleteEvent(documentID any, collectionName string, eventTime time.Time, partitionID int) Event {
	return Event{
		DocumentID: documentID, IsDeleted: true,
		CollectionName: collectionName, EventTime: eventTime, PartitionID: partitionID,
	}
}

func NewMutateEvent(documentID any, value []byte, fullDocument map[string]any, collectionName string, eventTime time.Time, partitionID int) Event {
	return Event{
		DocumentID: documentID, Value: value, FullDocument: fullDocument, IsMutated: true,
		CollectionName: collectionName, EventTime: eventTime, PartitionID: partitionID,
	}
}
