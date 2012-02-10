package mry

import (
	"time"
)

// TODO: in memory transaction shim 

type Storage interface {
	Init()
	SyncModel(model *Model) error
	//CreateTable(table string, depth int) error
	//CreateIndex(table, column string)
	GetTransaction(trxTime time.Time) (StorageTransaction, error)
	Nuke() error
}

type StorageTransaction interface {
	Set(table string, keys []string, data []byte) error
	Get(table string, keys []string) (*Row, error)
	GetQuery(query StorageQuery) (RowIterator, error)
	GetTimeline(table string, nbKey int, from time.Time, count int) ([]RowMutation, error)
	Rollback() error
	Commit() error
}

type StorageQuery struct {
	Table       string
	TablePrefix []string
	Limit       int
}

type Row struct {
	IntTimestamp int64
	Timestamp    time.Time
	Key1         string
	Key2         string
	Key3         string
	Key4         string
	Data         []byte
}

func (r *Row) ConvertTimestamp() {
	r.Timestamp = time.Unix(0, r.IntTimestamp)
}

func (r *Row) Reset() {
	r.IntTimestamp = 0
	r.Timestamp = time.Unix(0, 0)
	r.Key1 = ""
	r.Key2 = ""
	r.Key3 = ""
	r.Key4 = ""
	r.Data = nil
}

type RowIterator interface {
	Next() (*Row, error)
	Close()
}

type RowMutation struct {
	OldRow      *Row
	NewRow      *Row
	LastVersion bool
}
