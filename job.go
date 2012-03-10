package mry

import (
	pb "code.google.com/p/goprotobuf/proto"
	"github.com/appaquet/job"
	"github.com/appaquet/nrv"
	"time"
)

type TimelineJobFeeder struct {
	Db      *Db
	stopped bool
}

func (f *TimelineJobFeeder) Init(context *job.Context) {
	go func() {
		var fromTime time.Time = time.Unix(0, 0)
		if val, ok := context.Config["from_time"]; ok {
			switch val.(type) {
			case int64:
				fromTime = time.Unix(0, val.(int64))
			case float64:
				fromTime = time.Unix(0, int64(val.(float64)))
			}
		}

		for !f.stopped {

			// TODO: use token
			trx, err := f.Db.Storage.GetTransaction(nrv.Token(0), time.Now())
			if err != nil {
				nrv.Log.Fatal("Got an error getting transaction in job feeder: %s", err)
			}

			tableName := context.Config["table"].(string)
			table := f.Db.GetTable(tableName)
			if table == nil {
				nrv.Log.Fatal("Table %s doesn't exist", tableName)
				return
			}

			mutations, err := trx.GetTimeline(table, fromTime, 1000)
			if err != nil {
				nrv.Log.Fatal("Got an error getting timeline in job feeder: %s", err)
				return
			}


			for _, mutation := range mutations {
				srMutation := &JobRowMutation{}

				newValue := &TransactionValue{}
				pb.Unmarshal(mutation.NewRow.Data, newValue)
				srMutation.New = &JobRow{
					Timestamp: pb.Uint64(uint64(mutation.NewRow.IntTimestamp)),
					Key1: pb.String(mutation.NewRow.Key1),
					Key2: pb.String(mutation.NewRow.Key2),
					Key3: pb.String(mutation.NewRow.Key3),
					Key4: pb.String(mutation.NewRow.Key4),
					Data: newValue,
				}

				oldValue := &TransactionValue{}
				if mutation.OldRow.Key1 != "" {
					pb.Unmarshal(mutation.OldRow.Data, oldValue)
					srMutation.Old = &JobRow{
						Timestamp: pb.Uint64(uint64(mutation.OldRow.IntTimestamp)),
						Key1: pb.String(mutation.OldRow.Key1),
						Key2: pb.String(mutation.OldRow.Key2),
						Key3: pb.String(mutation.OldRow.Key3),
						Key4: pb.String(mutation.OldRow.Key4),
						Data: oldValue,
					}
				}

				context.DataChan <- nrv.Map{
					"mutation": srMutation,
				}

				fromTime = mutation.NewRow.Timestamp.Add(1)
			}

			trx.Commit()

			// no row, we wait
			// TODO: should bind to channel for realtime updates
			if len(mutations) == 0 {
				time.Sleep(1000000000)
			}
		}
	}()
}

func (f *TimelineJobFeeder) Stop() {
	f.stopped = true
}
