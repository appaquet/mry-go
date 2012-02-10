package mry

import (
	"github.com/appaquet/job"
	"github.com/appaquet/nrv"
	"time"
)

type TimelineJobFeeder struct {
	Storage Storage
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
			trx, err := f.Storage.GetTransaction(time.Now())
			if err != nil {
				nrv.Log.Fatal("Got an error getting transaction in job feeder: %s", err)
			}

			// TODO: table depth - nb keys!
			mutations, err := trx.GetTimeline(context.Config["table"].(string), 2, fromTime, 1000)
			if err != nil {
				nrv.Log.Fatal("Got an error getting timeline in job feeder: %s", err)
			}

			for _, mutation := range mutations {
				context.DataChan <- nrv.Map{
					"new": mutation.NewRow, // TODO: Serialize !!
					"old": mutation.OldRow,
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
