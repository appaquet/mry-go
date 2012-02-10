package mry

import (
	pb "code.google.com/p/goprotobuf/proto"
	"github.com/appaquet/nrv"
	"time"
)

//
// Database object bound to a specific domain of a given cluster
//
type Db struct {
	ServiceName string
	Cluster     nrv.Cluster
	Storage     Storage
	Service     *nrv.Service

	*Model
}

// TODO: rollback on panic
func (db *Db) SetupCluster() {
	db.Model = newModel()

	db.Service = db.Cluster.GetService(db.ServiceName)
	db.Service.Bind(&nrv.Binding{
		Path: "/execute",
		// TODO: RESOLVER ANY!
		Closure: func(request *nrv.ReceivedRequest) {
			logger := nrv.Logger(request.Logger)
			trace := logger.Trace("mry")
			iTrx := request.Message.Data["t"]

			if trx, ok := iTrx.(*Transaction); ok {
				logger.Debug("Executing transaction %d", *trx.Id)

				// dry execution to discover token and some errors
				traceDry := logger.Trace("execute_dry")
				context := db.executeLocal(trx, logger, true)
				if context.token == nil && context.ret.Error == nil {
					context.setError("Couldn't find token for transaction")
				}
				traceDry.End()

				// continue if no error
				if context.ret.Error == nil {
					logger.Debug("Transaction has token %d", *context.token)

					traceReal := logger.Trace("execute_real")
					context = db.executeLocal(trx, logger, false)
					if context.ret.Error == nil {
						err := context.storageTrx.Commit()
						if err != nil {
							context.setError("Couldn't commit transaction: %s", err)
						}
					} else {
						logger.Debug("Error executing transaction: %s", context.ret.Error)
					}
					traceReal.End()
				} else {
					context.storageTrx.Rollback()
					logger.Debug("Error executing transaction in dry mode: %s", context.ret.Error)
				}
				trace.End()

				request.Reply(nrv.Map{
					"t": &Transaction{
						Id:     trx.Id,
						Return: context.ret,
					},
				})
			} else {
				logger.Error("Received a null transaction")

			}

		},
	})

	db.Cluster.GetDefaultProtocol().AddMarshaller(&pbMarshaller{})
	db.Storage.Init()
}

func (db *Db) NewTransaction(cb func(b Block)) *Transaction {
	trx := &Transaction{
		Id: pb.Uint64(uint64(time.Now().UnixNano())), // TODO: put real id from nrv		
	}
	cb(trx.newBlock())
	return trx
}

func (db *Db) Execute(t Transactable) *TransactionReturn {
	return db.ExecuteLogger(t, &nrv.RequestLogger{})
}

func (db *Db) ExecuteLogger(t Transactable, logger nrv.Logger) *TransactionReturn {
	trx := t.GetTransaction()
	resp := <-db.Service.CallChan("/execute", &nrv.Request{
		Message: &nrv.Message{
			Logger: logger,
			Data: nrv.Map{
				"t": trx,
			},
		},
	})
	return resp.Message.Data["t"].(*Transaction).Return
}

func (db *Db) executeLocal(trx *Transaction, logger nrv.Logger, dry bool) *transactionContext {
	context := &transactionContext{
		dry:        dry,
		db:         db,
		trx:        trx,
		logger:     logger,
		storageTrx: nil,
	}
	context.init()

	if !dry {
		trxTime := time.Unix(0, int64(*trx.Id))
		storageTrx, err := db.Storage.GetTransaction(trxTime)
		if err != nil {
			context.setError("Couldn't get storage transaction: %s", err)
			return context
		}
		context.storageTrx = storageTrx
	}

	trx.execute(context)
	return context
}

//
// Transaction marshaller used by nrv to stream stransactions over 
// network
//
type pbMarshaller struct {
}

func (pbm *pbMarshaller) MarshallerName() string {
	return "mrytrx"
}

func (pbm *pbMarshaller) CanMarshal(obj interface{}) bool {
	if _, ok := obj.(*Transaction); ok {
		return true
	}
	return false
}

func (pbm *pbMarshaller) Marshal(obj interface{}) ([]byte, error) {
	return pb.Marshal(obj)
}

func (pbm *pbMarshaller) Unmarshal(bytes []byte) (interface{}, error) {
	trx := &Transaction{}
	err := pb.Unmarshal(bytes, trx)
	return trx, err
}
