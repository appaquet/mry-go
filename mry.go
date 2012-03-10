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
	*Model

	ServiceName string

	Cluster     nrv.Cluster
	Storage     Storage
	Service     *nrv.Service
}

func (db *Db) SetupCluster() {
	db.Model = newModel()

	db.Service = db.Cluster.GetService(db.ServiceName)

	db.Service.Bind(&nrv.Binding{
		Path: "^/execute$",
		Resolver: &nrv.ResolverParam{Count: 1},
		Controller: db,
		Method: "NrvExecute",
	})

	db.Service.Bind(&nrv.Binding{
		Path: "^/execute/write/(.*)$",
		Resolver: &nrv.ResolverParam{Count: 1},
		Controller: db,
		Method: "NrvExecuteWrite",
	})

	db.Service.Bind(&nrv.Binding{
		Path: "^/execute/read/(.*)$",
		Resolver: &nrv.ResolverParam{Count: 1},
		Controller: db,
		Method: "NrvExecuteRead",
	})

	db.Cluster.GetDefaultProtocol().AddMarshaller(&trxMarshaller{})
	db.Cluster.GetDefaultProtocol().AddMarshaller(&mutMarshaller{})
	db.Storage.Init()
}

// TODO: rollback on panic
func (db *Db) NrvExecute(request *nrv.ReceivedRequest) {
	logger := nrv.Logger(request.Logger)
	trace := logger.Trace("mry")
	iTrx := request.Message.Data["t"]

	if trx, ok := iTrx.(*Transaction); ok {
		logger.Debug("Executing transaction %d %s", *trx.Id)

		// dry execution to discover token and some errors
		traceDry := logger.Trace("execute_dry")

		context := &transactionContext{
			dry:        true,
			db:         db,
			trx:        trx,
			logger:     logger,
			storageTrx: nil,
		}
		context.init()

		db.executeLocal(context)
		if context.token == nil && context.ret.Error == nil {
			context.setError("Couldn't find token for transaction")
		}
		traceDry.End()

		// continue if no error
		if context.ret.Error == nil {
			logger.Debug("Transaction has token %d", *context.token)

			traceReal := logger.Trace("execute_real")

			context = &transactionContext{
				dry:        false,
				db:         db,
				trx:        trx,
				logger:     logger,
				token:      context.token,
				storageTrx: nil,
			}
			context.init()

			db.executeLocal(context)
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
			if context.storageTrx != nil {
				context.storageTrx.Rollback()
			}
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
}

func (db *Db) SyncModel() error {
	return db.Storage.SyncModel(db.Model)
}

func (db *Db) NewTransaction(cb func(b Block)) *Transaction {
	trx := &Transaction{
		Id: pb.Uint64(uint64(time.Now().UnixNano())), // TODO: put real id from nrv		
	}
	cb(trx.newBlock())
	return trx
}

func (db *Db) Execute(cb func(b Block)) *TransactionReturn {
	return db.ExecuteTrx(db.NewTransaction(cb))
}

func (db *Db) ExecuteLog(cb func(b Block), logger nrv.Logger) *TransactionReturn {
	return db.ExecuteTrxLog(db.NewTransaction(cb), logger)
}

func (db *Db) ExecuteTrx(t Transactable) *TransactionReturn {
	return db.ExecuteTrxLog(t, &nrv.RequestLogger{})
}

func (db *Db) ExecuteTrxLog(t Transactable, logger nrv.Logger) *TransactionReturn {
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

func (db *Db) executeLocal(context *transactionContext) {
	if !context.dry {
		trxTime := time.Unix(0, int64(*context.trx.Id))
		trc := context.logger.Trace("gettrx")
		storageTrx, err := context.db.Storage.GetTransaction(*context.token, trxTime)
		if err != nil {
			context.setError("Couldn't get storage transaction: %s", err)
			return
		}
		context.storageTrx = storageTrx
		trc.End()
	}

	context.trx.execute(context)
}

//
// Transaction marshaller used by nrv to stream stransactions over 
// network
//
type trxMarshaller struct {
}

func (pbm *trxMarshaller) MarshallerName() string {
	return "mrytrx"
}

func (pbm *trxMarshaller) CanMarshal(obj interface{}) bool {
	if _, ok := obj.(*Transaction); ok {
		return true
	}
	return false
}

func (pbm *trxMarshaller) Marshal(obj interface{}) ([]byte, error) {
	return pb.Marshal(obj)
}

func (pbm *trxMarshaller) Unmarshal(bytes []byte) (interface{}, error) {
	trx := &Transaction{}
	err := pb.Unmarshal(bytes, trx)
	return trx, err
}

type mutMarshaller struct {
}

func (pbm *mutMarshaller) MarshallerName() string {
	return "mrymut"
}

func (pbm *mutMarshaller) CanMarshal(obj interface{}) bool {
	if _, ok := obj.(*JobRowMutation); ok {
		return true
	}
	return false
}

func (pbm *mutMarshaller) Marshal(obj interface{}) ([]byte, error) {
	return pb.Marshal(obj)
}

func (pbm *mutMarshaller) Unmarshal(bytes []byte) (interface{}, error) {
	trx := &JobRowMutation{}
	err := pb.Unmarshal(bytes, trx)
	return trx, err
}
