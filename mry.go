package mry

import (
	"os"
	"github.com/appaquet/nrv"
	pb "goprotobuf.googlecode.com/hg/proto"
)

//
// Database object bound to a specific domain of a given cluster
//
type Db struct {
	DomainName string
	Cluster    nrv.Cluster
	Storage    Storage
	Domain     *nrv.Domain

	tables map[string]*Table
}

func (db *Db) Start() {
	db.Domain = db.Cluster.GetDomain(db.DomainName)
	db.Domain.Bind(&nrv.Binding{
		Path: "/execute",
		// TODO: RESOLVER ANY!
		Closure: func(request *nrv.ReceivedRequest) {
			nrv.Log.Debug("mry> Executing transaction: %s", request.Message.Params["t"])
			trx := request.Message.Params["t"].(*Transaction)

			// dry execution to disacover token and errors
			context := db.executeLocal(trx, true)
			if context.token == nil && context.ret.Error != nil {
				context.setError("Couldn't find token for transaction")
			}

			// continue if no error
			if context.ret.Error == nil { 
				nrv.Log.Trace("mry> Transaction has token %s", context.token)

				context = db.executeLocal(trx, false)
				if context.ret.Error == nil {
					err := context.storageTrx.Commit()
					if err != nil {
						context.setError("Couldn't commit transaction: %s", err)
					}
				} else {
					nrv.Log.Debug("mry> Error executing transaction: %s", context.ret.Error)
				}
			} else {
				nrv.Log.Debug("mry> Error executing transaction in dry mode: %s", context.ret.Error)
			}

			request.Respond(&nrv.Message{
				Params:nrv.Map{	
					"t": &Transaction{
						Id: trx.Id,
						Return: context.ret,
					},
				},
			})
		},
	})

	db.Cluster.GetDefaultProtocol().AddMarshaller(&pbMarshaller{})
	db.Storage.Init()
}

func (db *Db) NewTransaction(cb func(b *TransactionBlock)) *Transaction {
	trx := &Transaction{
		Id: pb.Uint64(0), // TODO: put real id from nrv		
	}
	cb(trx.newBlock())
	return trx
}

func (db *Db) Execute(t Transactable) *TransactionReturn {
	trx := t.GetTransaction()
	resp := <-db.Domain.CallChan("/execute", &nrv.Request{
		Message: &nrv.Message{
			Params: nrv.Map{
				"t": trx,
			},
		},
	})
	return resp.Message.Params["t"].(*Transaction).Return
}

func (db *Db) executeLocal(trx *Transaction, dry bool) *transactionContext {
	context := &transactionContext{
		dry: dry,
		db:  db,
		trx: trx,
		storageTrx: nil,
	}
	context.init()

	storageTrx, err := db.Storage.GetTransaction()
	if err != nil {
		context.setError("Couldn't get storage transaction: %s", err)
		return context
	}

	context.storageTrx = storageTrx

	trx.execute(context)
	return context
}

func (db *Db) GetTable(name string) *Table {
	table, found := db.tables[name]

	// TODO: probably shouldn't create it straight away, but wait for user to create it (return error??)
	if !found {
		table = &Table{name}
		db.tables[name] = table
	}

	return table
}

//
// Structure that represents a table in the storage
//
type Table struct {
	Name string
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

func (pbm *pbMarshaller) Marshal(obj interface{}) ([]byte, os.Error) {
	return pb.Marshal(obj)
}

func (pbm *pbMarshaller) Unmarshal(bytes []byte) (interface{}, os.Error) {
	trx := &Transaction{}
	err := pb.Unmarshal(bytes, trx)
	return trx, err
}
