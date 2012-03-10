package mry

import (
	pb "code.google.com/p/goprotobuf/proto"
	"errors"
	"fmt"

	"reflect"
)

// Interface of an object that can be handled as a transaction
type Transactable interface {
	GetTransaction() *Transaction
}

type Block interface {
	From(name string) BlockVariable
	Into(name string) BlockVariable
	Return(data ...interface{}) BlockVariable
}

type Return interface {
	GetAll() []interface{}
	Into(destinations ...interface{}) error
}

type BlockVariable interface {
	Rel(tableName string) BlockVariable
	Get(key interface{}) BlockVariable
	Set(key interface{}, val interface{}) BlockVariable
	Return() BlockVariable
	Filter(something interface{}) BlockVariable
	Order(something interface{}) BlockVariable
	GetAll() BlockVariable
}

// Transaction that encapsulates operations that will be executed on 
// the storage

//type Transaction struct {
//	Id               *uint64             `protobuf:"varint,1,opt,name=id"`
//	Return           *TransactionReturn  `protobuf:"bytes,2,opt,name=return"`
//	Blocks           []*TransactionBlock `protobuf:"bytes,10,rep,name=blocks"`
//	XXX_unrecognized []byte
//}

func (trx *Transaction) GetTransaction() *Transaction {
	return trx
}

func (trx *Transaction) newBlock() *TransactionBlock {
	id := len(trx.Blocks)
	b := &TransactionBlock{
		Id:         pb.Uint32(uint32(id)),
		Variables:  make([]*TransactionVariable, 0),
		Operations: make([]*TransactionOperation, 0),
	}
	trx.Blocks = append(trx.Blocks, b)
	return b
}

func (trx *Transaction) execute(context *transactionContext) {
	context.init()

	// find main block
	var mainBlock *TransactionBlock
	for _, block := range trx.Blocks {
		if block.Parent == nil {
			mainBlock = block
			break
		}
	}

	if mainBlock == nil {
		context.setError("No main block defined")
		return
	}

	mainBlock.execute(context)
}

func (b *TransactionBlock) execute(context *transactionContext) {
	for _, op := range b.Operations {
		stop := op.execute(context)
		if stop || context.ret.Error != nil {
			return
		}
	}
}

func (b *TransactionBlock) newClientVariable() *clientVar {
	id := len(b.Variables)
	v := &TransactionVariable{
		Id:    pb.Uint32(uint32(id)),
		Block: b.Id,
	}
	b.Variables = append(b.Variables, v)
	return &clientVar{block: b, variable: v}
}

func (b *TransactionBlock) addOperation(op *TransactionOperation) {
	b.Operations = append(b.Operations, op)
}

func (b *TransactionBlock) From(name string) BlockVariable {
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		GetTable: &TransactionOperation_GetTable{
			TableName:   toObject(name),
			Destination: nv.variable,
		},
	})
	return nv
}

func (b *TransactionBlock) Into(name string) BlockVariable {
	return b.From(name)
}

func (b *TransactionBlock) Return(data ...interface{}) BlockVariable {
	nv := b.newClientVariable()

	objData := make([]*TransactionObject, len(data))
	for i, iface := range data {
		objData[i] = toObject(iface)
	}

	b.addOperation(&TransactionOperation{
		Return: &TransactionOperation_Return{
			Data: objData,
		},
	})
	return nv
}

// Wrapped transaction variable to support operations 
// that will be stacked on the current block
type clientVar struct {
	block    *TransactionBlock
	variable *TransactionVariable
}

func (v *clientVar) getBlock() *TransactionBlock {
	// TODO: should return last stacked block (for recursive block)
	return v.block
}

func (v *clientVar) Rel(tableName string) BlockVariable {
	b := v.getBlock()
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		GetTable: &TransactionOperation_GetTable{
			TableName:   toObject(tableName),
			Destination: nv.variable,
			Source:      v.variable,
		},
	})
	return nv

}

func (v *clientVar) Get(key interface{}) BlockVariable {
	b := v.getBlock()
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		Get: &TransactionOperation_Get{
			Source:      v.variable,
			Key:         toObject(key),
			Destination: nv.variable,
		},
	})
	return nv
}

func (v *clientVar) Set(key interface{}, val interface{}) BlockVariable {
	b := v.getBlock()
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		Set: &TransactionOperation_Set{
			Destination: v.variable,
			Key:         toObject(key),
			Value:       toObject(val),
		},
	})
	return nv
}

func (v *clientVar) Return() BlockVariable {
	b := v.getBlock()
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		Return: &TransactionOperation_Return{
			Data: []*TransactionObject{toObject(v.variable)},
		},
	})
	return nv
}

func (v *clientVar) Filter(something interface{}) BlockVariable {
	nv := v.getBlock().newClientVariable()
	// TODO: implement
	return nv
}

func (v *clientVar) Order(something interface{}) BlockVariable {
	nv := v.getBlock().newClientVariable()
	// TODO: implement
	return nv
}

func (v *clientVar) GetAll() BlockVariable {
	b := v.getBlock()
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		Getall: &TransactionOperation_GetAll{
			Destination: nv.variable,
			Source:      v.variable,
		},
	})
	return nv
}

//type TransactionReturn struct {
//	Error            *TransactionError   `protobuf:"bytes,1,opt,name=error"`
//	Data             []*TransactionValue `protobuf:"bytes,2,rep,name=data"`
//	XXX_unrecognized []byte
//}

func (r *TransactionReturn) GetAll() []interface{} {
	ret := make([]interface{}, len(r.Data))
	for i, tVal := range r.Data {
		if tVal != nil {
			ret[i] = tVal.ToInterface()
		} else {
			ret[i] = nil
		}
	}
	return ret
}

func (r *TransactionReturn) Into(destinations ...interface{}) error {
	iVals := r.GetAll()
	var err error

	for i := 0; i < len(destinations) && i < len(iVals); i++ {
		rflDest := reflect.ValueOf(destinations[i])
		rflVal := reflect.ValueOf(iVals[i])

		// if got a nil value, set dest nil
		if !rflVal.IsValid() {
			destinations[i] = nil
		} else {
			// directly assignable (ptr to ptr)
			if rflDest.Type().AssignableTo(rflVal.Type()) {
				rflDest.Set(rflVal)
			} else {
				// else, set ptr value
				elmDest := rflDest.Elem()
				if elmDest.CanSet() && elmDest.Type().AssignableTo(rflVal.Type()) {
					elmDest.Set(rflVal)
				} else {
					err = errors.New(fmt.Sprintf("Cannot set destination %i: Destination non-settable or value unassignable (val %s)", i, rflVal))
				}
			}
		}
	}

	return err
}
