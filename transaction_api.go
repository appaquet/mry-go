package mry

import (
	"os"
	pb "goprotobuf.googlecode.com/hg/proto"
)

// Interface of an object that can be handled as a transaction
type Transactable interface {
	GetTransaction()  *Transaction
}

// Transaction that encapsulates operations that will be executed on 
// the storage
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

func (b *TransactionBlock) From(name string) *clientVar {
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		GetTable: &TransactionOperation_GetTable{
			TableName:   toObject(name),
			Destination: nv.variable,
		},
	})
	return nv
}

func (b *TransactionBlock) Into(name string) *clientVar {
	return b.From(name)
}

func (b *TransactionBlock) Return(data... interface{}) *clientVar {
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
	block         *TransactionBlock
	variable      *TransactionVariable
}

func (v *clientVar) getBlock() *TransactionBlock {
	// TODO: should return last stacked block (for recursive block)
	return v.block
}

func (v *clientVar) Rel(tableName string) *clientVar {
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

func (v *clientVar) Get(key interface{}) *clientVar {
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

func (v *clientVar) Set(key interface{}, val interface{}) *clientVar {
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

func (v *clientVar) Return() *clientVar {
	b := v.getBlock()
	nv := b.newClientVariable()
	b.addOperation(&TransactionOperation{
		Return: &TransactionOperation_Return{
			Data: []*TransactionObject{toObject(v.variable)},
		},
	})
	return nv
}

func (v *clientVar) Filter(something interface{}) *clientVar {
	nv := v.getBlock().newClientVariable()
	// TODO: implement
	return nv
}

func (v *clientVar) Order(something interface{}) *clientVar {
	nv := v.getBlock().newClientVariable()
	// TODO: implement
	return nv
}

func (v *clientVar) GetAll() *clientVar {
	nv := v.getBlock().newClientVariable()
	// TODO: implement
	return nv
}



func (val *TransactionValue) ToInterface() interface{} {
	switch {
	case val.BoolValue != nil:
		return *val.BoolValue
	case val.BytesValue != nil:
		return val.BytesValue
	case val.DoubleValue != nil:
		return *val.DoubleValue
	case val.FloatValue != nil:
		return *val.FloatValue
	case val.IntValue != nil:
		return *val.IntValue
	case val.StringValue != nil:
		return *val.StringValue
	}

	panic("Cannot convert value to interface")
	return nil
}

func (val *TransactionValue) Unmarshall(buf []byte) os.Error {
	return pb.Unmarshal(buf, val)
}

func (val *TransactionValue) Marshall() ([]byte, os.Error) {
	return pb.Marshal(val)
}

func (o *TransactionObject) getValue(context *transactionContext) *TransactionValue {
	// TODO: make sure at least value or variable are set
	if o.Value != nil {
		return o.Value
	}
	v := context.getServerVariable(o.Variable)
	return v.value.toTransactionValue()
}

func toObject(o interface{}) *TransactionObject {
	switch o.(type) {

	case *TransactionObject:
		return o.(*TransactionObject)

	case *TransactionVariable:
		return &TransactionObject{
			Variable: o.(*TransactionVariable),
		}

	case *clientVar:
		return &TransactionObject{
			Variable: o.(*clientVar).variable,
		}

	}

	return &TransactionObject{
		Value: toTransactionValue(o),
	}
}

func toTransactionValue(o interface{}) *TransactionValue {
	switch o.(type) {
	case string:
		return &TransactionValue{
			StringValue: pb.String(o.(string)),
		}
	case nil:
		return &TransactionValue{
		}
	}

	panic("Value not supported")
	return nil
}
