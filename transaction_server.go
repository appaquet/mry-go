package mry

import (
	pb "goprotobuf.googlecode.com/hg/proto"
	"github.com/appaquet/nrv"
	"fmt"
)

// Transaction execution context that encapsulate everything
// a transaction execution needs
type transactionContext struct {
	dry        bool
	db         *Db
	trx        *Transaction
	ret        *TransactionReturn
	storageTrx StorageTransaction
	vars       map[string]*serverVariable
	token      *nrv.Token
}

func (tc *transactionContext) setError(message string, params ...interface{}) {
	tc.ret.Error = &TransactionError{
		Id:      pb.Uint32(0),
		Message: pb.String(fmt.Sprintf(message, params)),
	}
}

func (tc *transactionContext) init() {
	tc.ret = &TransactionReturn{}
	tc.vars = make(map[string]*serverVariable)
}

func (tc *transactionContext) getServerVariable(variable *TransactionVariable) *serverVariable {
	sv, found := tc.vars[variable.String()]
	if !found {
		sv = &serverVariable{
			variable: variable,
			value:    nil,
		}
		tc.vars[variable.String()] = sv
	}
	return sv
}


//
// Operations 
//

func (o *TransactionOperation) execute(context *transactionContext) (stop bool) {
	nrv.Log.Trace("mry> Executing operation %s", o)

	if o.Get != nil {
		o.Get.execute(o, context)
		return false
	} else if o.Set != nil {
		o.Set.execute(o, context)
		return false
	} else if o.GetTable != nil {
		o.GetTable.execute(o, context)
		return false
	} else if o.Return != nil {
		o.Return.execute(o, context)
		return true
	}

	context.setError("Unsupported  operation %s", o)
	return true
}

func (og *TransactionOperation_Get) execute(op *TransactionOperation, context *transactionContext) {
	sourceVar := context.getServerVariable(og.Source)
	if handler, ok := sourceVar.value.(getHandler); ok {
		destVar := context.getServerVariable(og.Destination)
		handler.get(context, og.Key.getValue(context).ToInterface(), destVar)

	} else if !context.dry {
		context.setError("Cannot execute get on that variable")
	}
}

func (os *TransactionOperation_Set) execute(op *TransactionOperation, context *transactionContext) {
	destVar := context.getServerVariable(os.Destination)
	if handler, ok := destVar.value.(setHandler); ok {
		handler.set(context, os.Key.getValue(context).ToInterface(), toServerValue(os.Value.getValue(context)))

	} else if !context.dry {
		context.setError("Cannot execute set on that variable")
	}
}

func (os *TransactionOperation_GetTable) execute(op *TransactionOperation, context *transactionContext) {
	// TODO: handle if os.From != nil, we get table in relation with another object 
	destVar := context.getServerVariable(os.Destination)
	destVar.value = &tableValue{
		table: os.TableName.getValue(context).ToInterface().(string),
		op:    op,
	}
}

func (os *TransactionOperation_Return) execute(op *TransactionOperation, context *transactionContext) {
	if !context.dry {
		vals := make([]*TransactionValue, len(os.Data))

		for i, obj := range os.Data {
			vals[i] = obj.getValue(context)
		}

		context.ret = &TransactionReturn{
			Data: vals,
		}
	}
}


type serverVariable struct {
	variable *TransactionVariable
	value    serverValue
}

type serverValue interface {
	toTransactionValue() *TransactionValue
}

func toServerValue(val *TransactionValue) serverValue {
	switch {
	case val.BoolValue != nil:
		return nil // TODO
	case val.BytesValue != nil:
		return nil // TODO
	case val.DoubleValue != nil:
		return nil // TODO
	case val.FloatValue != nil:
		return nil // TODO
	case val.IntValue != nil:
		return nil // TODO
	case val.StringValue != nil:
		return &stringValue{*val.StringValue}
	}

	return &nilValue{}
}


// Represents a value on which we can execute "Get"
type getHandler interface {
	serverValue
	get(context *transactionContext, key interface{}, destination *serverVariable)
}

// Represents a value on which we can execute "Set"
type setHandler interface {
	serverValue
	set(context *transactionContext, key interface{}, value serverValue)
}



// Represents a nil value
type nilValue struct {
}

func (v *nilValue) toTransactionValue() *TransactionValue {
	return toTransactionValue(nil)
}


// Represents a string value
type stringValue struct {
	value  string	
}

func (sv *stringValue) toTransactionValue() *TransactionValue {
	return toTransactionValue(sv.value)
}


// Represents a table value
type tableValue struct {
	// TODO: prefix keys
	table string
	op    *TransactionOperation
}

func (tv *tableValue) get(context *transactionContext, key interface{}, destination *serverVariable) {
	nrv.Log.Trace("mry> Executing 'get' on table %s with key %s", tv.table, key)

	strKey := fmt.Sprint(key)

	token := nrv.ResolveToken(strKey)
	if context.token != nil && *context.token != token {
		context.setError("Token conflict: %s!=%s", token, *context.token)
		return
	}
	context.token = &token

	if !context.dry {
		buf, err := context.storageTrx.Get(tv.table, []string{key.(string)})
		if err != nil {
			context.setError("Couldn't get from stage: %s", err.String())
			return
		}

		retVal := &TransactionValue{}
		err = retVal.Unmarshall(buf)
		if err != nil {
			context.setError("Couldn't unmarshall value: %s", err.String())
			return
		}
		destination.value = toServerValue(retVal)
	}
}

func (tv *tableValue) set(context *transactionContext, key interface{}, value serverValue) {
	nrv.Log.Trace("mry> Executing 'set' on table %s with key %s", tv.table, key)

	bytes, err := value.toTransactionValue().Marshall()
	if err != nil {
		context.setError("Couldn't marshall value: %s", err.String())
		return
	}

	err = context.storageTrx.Set(tv.table, []string{key.(string)}, bytes)
	if err != nil {
		context.setError("Couldn't set value into table: %s", err.String())
		return
	}
}

func (tv *tableValue) toTransactionValue() *TransactionValue {
	// TODO: return something else ??
	return toTransactionValue("TABLE " + tv.table)
}


