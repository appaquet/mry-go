package mry

import (
	pb "code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/appaquet/nrv"
)

// Transaction execution context that encapsulate everything
// a transaction execution needs
type transactionContext struct {
	dry        bool
	db         *Db
	trx        *Transaction
	ret        *TransactionReturn
	storageTrx StorageTransaction
	logger     nrv.Logger
	vars       map[string]*serverVariable
	token      *nrv.Token
}

func (tc *transactionContext) setError(message string, params ...interface{}) {
	errMsg := fmt.Sprintf(message, params...)
	tc.logger.Debug("Transaction error: %s", errMsg)
	tc.ret.Error = &TransactionError{
		Id:      pb.Uint32(0),
		Message: pb.String(errMsg),
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
	switch {

	case o.Get != nil:
		o.Get.execute(o, context)
		return false
	case o.Set != nil:
		o.Set.execute(o, context)
		return false
	case o.GetTable != nil:
		o.GetTable.execute(o, context)
		return false
	case o.Getall != nil:
		o.Getall.execute(o, context)
		return false

	case o.Return != nil:
		o.Return.execute(o, context)
		return true
	}

	context.setError("Unsupported operation %s", o)
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

	if os.Source == nil {
		destVar := context.getServerVariable(os.Destination)

		// get table from model
		strTable := os.TableName.getValue(context).ToInterface().(string)
		table := context.db.GetTable(strTable)
		if table == nil {
			context.setError("Table not found", strTable)
			return
		}

		destVar.value = &tableValue{
			table:  table,
			prefix: []string{},
		}
	} else {
		sourceVar := context.getServerVariable(os.Source)
		if handler, ok := sourceVar.value.(getTableHandler); ok {
			destVar := context.getServerVariable(os.Destination)
			handler.getTable(context, os.TableName.getValue(context).ToInterface(), destVar)

		} else if !context.dry {
			context.setError("Cannot execute get table on that variable")
		}
	}
}

func (os *TransactionOperation_Return) execute(op *TransactionOperation, context *transactionContext) {
	context.logger.Debug("Executing 'return' with %s", os.Data)

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

func (og *TransactionOperation_GetAll) execute(op *TransactionOperation, context *transactionContext) {
	sourceVar := context.getServerVariable(og.Source)
	if handler, ok := sourceVar.value.(getAllHandler); ok {
		destVar := context.getServerVariable(og.Destination)
		handler.getAll(context, destVar)

	} else if !context.dry {
		context.setError("Cannot execute getAll on that variable")
	}
}

//
// Operation handlers
//

// Represents a value on which we can execute "Get"
type getHandler interface {
	serverValue
	get(context *transactionContext, key interface{}, destination *serverVariable)
}

// Represents a value on which we can execute "GetAll"
type getAllHandler interface {
	serverValue
	getAll(context *transactionContext, destination *serverVariable)
}

// Represents a value on which we can execute "GetTable"
type getTableHandler interface {
	serverValue
	getTable(context *transactionContext, key interface{}, destination *serverVariable)
}

// Represents a value on which we can execute "Set"
type setHandler interface {
	serverValue
	set(context *transactionContext, key interface{}, value serverValue)
}

//
// Server variables & values
//

type serverVariable struct {
	variable *TransactionVariable
	value    serverValue
}

type serverValue interface {
	toTransactionValue() *TransactionValue
}

func toServerValue(val *TransactionValue) serverValue {
	switch {
	case val.StringValue != nil:
		return &stringValue{*val.StringValue}
	case val.IntValue != nil:
		return &intValue{*val.IntValue}
	case val.Map != nil:
		return &mapValue{val.Map, nil}
	case val.Array != nil:
		return &arrayValue{val.Array, nil}
	}

	panic(fmt.Sprintf("Unsupported server value: %s", val))
	return nil
}

// Represents a nil value
type nilValue struct {
}

func (v *nilValue) toTransactionValue() *TransactionValue {
	return toTransactionValue(nil)
}

// Represents a string value
type stringValue struct {
	value string
}

func (sv *stringValue) toTransactionValue() *TransactionValue {
	return toTransactionValue(sv.value)
}

// Represents a int value
type intValue struct {
	value int64
}

func (sv *intValue) toTransactionValue() *TransactionValue {
	return toTransactionValue(sv.value)
}

// Represents a map value
type mapValue struct {
	trxCollection *TransactionCollection
	value         nrv.Map
}

func (mv *mapValue) getMap() nrv.Map {
	if mv.value == nil {
		trxVal := &TransactionValue{Map: mv.trxCollection}
		mv.value = trxVal.ToInterface().(nrv.Map)
	}

	return mv.value
}

func (mv *mapValue) remove(key string) {
	delete(mv.getMap(), key)
}

func (mv *mapValue) toTransactionValue() *TransactionValue {
	// if interface is set, value may have changed
	if mv.value != nil {
		return toTransactionValue(mv.value)
	}

	return &TransactionValue{
		Map: mv.trxCollection,
	}
}

// Represents a array value
type arrayValue struct {
	value *TransactionCollection
	intrf interface{}
}

func (av *arrayValue) toTransactionValue() *TransactionValue {
	// if interface is set, value may have changed
	if av.intrf != nil {
		return toTransactionValue(av.intrf)
	}

	return &TransactionValue{
		Array: av.value,
	}
}

// Query that can be executed on a storage
type queryValue struct {
	query *StorageQuery
}

func (qv *queryValue) toTransactionValue() *TransactionValue {
	return toTransactionValue(fmt.Sprintf("QUERY %s", qv.query))
}

func (qv *queryValue) getAll(context *transactionContext, destination *serverVariable) {
	context.logger.Debug("Executing 'getAll' on query value %s", qv)

	// if no prefix, we are at top level
	if len(qv.query.TablePrefix) == 0 {
		context.setError("'getAll' not supported on top level tables")
		return
	}

	if !context.dry {
		iterator, err := context.storageTrx.GetQuery(*qv.query)
		if err != nil {
			context.setError("Got a storage error executing getquery: %s", err)
			return
		}

		collection := &TransactionCollection{}

		for {
			row, err := iterator.Next()
			if row != nil && err == nil {
				val := &TransactionValue{}
				marshErr := val.Unmarshall(row.Data)
				if marshErr != nil {
					context.setError("Couldn't unmarshall value: %s", marshErr)
					iterator.Close()
					return
				}

				collection.Add(&TransactionCollectionValue{Value: val})
			} else if err != nil {
				context.setError("Got a storage error iterating over 'getall': %s", err)
				iterator.Close()
				return
			} else if row == nil {
				break
			}
		}

		iterator.Close()

		destination.value = &arrayValue{value: collection}
	}
}

// Table
type tableValue struct {
	table  *Table 
	prefix []string
}

func (tv *tableValue) get(context *transactionContext, key interface{}, destination *serverVariable) {
	context.logger.Debug("Executing 'get' on table %s with key %s, prefix %s", tv.table, key, tv.prefix)

	strKey := fmt.Sprint(key)

	// if no prefix, we resolve token
	if len(tv.prefix) == 0 {
		token := nrv.HashToken(strKey)
		if context.token != nil && *context.token != token {
			context.setError("Token conflict: %s!=%s", token, *context.token)
			return
		}
		context.token = &token
	}

	row := &rowValue{
		table:   tv,
		key:     strKey,
		context: context,
	}

	// make sure we get data from db
	row.getRow()

	destination.value = row
}

func (tv *tableValue) set(context *transactionContext, key interface{}, value serverValue) {
	context.logger.Debug("Executing 'set' on table %s with key %s, prefix %s", tv.table, key, tv.prefix)

	strKey := fmt.Sprint(key)

	// if no prefix, we resolve token
	if len(tv.prefix) == 0 {
		token := nrv.HashToken(strKey)
		if context.token != nil && *context.token != token {
			context.setError("Token conflict: %s!=%s", token, *context.token)
			return
		}
		context.token = &token
	}

	if mapVal, isMap := value.(*mapValue); isMap {
		if !context.dry {
			mapVal.remove("_timestamp")
			mapVal.remove("_key1")
			mapVal.remove("_key2")
			mapVal.remove("_key3")
			mapVal.remove("_key4")

			bytes, err := mapVal.toTransactionValue().Marshall()
			if err != nil {
				context.setError("Couldn't marshall value: %s", err)
				return
			}

			l := len(tv.prefix) + 1
			keys := make([]string, l)
			for i := 0; i < l-1; i++ {
				keys[i] = tv.prefix[i]
			}
			keys[l-1] = fmt.Sprintf("%s", key)

			err = context.storageTrx.Set(tv.table, keys, bytes)
			if err != nil {
				context.setError("Couldn't set value into table: %s", err)
				return
			}
		}

	} else {
		context.setError("Can only store a map into table")
		return
	}
}

func (tv *tableValue) getAll(context *transactionContext, destination *serverVariable) {
	context.logger.Debug("Executing 'getAll' on table %s, prefix %s", tv.table, tv.prefix)

	// if no prefix, we are at top level
	if len(tv.prefix) == 0 {
		context.setError("'getAll' not supported on top level tables")
		return
	}

	if !context.dry {
		queryVal := &queryValue{&StorageQuery{
			Table:       tv.table,
			TablePrefix: tv.prefix,
		}}

		queryVal.getAll(context, destination)
	}
}

func (tv *tableValue) toTransactionValue() *TransactionValue {
	// TODO: return something else ??
	return toTransactionValue("TABLE " + tv.table.Name)
}

// Value of a row in a table
type rowValue struct {
	table   *tableValue
	key     string
	context *transactionContext

	row	     *Row
	srvValue     *mapValue
}

func (rv *rowValue) getRow() *Row {
	if rv.row == nil {
		if !rv.context.dry {
			// build keys array
			l := len(rv.table.prefix) + 1
			keys := make([]string, l)
			for i := 0; i < l-1; i++ {
				keys[i] = rv.table.prefix[i]
			}
			keys[l-1] = rv.key

			// get from storage
			row, err := rv.context.storageTrx.Get(rv.table.table, keys)
			if err != nil {
				rv.context.setError("Couldn't get from storage for table %s, keys %s: %s", rv.table.table.Name, keys, err)
				return nil
			}

			rv.row = row
		}
	}

	return rv.row
}

func (rv *rowValue) getSrvValue() *mapValue {
	if rv.srvValue == nil {
		row := rv.getRow()
		trxVal := &TransactionValue{}
		if row != nil {
			err := trxVal.Unmarshall(row.Data)
			if err != nil {
				rv.context.setError("Couldn't unmarshall value: %s", err)
				return &mapValue{value:nrv.Map{}}
			}

			rv.srvValue = &mapValue{
				trxCollection: trxVal.Map,
			}
		} else {
			return nil
		}

	}

	return rv.srvValue
}

func (rv *rowValue) toTransactionValue() *TransactionValue {
	srvValue := rv.getSrvValue()

	if srvValue != nil {
		srvValue.remove("_timestamp")
		srvValue.remove("_key1")
		srvValue.remove("_key2")
		srvValue.remove("_key3")
		srvValue.remove("_key4")

		row := rv.getRow()
		if row != nil && srvValue.value != nil {
			srvValue.value["_timestamp"] = row.IntTimestamp
			srvValue.value["_key1"] = row.Key1
			srvValue.value["_key2"] = row.Key2
			srvValue.value["_key3"] = row.Key3
			srvValue.value["_key4"] = row.Key4
		}

		return srvValue.toTransactionValue()
	}

	return nil
}

func (rv *rowValue) getTable(context *transactionContext, table interface{}, destination *serverVariable) {
	strTable := fmt.Sprint(table)

	context.logger.Debug("Executing 'getTable' on table %s via %s with key %s", strTable, rv.table, rv.key)

	modelTable := rv.table.table.GetSubTable(strTable)
	if modelTable == nil {
		context.setError("Couldn't find table named %s via %s", strTable, rv.table.table.Name)
		return
	}

	tv := &tableValue{
		table:  modelTable,
		prefix: append(rv.table.prefix, rv.key),
	}

	destination.value = tv
}
