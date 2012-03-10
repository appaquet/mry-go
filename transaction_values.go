package mry

import (
	pb "code.google.com/p/goprotobuf/proto"
	"fmt"
	"github.com/appaquet/nrv"
	"reflect"
)

//message TransactionValue {
//	optional int64 int_value = 1;
//	optional bool bool_value = 2;
//	optional double double_value = 3;
//	optional string string_value = 4;
//	optional bytes bytes_value = 5;
//	optional TransactionCollection array = 6;
//	optional TransactionCollection map = 7;
//}
func toTransactionValue(o interface{}) *TransactionValue {
	// TODO: do it by reflection to catch map and arrays
	switch o.(type) {

	case *TransactionValue:
		return o.(*TransactionValue)

	case string:
		return &TransactionValue{
			StringValue: pb.String(o.(string)),
		}

	case int:
		return &TransactionValue{
			IntValue: pb.Int64(int64(o.(int))),
		}

	case int64:
		return &TransactionValue{
			IntValue: pb.Int64(o.(int64)),
		}

	case bool:
		return &TransactionValue{
			BoolValue: pb.Bool(o.(bool)),
		}

	case float32:
		return &TransactionValue{
			DoubleValue: pb.Float64(float64(o.(float32))),
		}

	case float64:
		return &TransactionValue{
			DoubleValue: pb.Float64(o.(float64)),
		}

	case nrv.Map, map[string]interface{}:
		var mp map[string]interface{}
		if val, ok := o.(map[string]interface{}); ok {
			mp = val
		} else {
			mp = map[string]interface{}(o.(nrv.Map))
		}

		values := make([]*TransactionCollectionValue, len(mp))
		i := 0

		for k, v := range mp {
			values[i] = &TransactionCollectionValue{
				Key:   pb.String(k),
				Value: toTransactionValue(v),
			}
			i++
		}

		return &TransactionValue{
			Map: &TransactionCollection{
				Values: values,
			},
		}

	case nrv.Array, []interface{}:
		var ar []interface{}
		if val, ok := o.([]interface{}); ok {
			ar = val
		} else {
			ar = []interface{}(o.(nrv.Array))
		}
		values := make([]*TransactionCollectionValue, len(ar))

		for i, v := range ar {
			values[i] = &TransactionCollectionValue{
				Value: toTransactionValue(v),
			}
		}

		return &TransactionValue{
			Array: &TransactionCollection{
				Values: values,
			},
		}

	case nil:
		return &TransactionValue{}
	}

	panic(fmt.Sprintf("Value not supported: %s", reflect.TypeOf(o)))
	return nil
}

func (val *TransactionValue) ToInterface() interface{} {
	switch {
	case val.Map != nil:
		iMap := nrv.NewMap()
		for _, val := range val.Map.Values {
			iMap[*val.Key] = val.Value.ToInterface()
		}
		return iMap
	case val.StringValue != nil:
		return *val.StringValue
	case val.Array != nil:
		iArray := nrv.NewArraySize(len(val.Array.Values))
		for i, val := range val.Array.Values {
			iArray[i] = val.Value.ToInterface()
		}
		return iArray
	case val.IntValue != nil:
		return *val.IntValue
	case val.BoolValue != nil:
		return *val.BoolValue
	case val.BytesValue != nil:
		return val.BytesValue
	case val.DoubleValue != nil:
		return *val.DoubleValue
	}

	return nil
}

func (val *TransactionValue) Unmarshall(buf []byte) error {
	return pb.Unmarshal(buf, val)
}

func (val *TransactionValue) Marshall() ([]byte, error) {
	return pb.Marshal(val)
}

//type TransactionObject struct {
//	Value            *TransactionValue    `protobuf:"bytes,1,opt,name=value"`
//	Variable         *TransactionVariable `protobuf:"bytes,2,opt,name=variable"`
//	XXX_unrecognized []byte
//}

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

//type TransactionCollection struct {
//	Values           []*TransactionCollectionValue `protobuf:"bytes,1,rep,name=values"`
//	XXX_unrecognized []byte
//}

func (c *TransactionCollection) Add(value *TransactionCollectionValue) {
	if c.Values == nil {
		c.Values = []*TransactionCollectionValue{value}
	} else {
		c.Values = append(c.Values, value)
	}
}
