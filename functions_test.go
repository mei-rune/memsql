package memsql

import (
	"errors"

	"github.com/runner-mei/memsql/vm"
)

func init() {
	vm.Funcs["test"] = FuncTest
}

func FuncTest(ctx vm.Context, values []vm.Value) (vm.Value, error) {
	if len(values) == 0 {
		return vm.Null(), errors.New("test argument is missing")
	}
	if len(values) != 1 {
		return vm.Null(), errors.New("test argument isnot match")
	}

	switch values[0].Type {
	// case ValueNull:
	//   return BoolToValue(false), nil
	// case ValueBool:
	// 	if value.BoolValue() {
	// 		return UintToValue(1), nil
	// 	}
	// 	return UintToValue(0), nil
	case vm.ValueString:
		return vm.StringToValue("test_" + values[0].Str), nil
	// case ValueInt64:
	//     if decimaldigitValue.Int64 < 0 {
	//         return Null(), newArgumentError("round", "round argument decimaldigits invalid")
	//     }
	//     decimaldigits = int(decimaldigitValue.Int64)
	//  case ValueUint64:
	//   	decimaldigits = int( decimaldigitValue.Uint64)
	// case ValueFloat64:
	//  	return UintToValue(uint64(value.Float64)), nil
	// case ValueDatetime:
	// 	return value, nil
	// case ValueInterval:
	// 	return Null(), NewArithmeticError("convert", value.Type.String(), "datetime")
	default:
		return vm.Null(), errors.New("test argument type isnot match")
	}
}
