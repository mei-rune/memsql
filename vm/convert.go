package vm

import (
	"strconv"
	"strings"
)

func ConvertToBool(readValue func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		value, err := readValue(ctx)
		if err != nil {
			return Null(), err
		}
		switch value.Type {
		case ValueNull:
			return BoolToValue(false), nil
		case ValueBool:
			return value, nil
		case ValueString:
			if strings.ToLower(value.StrValue()) == "true" {
				return BoolToValue(true), nil
			}
			return BoolToValue(false), nil
		case ValueInt64:
			if value.IntValue() != 0 {
				return BoolToValue(true), nil
			}
			return BoolToValue(false), nil
		case ValueUint64:
			if value.UintValue() != 0 {
				return BoolToValue(true), nil
			}
			return BoolToValue(false), nil
		// case ValueFloat64:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "boolean")
		// case ValueDatetime:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "boolean")
		// case ValueInterval:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "boolean")
		default:
			return Null(), newConvertError(nil, value, "boolean")
		}
	}
}

func ConvertToInt(readValue func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		value, err := readValue(ctx)
		if err != nil {
			return Null(), err
		}
		switch value.Type {
		// case ValueNull:
		//   return BoolToValue(false), nil
		case ValueBool:
			if value.BoolValue() {
				return IntToValue(1), nil
			}
			return IntToValue(0), nil
		case ValueString:
			i64, err := strconv.ParseInt(value.StrValue(), 10, 64)
			if err != nil {
				return Null(), newConvertError(err, value, "int")
			}
			return IntToValue(i64), nil
		case ValueInt64:
			return value, nil
		case ValueUint64:
			return IntToValue(int64(value.UintValue())), nil
		case ValueFloat64:
			return IntToValue(int64(value.FloatValue())), nil
		// case ValueDatetime:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "int")
		// case ValueInterval:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "int")
		default:
			return Null(), newConvertError(nil, value, "int")
		}
	}
}

func ConvertToUint(readValue func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		value, err := readValue(ctx)
		if err != nil {
			return Null(), err
		}
		switch value.Type {
		// case ValueNull:
		//   return BoolToValue(false), nil
		case ValueBool:
			if value.BoolValue() {
				return UintToValue(1), nil
			}
			return UintToValue(0), nil
		case ValueString:
			u64, err := strconv.ParseUint(value.StrValue(), 10, 64)
			if err != nil {
				return Null(), newConvertError(err, value, "uint")
			}
			return UintToValue(u64), nil
		case ValueInt64:
			return UintToValue(uint64(value.IntValue())), nil
		case ValueUint64:
			return value, nil
		case ValueFloat64:
			return UintToValue(uint64(value.FloatValue())), nil
		// case ValueDatetime:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "uint")
		// case ValueInterval:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "uint")
		default:
			return Null(), newConvertError(nil, value, "uint")
		}
	}
}

func ConvertToDatetime(readValue func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		value, err := readValue(ctx)
		if err != nil {
			return Null(), err
		}
		switch value.Type {
		// case ValueNull:
		//   return BoolToValue(false), nil
		// case ValueBool:
		// 	if value.BoolValue() {
		// 		return UintToValue(1), nil
		// 	}
		// 	return UintToValue(0), nil
		case ValueString:
			return ToDatetimeValue(value.StrValue())
		// case ValueInt64:
		//  	return UintToValue(uint64(value.Int64)), nil
		// case ValueUint64:
		//  	return value, nil
		// case ValueFloat64:
		//  	return UintToValue(uint64(value.Float64)), nil
		case ValueDatetime:
			return value, nil
		// case ValueInterval:
		// 	return Null(), NewArithmeticError("convert", value.Type.String(), "datetime")
		default:
			return Null(), newConvertError(nil, value, "datetime")
		}
	}
}

func newConvertError(err error, value Value, typeStr string) error {
	return NewArithmeticError("convert", value.Type.String(), typeStr)
}
