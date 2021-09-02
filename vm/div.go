package vm

func DivFunc(left, right func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return Null(), err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return Null(), err
		}

		return Div(leftValue, rightValue)
	}
}

func Div(leftValue, rightValue Value) (Value, error) {
	switch rightValue.Type {
	case ValueNull:
		return Null(), NewArithmeticError("/", leftValue.Type.String(), rightValue.Type.String())
	case ValueBool:
		return Null(), NewArithmeticError("/", leftValue.Type.String(), rightValue.Type.String())
	case ValueString:
		return Null(), NewArithmeticError("/", leftValue.Type.String(), rightValue.Type.String())
	case ValueInt64:
		return divInt(leftValue, rightValue.Int64)
	case ValueUint64:
		return divUint(leftValue, rightValue.Uint64)
	case ValueFloat64:
		return divFloat(leftValue, rightValue.Float64)
	// case ValueDatetime:
	//   return divDatetime(leftValue, IntToDatetime(rightValue.Int64))
	// case ValueInterval:
	//   return divInterval(leftValue, IntToInterval(rightValue.Int64))
	default:
		return Null(), NewArithmeticError("/", leftValue.Type.String(), rightValue.Type.String())
	}
}

func divInt(left Value, right int64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("/", left.Type.String(), "int")
	case ValueBool:
		return Null(), NewArithmeticError("/", left.Type.String(), "int")
	case ValueString:
		return Null(), NewArithmeticError("/", left.Type.String(), "int")
	case ValueInt64:
		return FloatToValue(float64(left.Int64) / float64(right)), nil
	case ValueUint64:
		return FloatToValue(float64(left.Uint64) / float64(right)), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 / float64(right)), nil
	default:
		return Null(), NewArithmeticError("/", left.Type.String(), "int")
	}
}

func divUint(left Value, right uint64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("/", left.Type.String(), "uint")
	case ValueBool:
		return Null(), NewArithmeticError("/", left.Type.String(), "uint")
	case ValueString:
		return Null(), NewArithmeticError("/", left.Type.String(), "uint")
	case ValueInt64:
		return FloatToValue(float64(left.Int64) / float64(right)), nil
	case ValueUint64:
		return FloatToValue(float64(left.Uint64) / float64(right)), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 / float64(right)), nil
	default:
		return Null(), NewArithmeticError("/", left.Type.String(), "uint")
	}
}

func divFloat(left Value, right float64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("/", left.Type.String(), "float")
	case ValueBool:
		return Null(), NewArithmeticError("/", left.Type.String(), "float")
	case ValueString:
		return Null(), NewArithmeticError("/", left.Type.String(), "float")
	case ValueInt64:
		return FloatToValue(float64(left.Int64) / float64(right)), nil
	case ValueUint64:
		return FloatToValue(float64(left.Uint64) / float64(right)), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 / right), nil
	default:
		return Null(), NewArithmeticError("/", left.Type.String(), "float")
	}
}

// func divDatetime(left Value, right time.Time) (Value, error) {
// 	if left.Type != ValueDatetime {
// 		return Null(), NewArithmeticError("/", left.Type.String(), "datetime")
// 	}

// 	t := IntToDatetime(left.Int64)
// 	return IntervalToValue(t.Sub(right)), nil
// }

// func divInterval(left Value, right time.Duration) (Value, error) {
// 	switch left.Type {
// 	case ValueDatetime:
// 		t := IntToDatetime(left.Int64)
// 		return DatetimeToValue(t.Add(-right)), nil
// 	case ValueInterval:
// 		t := IntToInterval(left.Int64)
// 		return IntervalToValue(t -right), nil
// 	default:
// 		return Null(), NewArithmeticError("/", left.Type.String(), "datetime")
// 	}
// }
