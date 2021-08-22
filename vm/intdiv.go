package vm

func IntDivFunc(left, right func(Context) (Value, error)) func(Context) (Value, error) {
  return func(ctx Context) (Value, error) {
    leftValue, err := left(ctx)
    if err != nil {
      return Null(), err
    }
    rightValue, err := right(ctx)
    if err != nil {
      return Null(), err
    }

    switch rightValue.Type {
    case ValueNull:
      return Null(), NewArithmeticError("div", leftValue.Type.String(), rightValue.Type.String())
    case ValueBool:
      return Null(), NewArithmeticError("div", leftValue.Type.String(), rightValue.Type.String())
    case ValueString:
      return Null(), NewArithmeticError("div", leftValue.Type.String(), rightValue.Type.String())
    case ValueInt64:
      return intDivInt(leftValue, rightValue.Int64)
    case ValueUint64:
      return intDivUint(leftValue, rightValue.Uint64)
    // case ValueFloat64:
    //   return intDivFloat(leftValue, rightValue.Float64)
    // case ValueDatetime:
    //   return intDivDatetime(leftValue, IntToDatetime(rightValue.Int64))
    // case ValueInterval:
    //   return intDivInterval(leftValue, IntToInterval(rightValue.Int64))
    default:
      return Null(), NewArithmeticError("div", leftValue.Type.String(), rightValue.Type.String())
    }
  }
}

func intDivInt(left Value, right int64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("div", left.Type.String(), "int")
	case ValueBool:
		return Null(), NewArithmeticError("div", left.Type.String(), "int")
	case ValueString:
		return Null(), NewArithmeticError("div", left.Type.String(), "int")
	case ValueInt64:
		return IntToValue(left.Int64 / right), nil
	case ValueUint64:
		if right < 0 {
			return IntToValue(- int64(left.Uint64/uint64(-right))), nil
		}
		return UintToValue(left.Uint64 / uint64(right)), nil
	// case ValueFloat64:
	// 	return FloatToValue(left.Float64 * float64(right)), nil
	}
	return Null(), NewArithmeticError("div", left.Type.String(), "int")
}

func intDivUint(left Value, right uint64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("div", left.Type.String(), "uint")
	case ValueBool:
		return Null(), NewArithmeticError("div", left.Type.String(), "uint")
	case ValueString:
		return Null(), NewArithmeticError("div", left.Type.String(), "uint")
	case ValueInt64:
		if left.Int64 < 0 {
			return IntToValue(- int64(uint64(left.Int64) / right)), nil
		}
		return UintToValue(uint64(left.Int64) / right), nil
	case ValueUint64:
		return UintToValue(left.Uint64 / right), nil
	// case ValueFloat64:
	// 	return FloatToValue(left.Float64 * float64(right)), nil
	default:
		return Null(), NewArithmeticError("div", left.Type.String(), "uint")
	}
}

// func intDivFloat(left Value, right float64) (Value, error) {
// 	switch left.Type {
// 	case ValueNull:
// 		return Null(), NewArithmeticError("div", left.Type.String(), "float")
// 	case ValueBool:
// 		return Null(), NewArithmeticError("div", left.Type.String(), "float")
// 	case ValueString:
// 		return Null(), NewArithmeticError("div", left.Type.String(), "float")
// 	case ValueInt64:	
// 		return FloatToValue(float64(left.Int64) * right), nil
// 	case ValueUint64:
// 		return FloatToValue(float64(left.Uint64) * right), nil
// 	case ValueFloat64:
// 		return FloatToValue(left.Float64 * right), nil
// 	default:
// 		return Null(), NewArithmeticError("div", left.Type.String(), "float")
// 	}
// }

// func intDivInterval(left Value, right time.Duration) (Value, error) {
// 	switch left.Type {
// 	case ValueDatetime:
// 		t := IntToDatetime(left.Int64)
// 		return DatetimeToValue(t.Add(-right)), nil
// 	case ValueInterval:
// 		t := IntToInterval(left.Int64)
// 		return IntervalToValue(t -right), nil
// 	default:
// 		return Null(), NewArithmeticError("div", left.Type.String(), "datetime")
// 	}
// }