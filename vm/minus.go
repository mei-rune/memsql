package vm

import (
	"time"
)

func MinusFunc(left, right func(Context) (Value, error)) func(Context) (Value, error) {
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
      return Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    case ValueBool:
      return Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    case ValueString:
      return Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    case ValueInt64:
      return minusInt(leftValue, rightValue.Int64)
    case ValueUint64:
      return minusUint(leftValue, rightValue.Uint64)
    case ValueFloat64:
      return minusFloat(leftValue, rightValue.Float64)
    case ValueDatetime:
      return minusDatetime(leftValue, IntToDatetime(rightValue.Int64))
    case ValueInterval:
      return minusInterval(leftValue, IntToInterval(rightValue.Int64))
    default:
      return Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    }
  }
}

func minusInt(left Value, right int64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("-", left.Type.String(), "int")
	case ValueBool:
		return Null(), NewArithmeticError("-", left.Type.String(), "int")
	case ValueString:
		return Null(), NewArithmeticError("-", left.Type.String(), "int")
	case ValueInt64:
		return IntToValue(left.Int64 - right), nil
	case ValueUint64:
		if right < 0 {
			u64 :=  uint64(-right)
			return UintToValue(left.Uint64 + u64), nil
		}
		return UintToValue(left.Uint64 + uint64(right)), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 - float64(right)), nil
	default:
		return Null(), NewArithmeticError("-", left.Type.String(), "int")
	}
}

func minusUint(left Value, right uint64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("-", left.Type.String(), "uint")
	case ValueBool:
		return Null(), NewArithmeticError("-", left.Type.String(), "uint")
	case ValueString:
		return Null(), NewArithmeticError("-", left.Type.String(), "uint")
	case ValueInt64:
		if left.Int64 < 0 {
			return IntToValue(left.Int64 - int64(right)), nil
		}
		return IntToValue(left.Int64 - int64(right)), nil
	case ValueUint64:
		if left.Uint64 > right {
			return UintToValue(left.Uint64 - right), nil
		}
		return IntToValue( - int64(right - left.Uint64)), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 - float64(right)), nil
	default:
		return Null(), NewArithmeticError("-", left.Type.String(), "uint")
	}
}

func minusFloat(left Value, right float64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("-", left.Type.String(), "float")
	case ValueBool:
		return Null(), NewArithmeticError("-", left.Type.String(), "float")
	case ValueString:
		return Null(), NewArithmeticError("-", left.Type.String(), "float")
	case ValueInt64:	
		return FloatToValue(float64(left.Int64) - right), nil
	case ValueUint64:
		return FloatToValue(float64(left.Uint64) - right), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 - right), nil
	default:
		return Null(), NewArithmeticError("-", left.Type.String(), "float")
	}
}

func minusDatetime(left Value, right time.Time) (Value, error) {
	if left.Type != ValueDatetime {
		return Null(), NewArithmeticError("-", left.Type.String(), "datetime")
	}

	t := IntToDatetime(left.Int64)
	return IntervalToValue(t.Sub(right)), nil
}

func minusInterval(left Value, right time.Duration) (Value, error) {
	switch left.Type {
	case ValueDatetime:
		t := IntToDatetime(left.Int64)
		return DatetimeToValue(t.Add(-right)), nil
	case ValueInterval:
		t := IntToInterval(left.Int64)
		return IntervalToValue(t -right), nil
	default:
		return Null(), NewArithmeticError("-", left.Type.String(), "datetime")
	}
}