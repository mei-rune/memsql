package filter

import (
	"time"
	"github.com/runner-mei/memsql/memcore"
)

func Minus(left, right func(Context) (memcore.Value, error)) func(Context) (memcore.Value, error) {
  return func(ctx Context) (memcore.Value, error) {
    leftValue, err := left(ctx)
    if err != nil {
      return memcore.Null(), err
    }
    rightValue, err := right(ctx)
    if err != nil {
      return memcore.Null(), err
    }

    switch rightValue.Type {
    case memcore.ValueNull:
      return memcore.Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    case memcore.ValueBool:
      return memcore.Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    case memcore.ValueString:
      return memcore.Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    case memcore.ValueInt64:
      return minusInt(leftValue, rightValue.Int64)
    case memcore.ValueUint64:
      return minusUint(leftValue, rightValue.Uint64)
    case memcore.ValueFloat64:
      return minusFloat(leftValue, rightValue.Float64)
    case memcore.ValueDatetime:
      return minusDatetime(leftValue, memcore.IntToDatetime(rightValue.Int64))
    case memcore.ValueInterval:
      return minusInterval(leftValue, memcore.IntToInterval(rightValue.Int64))
    default:
      return memcore.Null(), NewArithmeticError("-", leftValue.Type.String(), rightValue.Type.String())
    }
  }
}

func minusInt(left memcore.Value, right int64) (memcore.Value, error) {
	switch left.Type {
	case memcore.ValueNull:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "int")
	case memcore.ValueBool:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "int")
	case memcore.ValueString:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "int")
	case memcore.ValueInt64:
		return memcore.IntToValue(left.Int64 - right), nil
	case memcore.ValueUint64:
		if right < 0 {
			u64 :=  uint64(-right)
			return memcore.UintToValue(left.Uint64 + u64), nil
		}
		return memcore.UintToValue(left.Uint64 + uint64(right)), nil
	case memcore.ValueFloat64:
		return memcore.FloatToValue(left.Float64 - float64(right)), nil
	default:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "int")
	}
}

func minusUint(left memcore.Value, right uint64) (memcore.Value, error) {
	switch left.Type {
	case memcore.ValueNull:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "uint")
	case memcore.ValueBool:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "uint")
	case memcore.ValueString:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "uint")
	case memcore.ValueInt64:
		if left.Int64 < 0 {
			return memcore.IntToValue(left.Int64 - int64(right)), nil
		}
		return memcore.IntToValue(left.Int64 - int64(right)), nil
	case memcore.ValueUint64:
		if left.Uint64 > right {
			return memcore.UintToValue(left.Uint64 - right), nil
		}
		return memcore.IntToValue( - int64(right - left.Uint64)), nil
	case memcore.ValueFloat64:
		return memcore.FloatToValue(left.Float64 - float64(right)), nil
	default:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "uint")
	}
}

func minusFloat(left memcore.Value, right float64) (memcore.Value, error) {
	switch left.Type {
	case memcore.ValueNull:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "float")
	case memcore.ValueBool:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "float")
	case memcore.ValueString:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "float")
	case memcore.ValueInt64:	
		return memcore.FloatToValue(float64(left.Int64) - right), nil
	case memcore.ValueUint64:
		return memcore.FloatToValue(float64(left.Uint64) - right), nil
	case memcore.ValueFloat64:
		return memcore.FloatToValue(left.Float64 - right), nil
	default:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "float")
	}
}

func minusDatetime(left memcore.Value, right time.Time) (memcore.Value, error) {
	if left.Type != memcore.ValueDatetime {
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "datetime")
	}

	t := memcore.IntToDatetime(left.Int64)
	return memcore.IntervalToValue(t.Sub(right)), nil
}

func minusInterval(left memcore.Value, right time.Duration) (memcore.Value, error) {
	switch left.Type {
	case memcore.ValueDatetime:
		t := memcore.IntToDatetime(left.Int64)
		return memcore.DatetimeToValue(t.Add(-right)), nil
	case memcore.ValueInterval:
		t := memcore.IntToInterval(left.Int64)
		return memcore.IntervalToValue(t -right), nil
	default:
		return memcore.Null(), NewArithmeticError("-", left.Type.String(), "datetime")
	}
}