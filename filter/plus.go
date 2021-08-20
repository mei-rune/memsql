package filter

import (
	"time"
	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore"
)

func NewArithmeticError(op, left, right string) error {
	return errors.New("cloudn't '" + left + "' " + op + " '" + right + "'")
}

func Plus(left, right func(Context) (memcore.Value, error)) func(Context) (memcore.Value, error) {
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
      return memcore.Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
    case memcore.ValueBool:
      return memcore.Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
    case memcore.ValueString:
      return memcore.Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
    case memcore.ValueInt64:
      return plusInt(leftValue, rightValue.Int64)
    case memcore.ValueUint64:
      return plusUint(leftValue, rightValue.Uint64)
    case memcore.ValueFloat64:
      return plusFloat(leftValue, rightValue.Float64)
    case memcore.ValueDatetime:
      return plusDatetime(leftValue, memcore.IntToDatetime(rightValue.Int64))
    case memcore.ValueInterval:
      return plusInterval(leftValue, memcore.IntToInterval(rightValue.Int64))
    default:
      return memcore.Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
    }
  }
}

func plusInt(left memcore.Value, right int64) (memcore.Value, error) {
	switch left.Type {
	case memcore.ValueNull:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "int")
	case memcore.ValueBool:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "int")
	case memcore.ValueString:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "int")
	case memcore.ValueInt64:
		return memcore.IntToValue(left.Int64 + right), nil
	case memcore.ValueUint64:
		if right < 0 {
			u64 :=  uint64(-right)
			if left.Uint64 < u64 {
				return memcore.IntToValue(right + int64(left.Uint64)), nil
			}
			return memcore.UintToValue(left.Uint64 - u64), nil
		}
		return memcore.UintToValue(left.Uint64 + uint64(right)), nil
	case memcore.ValueFloat64:
		return memcore.FloatToValue(left.Float64 + float64(right)), nil
	default:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "int")
	}
}

func plusUint(left memcore.Value, right uint64) (memcore.Value, error) {
	switch left.Type {
	case memcore.ValueNull:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "uint")
	case memcore.ValueBool:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "uint")
	case memcore.ValueString:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "uint")
	case memcore.ValueInt64:
		if left.Int64 < 0 {
			u64 :=  uint64(-left.Int64)
			if u64 > right {
				return memcore.IntToValue(left.Int64 + int64(right)), nil
			}
			return memcore.UintToValue(right - u64), nil
		}
		return memcore.IntToValue(left.Int64 + int64(right)), nil
	case memcore.ValueUint64:
		return memcore.UintToValue(left.Uint64 + right), nil
	case memcore.ValueFloat64:
		return memcore.FloatToValue(left.Float64 + float64(right)), nil
	default:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "uint")
	}
}

func plusFloat(left memcore.Value, right float64) (memcore.Value, error) {
	switch left.Type {
	case memcore.ValueNull:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "float")
	case memcore.ValueBool:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "float")
	case memcore.ValueString:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "float")
	case memcore.ValueInt64:	
		return memcore.FloatToValue(float64(left.Int64) + right), nil
	case memcore.ValueUint64:
		return memcore.FloatToValue(float64(left.Uint64) + right), nil
	case memcore.ValueFloat64:
		return memcore.FloatToValue(left.Float64 + float64(right)), nil
	default:
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "float")
	}
}

func plusDatetime(left memcore.Value, right time.Time) (memcore.Value, error) {
	if left.Type != memcore.ValueInterval {
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "datetime")
	}

	return memcore.DatetimeToValue(right.Add(memcore.IntToInterval(left.Int64))), nil
}

func plusInterval(left memcore.Value, right time.Duration) (memcore.Value, error) {
	if left.Type != memcore.ValueDatetime {
		return memcore.Null(), NewArithmeticError("+", left.Type.String(), "datetime")
	}

	t := memcore.IntToDatetime(left.Int64)
	return memcore.DatetimeToValue(t.Add(right)), nil
}