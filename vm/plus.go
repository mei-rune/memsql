package vm

import (
	"time"

	"github.com/runner-mei/errors"
)

func NewArithmeticError(op, left, right string) error {
	return errors.New("cloudn't '" + left + "' " + op + " '" + right + "'")
}

func PlusFunc(left, right func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return Null(), err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return Null(), err
		}

		return Plus(leftValue, rightValue)
	}
}

func Plus(leftValue, rightValue Value) (Value, error) {
	switch rightValue.Type {
	case ValueNull:
		return Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
	case ValueBool:
		return Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
	case ValueString:
		return Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
	case ValueInt64:
		return plusInt(leftValue, rightValue.Int64)
	case ValueUint64:
		return plusUint(leftValue, rightValue.Uint64)
	case ValueFloat64:
		return plusFloat(leftValue, rightValue.Float64)
	case ValueDatetime:
		return plusDatetime(leftValue, IntToDatetime(rightValue.Int64))
	case ValueInterval:
		return plusInterval(leftValue, IntToInterval(rightValue.Int64))
	default:
		return Null(), NewArithmeticError("+", leftValue.Type.String(), rightValue.Type.String())
	}
}

func plusInt(left Value, right int64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("+", left.Type.String(), "int")
	case ValueBool:
		return Null(), NewArithmeticError("+", left.Type.String(), "int")
	case ValueString:
		return Null(), NewArithmeticError("+", left.Type.String(), "int")
	case ValueInt64:
		return IntToValue(left.Int64 + right), nil
	case ValueUint64:
		if right < 0 {
			u64 := uint64(-right)
			if left.Uint64 < u64 {
				return IntToValue(right + int64(left.Uint64)), nil
			}
			return UintToValue(left.Uint64 - u64), nil
		}
		return UintToValue(left.Uint64 + uint64(right)), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 + float64(right)), nil
	default:
		return Null(), NewArithmeticError("+", left.Type.String(), "int")
	}
}

func plusUint(left Value, right uint64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("+", left.Type.String(), "uint")
	case ValueBool:
		return Null(), NewArithmeticError("+", left.Type.String(), "uint")
	case ValueString:
		return Null(), NewArithmeticError("+", left.Type.String(), "uint")
	case ValueInt64:
		if left.Int64 < 0 {
			u64 := uint64(-left.Int64)
			if u64 > right {
				return IntToValue(left.Int64 + int64(right)), nil
			}
			return UintToValue(right - u64), nil
		}
		return IntToValue(left.Int64 + int64(right)), nil
	case ValueUint64:
		return UintToValue(left.Uint64 + right), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 + float64(right)), nil
	default:
		return Null(), NewArithmeticError("+", left.Type.String(), "uint")
	}
}

func plusFloat(left Value, right float64) (Value, error) {
	switch left.Type {
	case ValueNull:
		return Null(), NewArithmeticError("+", left.Type.String(), "float")
	case ValueBool:
		return Null(), NewArithmeticError("+", left.Type.String(), "float")
	case ValueString:
		return Null(), NewArithmeticError("+", left.Type.String(), "float")
	case ValueInt64:
		return FloatToValue(float64(left.Int64) + right), nil
	case ValueUint64:
		return FloatToValue(float64(left.Uint64) + right), nil
	case ValueFloat64:
		return FloatToValue(left.Float64 + float64(right)), nil
	default:
		return Null(), NewArithmeticError("+", left.Type.String(), "float")
	}
}

func plusDatetime(left Value, right time.Time) (Value, error) {
	if left.Type != ValueInterval {
		return Null(), NewArithmeticError("+", left.Type.String(), "datetime")
	}

	return DatetimeToValue(right.Add(IntToInterval(left.Int64))), nil
}

func plusInterval(left Value, right time.Duration) (Value, error) {
	if left.Type != ValueDatetime {
		return Null(), NewArithmeticError("+", left.Type.String(), "datetime")
	}

	t := IntToDatetime(left.Int64)
	return DatetimeToValue(t.Add(right)), nil
}
