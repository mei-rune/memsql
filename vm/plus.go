package vm

import (
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
