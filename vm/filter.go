package vm

import (
	"regexp"
	"strings"

	"github.com/runner-mei/errors"
)

type Context = GetValuer

func And(left, right func(Context) (bool, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		ok, err := left(ctx)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}

		return right(ctx)
	}
}

func Or(left, right func(Context) (bool, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		ok, err := left(ctx)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		return right(ctx)
	}
}

func Not(f func(Context) (bool, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		ok, err := f(ctx)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}
}

func Equal(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}

		return leftValue.EqualTo(rightValue, EmptyCompareOption())
	}
}

func LessThan(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}
		result, err := leftValue.CompareTo(rightValue, EmptyCompareOption())
		if err != nil {
			return false, err
		}
		return result < 0, nil
	}
}

func GreaterThan(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}

		result, err := leftValue.CompareTo(rightValue, EmptyCompareOption())
		if err != nil {
			return false, err
		}
		return result > 0, nil
	}
}

func LessEqual(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}

		result, err := leftValue.CompareTo(rightValue, EmptyCompareOption())
		if err != nil {
			return false, err
		}
		return result <= 0, nil
	}
}

func GreaterEqual(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}

		result, err := leftValue.CompareTo(rightValue, EmptyCompareOption())
		if err != nil {
			return false, err
		}
		return result >= 0, nil
	}
}

func NotEqual(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}

		result, err := leftValue.EqualTo(rightValue, EmptyCompareOption())
		if err != nil {
			return false, err
		}

		return !result, nil
	}
}

func In(left func(Context) (Value, error), right func(Context) ([]Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValues, err := right(ctx)
		if err != nil {
			return false, err
		}
		for _, value := range rightValues {
			result, err := value.EqualTo(leftValue, EmptyCompareOption())
			if err != nil {
				return false, err
			}
			if result {
				return true, nil
			}
		}
		return false, nil
	}
}

func NotIn(left func(Context) (Value, error), right func(Context) ([]Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValues, err := right(ctx)
		if err != nil {
			return false, err
		}
		for _, value := range rightValues {
			result, err := value.EqualTo(leftValue, EmptyCompareOption())
			if err != nil {
				return false, err
			}
			if result {
				return false, nil
			}
		}
		return true, nil
	}
}

func Like(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}
		if leftValue.Type != ValueString {
			return false, errors.Wrap(err, "left operant isnot string")
		}
		leftStr := leftValue.Str

		if rightValue.Type != ValueString {
			return false, errors.Wrap(err, "right operant isnot string")
		}
		rightStr := rightValue.Str

		if strings.HasPrefix(rightStr, "%") {
			if strings.HasSuffix(rightStr, "%") {
				s := strings.TrimPrefix(rightStr, "%")
				s = strings.TrimSuffix(s, "%")
				return strings.Contains(leftStr, s), nil
			}
			return strings.HasSuffix(leftStr, strings.TrimPrefix(rightStr, "%")), nil
		}
		if strings.HasSuffix(rightStr, "%") {
			return strings.HasPrefix(leftStr, strings.TrimSuffix(rightStr, "%")), nil
		}
		return leftStr == rightStr, nil
	}
}

func NotLike(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return Not(Like(left, right))
}

func Regexp(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		rightValue, err := right(ctx)
		if err != nil {
			return false, err
		}
		if leftValue.Type != ValueString {
			return false, errors.Wrap(err, "left operant isnot string")
		}
		leftStr := leftValue.Str

		if rightValue.Type != ValueString {
			return false, errors.Wrap(err, "right operant isnot string")
		}
		rightStr := rightValue.Str

		return regexp.MatchString(rightStr, leftStr)
	}
}

func NotRegexp(left, right func(Context) (Value, error)) func(Context) (bool, error) {
	return Not(Regexp(left, right))
}

func Between(left, from, to func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		leftValue, err := left(ctx)
		if err != nil {
			return false, err
		}
		fromValue, err := from(ctx)
		if err != nil {
			return false, err
		}
		toValue, err := to(ctx)
		if err != nil {
			return false, err
		}

		result, err := leftValue.CompareTo(fromValue, EmptyCompareOption())
		if err != nil {
			return false, err
		}
		if result < 0 {
			return false, nil
		}
		result, err = leftValue.CompareTo(toValue, EmptyCompareOption())
		if err != nil {
			return false, err
		}
		return result <= 0, nil
	}
}

func NotBetween(left, from, to func(Context) (Value, error)) func(Context) (bool, error) {
	return Not(Between(left, from, to))
}

func IsNull(value func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		v, err := value(ctx)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return true, nil
			}

			return false, err
		}
		return v.IsNil(), nil
	}
}

func IsNotNull(value func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		v, err := value(ctx)
		if err != nil {
			return false, err
		}
		return !v.IsNil(), nil
	}
}

func IsTrue(value func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		v, err := value(ctx)
		if err != nil {
			return false, err
		}

		if v.Type != ValueBool {
			return false, NewTypeError(v.String(), v.Type.String(), "boolean")
		}
		return v.BoolValue(), nil
	}
}

func IsNotTrue(value func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		v, err := value(ctx)
		if err != nil {
			return false, err
		}

		if v.Type != ValueBool {
			return false, NewTypeError(v.String(), v.Type.String(), "boolean")
		}
		return !v.BoolValue(), nil
	}
}

func IsFalse(value func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		v, err := value(ctx)
		if err != nil {
			return false, err
		}

		if v.Type != ValueBool {
			return false, NewTypeError(v.String(), v.Type.String(), "boolean")
		}
		return !v.BoolValue(), nil
	}
}

func IsNotFalse(value func(Context) (Value, error)) func(Context) (bool, error) {
	return func(ctx Context) (bool, error) {
		v, err := value(ctx)
		if err != nil {
			return false, err
		}

		if v.Type != ValueBool {
			return false, NewTypeError(v.String(), v.Type.String(), "boolean")
		}
		return v.BoolValue(), nil
	}
}
