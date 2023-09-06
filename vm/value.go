package vm

import (
	"time"

	"github.com/mei-rune/luluo"
	"github.com/runner-mei/errors"
)

var ErrNotFound = errors.ErrNotFound

type GetValuer interface {
	GetValue(tableName, name string) (Value, error)
}

type GetValueFunc func(tableName, name string) (Value, error)

func (f GetValueFunc) GetValue(tableName, name string) (Value, error) {
	return f(tableName, name)
}

type ValueType = luluo.ValueType

const (
	ValueNull     = luluo.ValueNull
	ValueBool     = luluo.ValueBool
	ValueString   = luluo.ValueString
	ValueInt64    = luluo.ValueInt64
	ValueUint64   = luluo.ValueUint64
	ValueFloat64  = luluo.ValueFloat64
	ValueDatetime = luluo.ValueDatetime
	ValueInterval = luluo.ValueInterval
	ValueAny      = luluo.ValueAny
)

var ErrUnknownValueType = luluo.ErrUnknownValueType

type TypeError = luluo.TypeError

func NewTypeError(r interface{}, actual, expected string) error {
	return luluo.NewTypeError(r, actual, expected)
}

func NewTypeMismatch(actual, expected string) error {
	return luluo.NewTypeMismatch(actual, expected)
}

func ToDatetime(s string) (time.Time, error) {
	return luluo.ToDatetime(s)
}

func ToDatetimeValue(s string) (Value, error) {
	return luluo.ToDatetimeValue(s)
}

func DatetimeToInt(t time.Time) int64 {
	return luluo.DatetimeToInt(t)
}

func IntToDatetime(t int64) time.Time {
	return luluo.IntToDatetime(t)
}

func DurationToInt(t time.Duration) int64 {
	return luluo.DurationToInt(t)
}

func IntToDuration(t int64) time.Duration {
	return luluo.IntToDuration(t)
}

func IntervalToInt(t time.Duration) int64 {
	return luluo.IntervalToInt(t)
}

func IntToInterval(t int64) time.Duration {
	return luluo.IntToInterval(t)
}

type Value = luluo.Value

type CompareOption = luluo.CompareOption

func EmptyCompareOption() CompareOption {
	return luluo.EmptyCompareOption()
}

// func (v *Value) MarshalText() ( []byte,  error) {
// 	return v.marshalText()
// }

func Null() Value {
	return luluo.Null()
}

func ToValue(value interface{}) (Value, error) {
	return luluo.ToValue(value)
}

func MustToValue(value interface{}) Value {
	return luluo.MustToValue(value)
}

func BoolToValue(value bool) Value {
	return luluo.BoolToValue(value)
}

func IntToValue(value int64) Value {
	return luluo.IntToValue(value)
}

func UintToValue(value uint64) Value {
	return luluo.UintToValue(value)
}

func FloatToValue(value float64) Value {
	return luluo.FloatToValue(value)
}

func StringToValue(value string) Value {
	return luluo.StringToValue(value)
}

func StringAsNumber(s string) (Value, error) {
	return luluo.StringAsNumber(s)
}

func DatetimeToValue(value time.Time) Value {
	return luluo.DatetimeToValue(value)
}

func IntervalToValue(value time.Duration) Value {
	return luluo.IntervalToValue(value)
}

func DurationToValue(value time.Duration) Value {
	return luluo.DurationToValue(value)
}

func AnyToValue(value interface{}) Value {
	return luluo.AnyToValue(value)
}

func ReadValueFromString(s string) Value {
	return luluo.ReadValueFromString(s)
}

// func NewArithmeticError(op, left, right string) error {
// 	return luluo.NewArithmeticError(op, left, right)
// }

func Plus(leftValue, rightValue Value) (Value, error) {
	return luluo.Plus(leftValue, rightValue)
}

func Mult(leftValue, rightValue Value) (Value, error) {
	return luluo.Mult(leftValue, rightValue)
}

func Mod(leftValue, rightValue Value) (Value, error) {
	return luluo.Mod(leftValue, rightValue)
}

func Minus(leftValue, rightValue Value) (Value, error) {
	return luluo.Minus(leftValue, rightValue)
}

func Uminus(value Value) (Value, error) {
	return luluo.Uminus(value)
}

func IntDiv(leftValue, rightValue Value) (Value, error) {
	return luluo.IntDiv(leftValue, rightValue)
}

func Div(leftValue, rightValue Value) (Value, error) {
	return luluo.Div(leftValue, rightValue)
}

func DivInt(leftValue Value, rightValue int64) (Value, error) {
	return luluo.DivInt(leftValue, rightValue)
}

func DivUint(leftValue Value, rightValue uint64) (Value, error) {
	return luluo.DivUint(leftValue, rightValue)
}
