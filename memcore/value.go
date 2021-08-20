package memcore

import (
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/runner-mei/errors"
)

type ValueType int

const (
	ValueNull ValueType = iota
	ValueBool
	ValueString
	ValueInt64
	ValueUint64
	ValueFloat64
	ValueDatetime
	ValueInterval
)

func (v ValueType) String() string {
	switch v {
	case ValueNull:
		return "null"
	case ValueBool:
		return "bool"
	case ValueString:
		return "string"
	case ValueInt64:
		return "int"
	case ValueUint64:
		return "uint"
	case ValueFloat64:
		return "float"
	case ValueDatetime:
		return "datetime"
	case ValueInterval:
		return "interval"
	default:
		return "unknown_" + strconv.FormatInt(int64(v), 10)
	}
}

var ErrUnknownValueType = errors.New("unknown value type")

type TypeError struct {
	Actual   string
	Excepted string
}

func (e *TypeError) Error() string {
	return "type erorr: want " + e.Excepted + " got " + e.Actual
}

func NewTypeError(r interface{}, actual, excepted string) error {
	return &TypeError{
		Actual:   actual,
		Excepted: excepted,
	}
}

func NewTypeMismatch(actual, excepted string) error {
	return &TypeError{
		Actual:   actual,
		Excepted: excepted,
	}
}

var TimeFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05.000Z07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

var TimeLocal = time.Local

func ToDatetime(s string) (time.Time, error) {
	for _, layout := range TimeFormats {
		m, e := time.ParseInLocation(layout, s, TimeLocal)
		if nil == e {
			return m, nil
		}
	}
	return time.Time{}, errors.New("invalid time: " + s)
}

func DatetimeToInt(t time.Time) int64 {
	return t.Unix()
}

func IntToDatetime(t int64) time.Time {
	return time.Unix(t, 0)
}

func DurationToInt(t time.Duration) int64 {
	return int64(t)
}

func IntToDuration(t int64) time.Duration {
	return time.Duration(t)
}

func IntervalToInt(t time.Duration) int64 {
	return int64(t)
}

func IntToInterval(t int64) time.Duration {
	return time.Duration(t)
}

type Value struct {
	Type    ValueType
	Bool    bool
	Str     string
	Int64   int64
	Uint64  uint64
	Float64 float64
}

func (v *Value) String() string {
	switch v.Type {
	case ValueNull:
		return "null"
	case ValueBool:
		if v.Bool {
			return "true"
		}
		return "false"
	case ValueString:
		return v.Str
	case ValueInt64:
		return strconv.FormatInt(v.Int64, 10)
	case ValueUint64:
		return strconv.FormatUint(v.Uint64, 10)
	case ValueFloat64:
		return strconv.FormatFloat(v.Float64, 'g', -1, 64)
	case ValueDatetime:
		return IntToDatetime(v.Int64).Format(time.RFC3339)
	case ValueInterval:
		return "interval " + time.Duration(v.Int64).String()
	default:
		return "unknown_value_" + strconv.FormatInt(int64(v.Type), 10)
	}
}

func (v *Value) IsNil() bool {
	return v.Type == ValueNull
}

type CompareOption struct {
	Weak       bool
	IgnoreCase bool
}

var emptyCompareOption = CompareOption{}

func (r *Value) EqualTo(to Value, opt CompareOption) (bool, error) {
	switch to.Type {
	case ValueNull:
		return r.IsNil(), nil
	case ValueBool:
		return r.EqualToBool(to.Bool, opt)
	case ValueString:
		return r.EqualToString(to.Str, opt)
	case ValueInt64:
		return r.EqualToInt64(to.Int64, opt)
	case ValueUint64:
		return r.EqualToUint64(to.Uint64, opt)
	case ValueFloat64:
		return r.EqualToFloat64(to.Float64, opt)
	case ValueDatetime:
		return r.EqualToDatetime(to.Int64, opt)
	case ValueInterval:
		return r.EqualToInterval(time.Duration(to.Int64), opt)
	default:
		return false, NewTypeMismatch(r.Type.String(), "unknown")
	}
}

func (r *Value) EqualToBool(to bool, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, nil
	case ValueBool:
		return r.Bool == to, nil
	case ValueString:
		if opt.Weak {
			switch r.Str {
			case "1", "t", "T", "true", "TRUE", "True":
				return to == true, nil
			case "0", "f", "F", "false", "FALSE", "False":
				return to == false, nil
			}
		}
	case ValueInt64:
		if opt.Weak {
			return (r.Int64 != 0) == to, nil
		}
	case ValueUint64:
		if opt.Weak {
			return (r.Uint64 != 0) == to, nil
		}
	}

	return false, NewTypeMismatch(r.Type.String(), "boolean")
}

func (r *Value) EqualToString(to string, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueBool:
		if opt.Weak {
			switch r.Str {
			case "1", "t", "T", "true", "TRUE", "True":
				return r.Bool == true, nil
			case "0", "f", "F", "false", "FALSE", "False":
				return r.Bool == false, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueString:
		if opt.IgnoreCase {
			return strings.EqualFold(r.Str, to), nil
		}
		return r.Str == to, nil
	case ValueInt64:
		if opt.Weak {
			i64, err := strconv.ParseInt(to, 10, 64)
			if err == nil {
				return r.Int64 == i64, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueUint64:
		if opt.Weak {
			u64, err := strconv.ParseUint(to, 10, 64)
			if err == nil {
				return r.Uint64 == u64, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueFloat64:
		return false, NewTypeMismatch(r.Type.String(), "string")
	default:
		return false, NewTypeMismatch(r.Type.String(), "string")
	}
}

func (r *Value) EqualToInt64(to int64, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, NewTypeMismatch(r.Type.String(), "int")
	case ValueBool:
		if opt.Weak {
			return (to != 0) == r.Bool, nil
		}
		return false, NewTypeMismatch(r.Type.String(), "int")
	case ValueString:
		if opt.Weak {
			i64, err := strconv.ParseInt(r.Str, 10, 64)
			if err == nil {
				return i64 == to, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "int")
	case ValueInt64:
		return r.Int64 == to, nil
	case ValueUint64:
		if to < 0 {
			return false, nil
		}
		return r.Uint64 == uint64(to), nil
	case ValueFloat64:
		return false, NewTypeMismatch(r.Type.String(), "int")
	default:
		return false, NewTypeMismatch(r.Type.String(), "int")
	}
}

func (r *Value) EqualToUint64(to uint64, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, NewTypeMismatch(r.Type.String(), "uint")
	case ValueBool:
		if opt.Weak {
			return (to != 0) == r.Bool, nil
		}
		return false, NewTypeMismatch(r.Type.String(), "uint")
	case ValueString:
		if opt.Weak {
			u64, err := strconv.ParseUint(r.Str, 10, 64)
			if err == nil {
				return u64 == to, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "uint")
	case ValueInt64:
		if r.Int64 < 0 {
			return false, nil
		}
		return uint64(r.Int64) == to, nil
	case ValueUint64:
		return r.Uint64 == to, nil
	case ValueFloat64:
		return false, NewTypeMismatch(r.Type.String(), "uint")
	default:
		return false, NewTypeMismatch(r.Type.String(), "uint")
	}
}

func (r *Value) EqualToFloat64(to float64, opt CompareOption) (bool, error) {
	return false, NewTypeMismatch(r.Type.String(), "float")
}

func (r *Value) EqualToDatetime(to int64, opt CompareOption) (bool, error) {
	if r.Type == ValueDatetime {
		return r.Int64 == to, nil
	}
	return false, NewTypeMismatch(r.Type.String(), "datetime")
}

func (r *Value) EqualToInterval(to time.Duration, opt CompareOption) (bool, error) {
	if r.Type == ValueInterval {
		return time.Duration(r.Int64) == to, nil
	}
	return false, NewTypeMismatch(r.Type.String(), "interval")
}

func (r *Value) CompareTo(to Value, opt CompareOption) (int, error) {
	switch to.Type {
	case ValueNull:
		if r.IsNil() {
			return 0, nil
		}
		return 1, nil
	case ValueBool:
		return r.CompareToBool(to.Bool, opt)
	case ValueString:
		return r.CompareToString(to.Str, opt)
	case ValueInt64:
		return r.CompareToInt64(to.Int64, opt)
	case ValueUint64:
		return r.CompareToUint64(to.Uint64, opt)
	case ValueFloat64:
		return r.CompareToFloat64(to.Float64, opt)
	case ValueDatetime:
		return r.CompareToDatetime(to.Int64, opt)
	case ValueInterval:
		return r.CompareToInterval(time.Duration(to.Int64), opt)
	default:
		return 0, ErrUnknownValueType
	}
}

func (r *Value) CompareToBool(to bool, opt CompareOption) (int, error) {
	if r.Type == ValueBool {
		if r.Bool == to {
			return 0, nil
		}
		if r.Bool {
			return 1, nil
		}
		return -1, nil
	}

	if opt.Weak {
		switch r.Type {
		case ValueString:
			switch r.Str {
			case "1", "t", "T", "true", "TRUE", "True":
				if to {
					return 0, nil
				}
				return -1, nil
			case "0", "f", "F", "false", "FALSE", "False":
				if to {
					return -1, nil
				}
				return 0, nil
			}
		case ValueInt64:
			if r.Int64 == 0 {
				if to {
					return -1, nil
				}
				return 0, nil
			}
			if to {
				return 0, nil
			}
			return -1, nil
		case ValueUint64:
			if r.Uint64 == 0 {
				if to {
					return -1, nil
				}
				return 0, nil
			}
			if to {
				return 0, nil
			}
			return -1, nil
		}
	}
	return 0, NewTypeError(r, r.Type.String(), "bool")
}

func (r *Value) CompareToString(to string, opt CompareOption) (int, error) {
	if r.Type == ValueString {
		if opt.IgnoreCase {
			if strings.EqualFold(r.Str, to) {
				return 0, nil
			}
			aS := strings.ToUpper(r.Str)
			toS := strings.ToUpper(to)

			if aS > toS {
				return 1, nil
			}
			return -1, nil
		}
		if r.Str == to {
			return 0, nil
		}
		if r.Str > to {
			return 1, nil
		}
		return -1, nil
	}

	if opt.Weak {
		switch r.Type {
		case ValueInt64:
			toI, err := strconv.ParseInt(to, 10, 64)
			if err != nil {
				return 0, NewTypeError(r, "string", "int")
			}
			if r.Int64 > toI {
				return 1, nil
			}
			if r.Int64 < toI {
				return -1, nil
			}
			return 0, nil
		case ValueUint64:
			toU, err := strconv.ParseUint(to, 10, 64)
			if err != nil {
				toI, err := strconv.ParseInt(to, 10, 64)
				if err != nil {
					return 0, NewTypeError(r, "string", "uint")
				}
				if toI < 0 {
					return 1, nil
				}
				toU = uint64(toI)
			}
			if r.Uint64 > toU {
				return 1, nil
			}
			if r.Uint64 < toU {
				return -1, nil
			}
			return 0, nil
		case ValueFloat64:
			toF, err := strconv.ParseFloat(to, 64)
			if err != nil {
				return 0, NewTypeError(r, "string", "uint")
			}
			if r.Float64 > toF {
				return 1, nil
			}
			return -1, nil
		}
	}
	return 0, NewTypeError(r, r.Type.String(), "string")
}

func compareToFloat64(a string, b float64, opt CompareOption, deferr error) (int, error) {
	aF, err := strconv.ParseFloat(a, 64)
	if err != nil {
		return 0, deferr
	}
	if aF > b {
		return 1, nil
	}
	return -1, nil
}

func (r *Value) CompareToInt64(to int64, opt CompareOption) (int, error) {
	switch r.Type {
	case ValueString:
		if opt.Weak {
			s := r.Str
			if strings.HasSuffix(s, ".0") {
				s = strings.TrimSuffix(s, ".0")
			} else if strings.HasSuffix(s, ".00") {
				s = strings.TrimSuffix(s, ".00")
			}
			aI, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return compareToFloat64(s, float64(to), opt, NewTypeError(r, "string", "int"))
			}
			if aI > to {
				return 1, nil
			}
			if aI < to {
				return -1, nil
			}
			return 0, nil
		}
	case ValueInt64:
		if r.Int64 > to {
			return 1, nil
		}
		if r.Int64 < to {
			return -1, nil
		}
		return 0, nil
	case ValueUint64:
		if to < 0 {
			return 1, nil
		}
		u := uint64(to)

		if r.Uint64 > u {
			return 1, nil
		}
		if r.Uint64 < u {
			return -1, nil
		}
		return 0, nil
	case ValueFloat64:
		u := float64(to)

		if r.Float64 > u {
			return 1, nil
		}
		return -1, nil
	}
	return 0, NewTypeError(r, r.Type.String(), "int")
}

func (r *Value) CompareToUint64(to uint64, opt CompareOption) (int, error) {
	switch r.Type {
	case ValueString:
		if opt.Weak {
			s := r.Str
			if strings.HasSuffix(s, ".0") {
				s = strings.TrimSuffix(s, ".0")
			} else if strings.HasSuffix(s, ".00") {
				s = strings.TrimSuffix(s, ".00")
			}
			aU, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				a, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return compareToFloat64(s, float64(to), opt, NewTypeError(r, "string", "uint"))
				}
				if a < 0 {
					return -1, nil
				}
				aU = uint64(a)
			}
			if aU > to {
				return 1, nil
			}
			if aU < to {
				return -1, nil
			}
			return 0, nil
		}
	case ValueInt64:
		if r.Int64 < 0 {
			return -1, nil
		}
		u := uint64(r.Int64)
		if u > to {
			return 1, nil
		}
		if u < to {
			return -1, nil
		}
		return 0, nil
	case ValueUint64:
		if r.Uint64 > to {
			return 1, nil
		}
		if r.Uint64 < to {
			return -1, nil
		}
		return 0, nil
	case ValueFloat64:
		u := float64(to)

		if r.Float64 > u {
			return 1, nil
		}
		return -1, nil
	}
	return 0, NewTypeError(r, r.Type.String(), "uint")
}

func (r *Value) CompareToFloat64(to float64, opt CompareOption) (int, error) {
	switch r.Type {
	case ValueString:
		if opt.Weak {
			return compareToFloat64(r.Str, to, opt, NewTypeError(r, "string", "int"))
		}
	case ValueInt64:
		u := float64(r.Int64)
		if u > to {
			return 1, nil
		}
		return -1, nil
	case ValueUint64:
		u := float64(r.Uint64)
		if u > to {
			return 1, nil
		}
		return -1, nil
	case ValueFloat64:
		if r.Float64 > to {
			return 1, nil
		}
		return -1, nil
	}
	return 0, NewTypeError(r, r.Type.String(), "float")
}

func (r *Value) CompareToDatetime(to int64, opt CompareOption) (int, error) {
	var value int64
	switch r.Type {
	case ValueDatetime:
		value = r.Int64
	case ValueString:
		if !opt.Weak {
			return 0, NewTypeError(r, r.Type.String(), "datetime")
		}
		t, err := ToDatetime(r.Str)
		if err != nil {
			return 0, NewTypeError(r, r.Type.String(), "datetime")
		}
		value = DatetimeToInt(t)
	case ValueInt64:
		if !opt.Weak {
			return 0, NewTypeError(r, r.Type.String(), "datetime")
		}
		value = r.Int64
	case ValueUint64:
		if !opt.Weak {
			return 0, NewTypeError(r, r.Type.String(), "datetime")
		}
		value = int64(r.Uint64)
	case ValueFloat64:
		if !opt.Weak {
			return 0, NewTypeError(r, r.Type.String(), "datetime")
		}
		value = int64(r.Float64)
	default:
		return 0, NewTypeError(r, r.Type.String(), "datetime")
	}

	if value > to {
		return 1, nil
	}
	if value < to {
		return -1, nil
	}
	return 0, nil
}


func (r *Value) CompareToInterval(to time.Duration, opt CompareOption) (int, error) {

	var value time.Duration
	switch r.Type {
	case ValueInterval:
		value = time.Duration(r.Int64)
	case ValueString:
		if !opt.Weak {
			return 0, NewTypeError(r, r.Type.String(), "interval")
		}
		t, err := time.ParseDuration(r.Str)
		if err != nil {
			return 0, NewTypeError(r, r.Type.String(), "interval")
		}
		value = t
	// case ValueInt64:
	// 	if !opt.Weak {
	// 		return 0, NewTypeError(r, r.Type.String(), "interval")
	// 	}
	// 	value = r.Int64
	// case ValueUint64:
	// 	if !opt.Weak {
	// 		return 0, NewTypeError(r, r.Type.String(), "interval")
	// 	}
	// 	value = int64(r.Uint64)
	// case ValueFloat64:
	// 	if !opt.Weak {
	// 		return 0, NewTypeError(r, r.Type.String(), "interval")
	// 	}
	// 	value = int64(r.Float64)
	default:
		return 0, NewTypeError(r, r.Type.String(), "interval")
	}

	if value > to {
		return 1, nil
	}
	if value < to {
		return -1, nil
	}
	return 0, nil
}


func (v *Value) marshalText() ([]byte, error) {
	switch v.Type {
	case ValueNull:
		return []byte("null"), nil
	case ValueBool:
		if v.Bool {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case ValueString:
		return json.Marshal(v.Str)
	case ValueInt64:
		return []byte(strconv.FormatInt(v.Int64, 10)), nil
	case ValueUint64:
		return []byte(strconv.FormatUint(v.Uint64, 10)), nil
	case ValueFloat64:
		return []byte(strconv.FormatFloat(v.Float64, 'g', -1, 64)), nil
	case ValueDatetime:
		return []byte("\"" + IntToDatetime(v.Int64).Format(time.RFC3339) + "\""), nil
	case ValueInterval:
		return []byte(strconv.FormatInt(v.Int64, 10)), nil
	default:
		return nil, ErrUnknownValueType
	}
}

func (v Value) MarshalText() ([]byte, error) {
	return v.marshalText()
}

var _ encoding.TextMarshaler = &Value{}

// func (v *Value) MarshalText() ( []byte,  error) {
// 	return v.marshalText()
// }

func Null() Value {
	return Value{Type: ValueNull}
}

func ToValue(value interface{}) (Value, error) {
	if value == nil {
		return Value{}, nil
	}
	switch v := value.(type) {
	case json.Number:
		i64, err := v.Int64()
		if err == nil {
			return Value{
				Type:  ValueInt64,
				Int64: i64,
			}, nil
		}
		u64, err := strconv.ParseUint(string(v), 10, 64)
		if err == nil {
			return Value{
				Type:   ValueUint64,
				Uint64: u64,
			}, nil
		}
		f64, err := v.Float64()
		if err == nil {
			return Value{
				Type:    ValueFloat64,
				Float64: f64,
			}, nil
		}
		return Value{}, err
	case string:
		return Value{
			Type: ValueString,
			Str:  v,
		}, nil
	case bool:
		return Value{
			Type: ValueBool,
			Bool: v,
		}, nil
	case int8:
		return Value{
			Type:  ValueInt64,
			Int64: int64(v),
		}, nil
	case int16:
		return Value{
			Type:  ValueInt64,
			Int64: int64(v),
		}, nil
	case int32:
		return Value{
			Type:  ValueInt64,
			Int64: int64(v),
		}, nil
	case int64:
		return Value{
			Type:  ValueInt64,
			Int64: v,
		}, nil
	case int:
		return Value{
			Type:  ValueInt64,
			Int64: int64(v),
		}, nil
	case uint8:
		return Value{
			Type:   ValueUint64,
			Uint64: uint64(v),
		}, nil
	case uint16:
		return Value{
			Type:   ValueUint64,
			Uint64: uint64(v),
		}, nil
	case uint32:
		return Value{
			Type:   ValueUint64,
			Uint64: uint64(v),
		}, nil
	case uint64:
		return Value{
			Type:   ValueUint64,
			Uint64: v,
		}, nil
	case uint:
		return Value{
			Type:   ValueUint64,
			Uint64: uint64(v),
		}, nil
	case float32:
		return Value{
			Type:    ValueFloat64,
			Float64: float64(v),
		}, nil
	case float64:
		return Value{
			Type:    ValueFloat64,
			Float64: float64(v),
		}, nil
	case time.Time:
		return Value{
			Type:  ValueDatetime,
			Int64: DatetimeToInt(v),
		}, nil
	case time.Duration:
		return Value{
			Type:  ValueInterval,
			Int64: int64(v),
		}, nil
	case Value:
		return v, nil
	}
	return Value{}, fmt.Errorf("Unknown type %T: %v", value, value)
}

func MustToValue(value interface{}) Value {
	v, err := ToValue(value)
	if err != nil {
		panic(err)
	}
	return v
}

func BoolToValue(value bool) Value {
	return Value{
		Type:  ValueBool,
		Bool: value,
	}
}

func IntToValue(value int64) Value {
	return Value{
		Type:  ValueInt64,
		Int64: value,
	}
}

func UintToValue(value uint64) Value {
	return Value{
		Type:   ValueUint64,
		Uint64: value,
	}
}

func FloatToValue(value float64) Value {
	return Value{
		Type:    ValueFloat64,
		Float64: value,
	}
}

func StringToValue(value string) Value {
	return Value{
		Type: ValueString,
		Str:  value,
	}
}

func StringAsNumber(s string) (Value, error) {
	i64, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return IntToValue(i64), nil
	}
	u64, err := strconv.ParseUint(s, 10, 64)
	if err == nil {
		return UintToValue(u64), nil
	}
	f64, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return FloatToValue(f64), nil
	}
	return Value{}, NewTypeError(s, "string", "number")
}

func DatetimeToValue(value time.Time) Value {
	return Value{
		Type:  ValueDatetime,
		Int64: DatetimeToInt(value),
	}
}

func IntervalToValue(value time.Duration) Value {
	return Value{
		Type:  ValueInterval,
		Int64: int64(value),
	}
}

type Column struct {
	Name string
}

func columnSearch(columns []Column, column Column) int {
	return columnSearchByName(columns, column.Name)
}

func columnSearchByName(columns []Column, column string) int {
	for idx := range columns {
		if columns[idx].Name == column {
			return idx
		}
	}
	return -1
}

type Record struct {
	Columns []Column
	Values  []Value
}

func (r *Record) ToLine(w io.Writer, sep string) {
	for idx, v := range r.Values {
		if idx != 0 {
			io.WriteString(w, sep)
		}
		switch v.Type {
		case ValueNull:
			io.WriteString(w, "null")
		case ValueBool:
			if v.Bool {
				io.WriteString(w, "true")
			} else {
				io.WriteString(w, "false")
			}
		case ValueString:
			bs, err := json.Marshal(v.Str)
			if err != nil {
				panic(err)
			}
			w.Write(bs)
		case ValueInt64:
			io.WriteString(w, strconv.FormatInt(v.Int64, 10))
		case ValueUint64:
			io.WriteString(w, strconv.FormatUint(v.Uint64, 10))
		case ValueFloat64:
			io.WriteString(w, strconv.FormatFloat(v.Float64, 'g', -1, 64))
		case ValueDatetime:
			io.WriteString(w, "\"")
			io.WriteString(w, IntToDatetime(v.Int64).Format(time.RFC3339))
			io.WriteString(w, "\"")
		case ValueInterval:
			io.WriteString(w, strconv.FormatInt(v.Int64, 10))
		default:
			io.WriteString(w, "\"")
			io.WriteString(w, "unknown_value_"+strconv.FormatInt(int64(v.Type), 10))
			io.WriteString(w, "\"")
		}
	}
}

type colunmSorter Record

func (s colunmSorter) Len() int {
	return len(s.Columns)
}

func (s colunmSorter) Swap(i, j int) {
	s.Columns[i], s.Columns[j] = s.Columns[j], s.Columns[i]
	if len(s.Values) != len(s.Columns) {
		for i := 0; i < len(s.Columns)-len(s.Values); i++ {
			s.Values = append(s.Values, Null())
		}
	}

	s.Values[i], s.Values[j] = s.Values[j], s.Values[i]
}

func (s colunmSorter) Less(i, j int) bool {
	return s.Columns[i].Name < s.Columns[j].Name
}

func SortByColumnName(r Record) Record {
	r = r.Clone()
	sort.Sort(colunmSorter(r))
	return r
}

func (r *Record) Clone() Record {
	columns := make([]Column, len(r.Columns))
	copy(columns, r.Columns)
	values := make([]Value, len(r.Values))
	copy(values, r.Values)

	return Record{
		Columns: columns,
		Values:  values,
	}
}

func (r *Record) Search(name string) int {
	return columnSearchByName(r.Columns, name)
}

func (r *Record) At(idx int) Value {
	if len(r.Values) >= idx {
		// Columns 和 Values 的长度不一定一致, 看下面的 ToTable
		return Null()
	}

	return r.Values[idx]
}

func (r *Record) Get(name string) (Value, bool) {
	idx := columnSearchByName(r.Columns, name)
	if idx < 0 {
		return Value{}, false
	}
	return r.Values[idx], true
}

func (r *Record) IsEmpty() bool {
	return len(r.Values) == 0
}

func (r *Record) EqualTo(to Record, opt CompareOption) (bool, error) {
	if len(r.Columns) != len(to.Columns) {
		return false, nil
	}
	for idx := range r.Columns {
		var toIdx = idx
		if r.Columns[idx].Name != to.Columns[idx].Name {
			toIdx = columnSearch(to.Columns, r.Columns[idx])
			if toIdx < 0 {
				return false, nil
			}
		}

		result, err := r.Values[idx].EqualTo(to.Values[toIdx], opt)
		if err != nil {
			return false, errors.Wrap(err, "column '"+r.Columns[idx].Name+"' is type mismatch")
		}
		if !result {
			return false, nil
		}
	}

	return true, nil
}

func (r *Record) marshalText() ([]byte, error) {
	var buf = make([]byte, 0, 256)
	buf = append(buf, '{')
	isFirst := true
	for idx := range r.Columns {
		if r.Values[idx].IsNil() {
			continue
		}

		if isFirst {
			isFirst = false
		} else {
			buf = append(buf, ',')
		}
		buf = append(buf, '"')
		buf = append(buf, r.Columns[idx].Name...)
		buf = append(buf, '"')
		buf = append(buf, ':')

		bs, err := r.Values[idx].MarshalText()
		if err != nil {
			return nil, err
		}
		buf = append(buf, bs...)
	}

	buf = append(buf, '}')
	return buf, nil
}

func (r Record) MarshalText() ([]byte, error) {
	return r.marshalText()
}

var _ encoding.TextMarshaler = &Record{}

type recordValuer Record

func (r *recordValuer) GetValue(tableName, name string) (Value, error) {
	value, ok := (*Record)(r).Get(name)
	if ok {
		return value, nil
	}
	if tableName == "" {
		return Value{}, ColumnNotFound(name)
	}
	return Value{}, ColumnNotFound(tableName + "." + name)
}

var _ GetValuer = (*recordValuer)(nil)

func ToRecordValuer(r *Record) GetValuer {
	return (*recordValuer)(r)
}

// func (r *Record) MarshalText() ( []byte,  error) {
// 	return r.marshalText()
// }

type RecordSet []Record

func (set *RecordSet) Add(r Record) {
	if set.Has(r) {
		return
	}
	*set = append(*set, r)
}

func (set *RecordSet) Delete(idx int) {
	tmp := []Record(*set)
	copy(tmp[idx:], tmp[idx+1:])
	*set = tmp[:len(tmp)-1]
}

func (set *RecordSet) Search(r Record) int {
	for idx := range *set {
		ok, _ := (*set)[idx].EqualTo(r, emptyCompareOption)
		if ok {
			return idx
		}
	}
	return -1
}

func (set *RecordSet) Has(r Record) bool {
	for _, a := range *set {
		ok, _ := a.EqualTo(r, emptyCompareOption)
		if ok {
			return true
		}
	}
	return false
}

type Table struct {
	Columns []Column
	Records [][]Value
}

func (table *Table) Length() int {
	return len(table.Records)
}

func (table *Table) At(idx int) Record {
	return Record{
		Columns: table.Columns,
		Values:  table.Records[idx],
	}
}

func ToTable(values []map[string]interface{}) (Table, error) {
	if len(values) == 0 {
		return Table{}, nil
	}
	var table = Table{}
	var record []Value
	for key, value := range values[0] {
		table.Columns = append(table.Columns, Column{Name: key})
		v, err := ToValue(value)
		if err != nil {
			return table, errors.Wrap(err, "value '"+fmt.Sprint(value)+"' with index is '0' and column is '"+key+"' is invalid ")
		}
		record = append(record, v)
	}
	table.Records = append(table.Records, record)

	for i := 1; i < len(values); i++ {
		record = make([]Value, len(table.Columns))
		for key, value := range values[i] {
			v, err := ToValue(value)
			if err != nil {
				return table, errors.Wrap(err, "value '"+fmt.Sprint(value)+"' with index is '"+strconv.Itoa(i)+"' and column is '"+key+"' is invalid ")
			}
			foundIndex := columnSearchByName(table.Columns, key)
			if foundIndex < 0 {
				// 这里添加了一列，那么前几行的列值的数目会少于 Columns
				table.Columns = append(table.Columns, Column{Name: key})
				record = append(record, v)
			} else {
				record[foundIndex] = v
			}
		}
		table.Records = append(table.Records, record)
	}
	return table, nil
}
