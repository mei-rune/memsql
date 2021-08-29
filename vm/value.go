package vm

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

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
	ValueAny
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
	case ValueAny:
		return "any"
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
	// Bool    bool
	Str     string
	Int64   int64
	Uint64  uint64
	Float64 float64
	Any interface{}
}
func (v *Value) BoolValue() bool {
	return v.Int64 != 0
}
func (v Value) GoString() string {
	return v.String()
}
func (v *Value) String() string {
	switch v.Type {
	case ValueNull:
		return "null"
	case ValueBool:
		if v.BoolValue() {
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
	case ValueAny:
		bs, err := json.Marshal(v.Any)
		if err != nil {
			return "error_" +err.Error()
		}
		return string(bs)
	default:
		return "unknown_value_" + strconv.FormatInt(int64(v.Type), 10)
	}
}

func (v *Value) ToString(w io.Writer) {
	switch v.Type {
	case ValueNull:
		io.WriteString(w, "null")
	case ValueBool:
		if v.BoolValue() {
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
		io.WriteString(w, "\"interval ")
		io.WriteString(w, IntToDuration(v.Int64).String())
		io.WriteString(w, "\"")
	case ValueAny:
		err := json.NewEncoder(w).Encode(v.Any)
		if err != nil {
			io.WriteString(w, "\"error: ")
			io.WriteString(w, err.Error())
			io.WriteString(w, "\"")
		}
	default:
		io.WriteString(w, "\"")
		io.WriteString(w, "unknown_value_"+strconv.FormatInt(int64(v.Type), 10))
		io.WriteString(w, "\"")
	}
}

func (v *Value) AsInt(weak bool) (int64, error) {
	switch v.Type {
	case ValueString:
		if weak {
			return strconv.ParseInt(v.Str, 10, 64)
		}
	case ValueInt64:
		return v.Int64, nil
	case ValueUint64:
		return int64(v.Uint64), nil
	}
	return 0, NewTypeMismatch(v.Type.String(), "unknown")
}

func (v *Value) AsUint(weak bool) (uint64, error) {
	switch v.Type {
	case ValueString:
		if weak {
			return strconv.ParseUint(v.Str, 10, 64)
		}
	case ValueInt64:
		if v.Int64 < 0 {
			return 0, nil
		}
		return uint64(v.Int64), nil
	case ValueUint64:
		return v.Uint64, nil
	}
	return 0, NewTypeMismatch(v.Type.String(), "unknown")
}

func (v *Value) IsNil() bool {
	return v.Type == ValueNull
}
func (v *Value) IsNull() bool {
	return v.Type == ValueNull
}
func (v *Value) SetInt64(i64 int64) {
	v.Type = ValueInt64
	v.Int64 = i64
}
func (v *Value) SetUint64(u64 uint64) {
	v.Type = ValueUint64
	v.Uint64 = u64
}
func (v *Value) SetString(s string) {
	v.Type = ValueString
	v.Str = s
}
func (v *Value) SetBool(b bool) {
	v.Type = ValueBool
	if b {
		v.Int64 = 1
	} else {
		v.Int64 = 0
	}
}
func (v *Value) SetFloat64(f float64) {
	v.Type = ValueFloat64
	v.Float64 = f
}

type CompareOption struct {
	Weak       bool
	IgnoreCase bool
}

var emptyCompareOption = CompareOption{}

func EmptyCompareOption() CompareOption {
	return emptyCompareOption
}

func (v *Value) marshalText() ([]byte, error) {
	switch v.Type {
	case ValueNull:
		return []byte("null"), nil
	case ValueBool:
		if v.BoolValue() {
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
	case ValueAny:
		var buf bytes.Buffer
		err := json.NewEncoder(&buf).Encode(v.Any)
		return buf.Bytes(), err
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
		if v {
		return Value{
			Type: ValueBool,
			Int64: 1,
		}, nil
		}
		return Value{
			Type: ValueBool,
			Int64: 0,
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
	if value {
	return Value{
		Type: ValueBool,
		Int64: 1,
	}
	}
	return Value{
		Type: ValueBool,
		Int64: 0,
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

func AnyToValue(value interface{}) Value {
	return Value{
		Type:  ValueAny,
		Any: value,
	}
}

func ReadValueFromString(s string) Value {
	if strings.HasPrefix(s, "\"") {
		return StringToValue(s)
	}

	s = strings.ToLower(s)
	if s == "null" {
		return Null()
	}
	switch strings.ToLower(s) {
	case "true":
		return BoolToValue(true)
	case "false":
		return BoolToValue(false)
	}
	if strings.HasPrefix(s, "u") {
		u64, err := strconv.ParseUint(strings.TrimPrefix(s, "u"), 10, 64)
		if err == nil {
			return UintToValue(u64)
		}
		return StringToValue(s)
	}
	if strings.HasPrefix(s, "i") {
		i64, err := strconv.ParseInt(strings.TrimPrefix(s, "i"), 10, 64)
		if err == nil {
			return IntToValue(i64)
		}
		return StringToValue(s)
	}
	if strings.HasPrefix(s, "interval ") {
		s = strings.TrimPrefix(s, "interval ")
		interval, err := time.ParseDuration(s)
		if err == nil {
			return IntervalToValue(interval)
		}
		return StringToValue(s)
	}
	i64, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return IntToValue(i64)
	}

	u64, err := strconv.ParseUint(s, 10, 64)
	if err == nil {
		return UintToValue(u64)
	}

	f64, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return FloatToValue(f64)
	}

	for _, fmtstr := range []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05Z07:00",
		"2006/01/02 15:04:05",
	} {
		t, err := time.Parse(fmtstr, s)
		if err == nil {
			return DatetimeToValue(t)
		}
	}
	return StringToValue(s)
}
