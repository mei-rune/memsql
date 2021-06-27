package memsql

import (
	"encoding"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"fmt"
)

type ValueType int

const (
	ValueNil ValueType = iota
	ValueBool
	ValueString
	ValueInt64
	ValueUint64
	ValueFloat64
	ValueDatetime
)


func (v ValueType) String() string {
	switch v{
	case ValueNil:
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
	default: 
		return "unknown_" + strconv.FormatInt(int64(v), 10) 
	}
}

var ErrUnknownValueType = errors.New("unknown value type")

type TypeError struct {
	Actual string
	Excepted string
}

func (e *TypeError) Error() string {
	return "type erorr: want "+e.Excepted+" got " + e.Actual
}

func NewTypeError(r interface{}, actual, excepted string) error {
	return &TypeError{
		Actual: actual,
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
	return time.Time{}, errors.New("invalid time: "+s)
}

func datetimeToInt(t time.Time) int64 {
	return t.Unix()
}

func intToDatetime(t  int64) time.Time {
	return time.Unix(t, 0)
}

type Value struct {
	Type    ValueType
	Bool    bool
	Str  string
	Int64   int64
	Uint64  uint64
	Float64 float64
}

func (v *Value) String() string {
	switch v.Type {
	case ValueNil:
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
		return intToDatetime(v.Int64).Format(time.RFC3339)
	default:
		return "unknown_value_" + strconv.FormatInt(int64(v.Type), 10) 
	}
}

func (v *Value) IsNil() bool {
	return v.Type == ValueNil
}

type CompareOption struct {
	Weak bool
	IgnoreCase bool
}

var emptyCompareOption = CompareOption{}

func (r *Value) EqualTo(to Value, opt CompareOption) bool {
	switch to.Type {
	case ValueNil:
		return r.IsNil()
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
	default:
		return false
	}
}

func (r *Value) EqualToBool(to bool, opt CompareOption) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		return r.Bool == to
	case ValueString:
		if opt.Weak {
			switch r.Str {
			case "1", "t", "T", "true", "TRUE", "True":
				return to == true
			case "0", "f", "F", "false", "FALSE", "False":
				return to == false
			}
		}
	case ValueInt64:
		if opt.Weak {
			return (r.Int64 != 0) == to
		} 
	case ValueUint64:
		if opt.Weak {
			return (r.Uint64 != 0) == to
		}
	}

	return false
}

func (r *Value) EqualToString(to string, opt CompareOption) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		if opt.Weak {
			switch r.Str {
			case "1", "t", "T", "true", "TRUE", "True":
				return r.Bool == true
			case "0", "f", "F", "false", "FALSE", "False":
				return r.Bool == false
			}
		}
		return false
	case ValueString:
		if opt.IgnoreCase {
			return strings.EqualFold(r.Str, to)
		}
		return r.Str == to
	case ValueInt64:
		if opt.Weak {
			i64, err := strconv.ParseInt(to, 10, 64)
			if err == nil {
				return r.Int64 == i64
			} 
		}
		return false
	case ValueUint64:
		if opt.Weak {
			u64, err := strconv.ParseUint(to, 10, 64)
			if err == nil {
				return r.Uint64 == u64
			}
		}
		return false
	case ValueFloat64:
		return false
	default:
		return false
	}
}

func (r *Value) EqualToInt64(to int64, opt CompareOption) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		if opt.Weak {
			return  (to != 0) == r.Bool
		}
		return false
	case ValueString:
		if opt.Weak {
			i64, err := strconv.ParseInt(r.Str, 10, 64)
			if err == nil {
				return i64 == to
			} 
		}
		return false
	case ValueInt64:
		return r.Int64 == to
	case ValueUint64:
		if to < 0 {
			return false
		}
		return r.Uint64 == uint64(to)
	case ValueFloat64:
		return false
	default:
		return false
	}
}

func (r *Value) EqualToUint64(to uint64, opt CompareOption) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		if opt.Weak {
			return  (to != 0) == r.Bool
		}
		return false
	case ValueString:
		if opt.Weak {
			u64, err := strconv.ParseUint(r.Str, 10, 64)
			if err == nil {
				return u64 == to
			} 
		}
		return false
	case ValueInt64:
		if r.Int64 < 0 {
			return false
		}
		return uint64(r.Int64) == to
	case ValueUint64:
		return r.Uint64 == to
	case ValueFloat64:
		return false
	default:
		return false
	}
}

func (r *Value) EqualToFloat64(to float64, opt CompareOption) bool {
	return false
}

func (r *Value) EqualToDatetime(to int64, opt CompareOption) bool {
	if r.Type == ValueDatetime {
		return r.Int64 == to
	}
	return false
}

func (r *Value) CompareTo(to Value, opt CompareOption) (int, error) {
	switch to.Type {
	case ValueNil:
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
	default:
		return 0, ErrUnknownValueType
	}
}


func (r *Value) CompareToBool(to bool, opt CompareOption) (int, error) {
	if r.Type == ValueBool {
		if  r.Bool == to {
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
				if  to {
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
		if  r.Str == to {
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
		value = datetimeToInt(t)
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

func (v *Value) marshalText() ([]byte, error) {
	switch v.Type {
	case ValueNil:
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
		return []byte("\""+ intToDatetime(v.Int64).Format(time.RFC3339) + "\""), nil
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


func ToValue(value interface{}) (Value, error) {
	if value == nil {
		return Value{}, nil
	}
	switch v := value.(type) {
	case json.Number:
		i64, err := v.Int64()
		if err == nil {
			return Value{
				Type: ValueInt64,
				Int64: i64,
			}, nil
		}
		u64, err := strconv.ParseUint(string(v), 10, 64)
		if err == nil {
			return Value{
				Type: ValueUint64,
				Uint64: u64,
			}, nil
		}
		f64, err := v.Float64()
		if err == nil {
			return Value{
				Type: ValueFloat64,
				Float64: f64,
			}, nil
		}
		return Value{}, err
	case string:
		return Value{
			Type: ValueString,
			Str: v,
		}, nil
	case bool:
		return Value{
			Type: ValueBool,
			Bool: v,
		}, nil
	case int8:
		return Value{
			Type: ValueInt64,
			Int64: int64(v),
		}, nil
	case int16:
		return Value{
			Type: ValueInt64,
			Int64: int64(v),
		}, nil
	case int32:
		return Value{
			Type: ValueInt64,
			Int64: int64(v),
		}, nil
	case int64:
		return Value{
			Type: ValueInt64,
			Int64: v,
		}, nil
	case int:
		return Value{
			Type: ValueInt64,
			Int64: int64(v),
		}, nil
	case uint8:
		return Value{
			Type: ValueUint64,
			Uint64: uint64(v),
		}, nil
	case uint16:
		return Value{
			Type: ValueUint64,
			Uint64: uint64(v),
		}, nil
	case uint32:
		return Value{
			Type: ValueUint64,
			Uint64: uint64(v),
		}, nil
	case uint64:
		return Value{
			Type: ValueUint64,
			Uint64: v,
		}, nil
	case uint:
		return Value{
			Type: ValueUint64,
			Uint64: uint64(v),
		}, nil
	case float32:
		return Value{
			Type: ValueFloat64,
			Float64: float64(v),
		}, nil
	case float64:
		return Value{
			Type: ValueFloat64,
			Float64: float64(v),
		}, nil
	case time.Time:
		return Value{
			Type: ValueDatetime,
			Int64: datetimeToInt(v),
		}, nil
	}
	return Value{}, fmt.Errorf("Unknown type %T: %v", value, value)
}

func MustToValue(value interface{}) (Value) {
	v, err := ToValue(value)
	if err != nil {
		panic(err)
	}
	return v
}

func IntToValue(value int64) Value {
		return Value{
			Type: ValueInt64,
			Int64: value,
		}
}

func UintToValue(value uint64) Value {
	return Value{
		Type: ValueUint64,
		Uint64: value,
	}
}

func FloatToValue(value float64) Value {
	return Value{
		Type: ValueFloat64,
		Float64: value,
	}
}

func BoolToValue(value bool) Value {
	return Value{
		Type: ValueBool,
		Bool: value,
	}
}

func StringToValue(value string) Value {
	return Value{
		Type: ValueString,
		Str: value,
	}
}

func DatetimeToValue(value time.Time) Value {
	return Value{
		Type: ValueDatetime,
		Int64: datetimeToInt(value),
	}
}

type Column struct {
	Name string
}

func columnSearch(columns []Column, column Column) int {
	for idx := range columns {
		if columns[idx].Name == column.Name {
			return idx
		}
	}
	return -1
}

type Record struct {
	Columns []Column
	Values  []Value
}

func (r *Record) IsEmpty() bool {
	return len(r.Values) == 0
}

func (r *Record) EqualTo(to Record, opt CompareOption) bool {
	if len(r.Columns) != len(to.Columns) {
		return false
	}
	for idx := range r.Columns {
		var toIdx = idx
		if r.Columns[idx].Name != to.Columns[idx].Name {
			toIdx = columnSearch(to.Columns, r.Columns[idx])
			if toIdx < 0 {
				return false
			}
		}

		if !r.Values[idx].EqualTo(to.Values[toIdx], opt) {
			return false
		}
	}

	return true
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
		if (*set)[idx].EqualTo(r, emptyCompareOption) {
			return idx
		}
	}
	return -1
}

func (set *RecordSet) Has(r Record) bool {
	for _, a := range *set {
		if a.EqualTo(r, emptyCompareOption) {
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
