package memsql

import (
	"encoding"
	"encoding/json"
	"errors"
	"strconv"
)

type ValueType int

const (
	ValueNil ValueType = iota
	ValueBool
	ValueString
	ValueInt64
	ValueUint64
	ValueFloat64
)

var ErrUnknownValueType = errors.New("unknown value type")

type Value struct {
	Type    ValueType
	Bool    bool
	String  string
	Int64   int64
	Uint64  uint64
	Float64 float64
}

func (v *Value) IsNil() bool {
	return v.Type == ValueNil
}

func (r *Value) EqualTo(to Value) bool {
	switch to.Type {
	case ValueNil:
		return to.IsNil()
	case ValueBool:
		return r.EqualToBool(to.Bool)
	case ValueString:
		return r.EqualToString(to.String)
	case ValueInt64:
		return r.EqualToInt64(to.Int64)
	case ValueUint64:
		return r.EqualToUint64(to.Uint64)
	case ValueFloat64:
		return r.EqualToFloat64(to.Float64)
	default:
		return false
	}
}

func (r *Value) EqualToBool(to bool) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		return r.Bool == to
	case ValueString:
		return false
	case ValueInt64:
		return false
	case ValueUint64:
		return false
	case ValueFloat64:
		return false
	default:
		return false
	}
}

func (r *Value) EqualToString(to string) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		return false
	case ValueString:
		return r.String == to
	case ValueInt64:
		return false
	case ValueUint64:
		return false
	case ValueFloat64:
		return false
	default:
		return false
	}
}

func (r *Value) EqualToInt64(to int64) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		return false
	case ValueString:
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

func (r *Value) EqualToUint64(to uint64) bool {
	switch r.Type {
	case ValueNil:
		return false
	case ValueBool:
		return false
	case ValueString:
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

func (r *Value) EqualToFloat64(to float64) bool {
	return false
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
		return json.Marshal(v.String)
	case ValueInt64:
		return []byte(strconv.FormatInt(v.Int64, 10)), nil
	case ValueUint64:
		return []byte(strconv.FormatUint(v.Uint64, 10)), nil
	case ValueFloat64:
		return []byte(strconv.FormatFloat(v.Float64, 'g', -1, 64)), nil
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

func (r *Record) EqualTo(to Record) bool {
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

		if !r.Values[idx].EqualTo(to.Values[toIdx]) {
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
		if (*set)[idx].EqualTo(r) {
			return idx
		}
	}
	return -1
}

func (set *RecordSet) Has(r Record) bool {
	for _, a := range *set {
		if a.EqualTo(r) {
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
