package memcore

import (
	"encoding"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/vm"
)

type Value = vm.Value

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
		v.ToString(w)
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
			s.Values = append(s.Values, vm.Null())
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
		return vm.Null()
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

func (r *Record) EqualTo(to Record, opt vm.CompareOption) (bool, error) {
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
//  return r.marshalText()
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
		ok, _ := (*set)[idx].EqualTo(r, vm.EmptyCompareOption())
		if ok {
			return idx
		}
	}
	return -1
}

func (set *RecordSet) Has(r Record) bool {
	for _, a := range *set {
		ok, _ := a.EqualTo(r, vm.EmptyCompareOption())
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
		v, err := vm.ToValue(value)
		if err != nil {
			return table, errors.Wrap(err, "value '"+fmt.Sprint(value)+"' with index is '0' and column is '"+key+"' is invalid ")
		}
		record = append(record, v)
	}
	table.Records = append(table.Records, record)

	for i := 1; i < len(values); i++ {
		record = make([]Value, len(table.Columns))
		for key, value := range values[i] {
			v, err := vm.ToValue(value)
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
