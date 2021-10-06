package memcore

import (
	"encoding"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/vm"
)

const TagIndexStart = 100000

type Value = vm.Value

type Column struct {
	TableName string
	TableAs   string
	Name      string
}

func mkColumn(name string) Column {
	return Column{Name: name}
}

func columnSearch(columns []Column, column Column) int {
	if column.TableName == "" && column.TableAs == "" {
		return columnSearchByName(columns, column.Name)
	}
	for idx := range columns {
		if columns[idx].TableName != column.TableName && columns[idx].TableAs != column.TableAs {
			continue
		}
		if columns[idx].Name == column.Name {
			return idx
		}
	}
	return -1
}

func columnSearchByQualifierName(columns []Column, tableAs, column string) int {
	if tableAs == "" {
		return columnSearchByName(columns, column)
	}

	for idx := range columns {
		if columns[idx].TableName != tableAs && columns[idx].TableAs != tableAs {
			continue
		}

		if columns[idx].Name == column {
			return idx
		}
	}
	return -1
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
	Tags    KeyValues
	Columns []Column
	Values  []Value
}

func (r Record) GoString() string {
	bs, _ := r.marshalText(true)
	return string(bs)
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

	tags := make([]KeyValue, len(r.Tags))
	copy(tags, r.Tags)

	return Record{
		Tags:    tags,
		Columns: columns,
		Values:  values,
	}
}

func (r *Record) Search(name string) int {
	idx := columnSearchByName(r.Columns, name)
	if idx < 0 {
		if strings.HasPrefix(name, "@") {
			name = strings.TrimPrefix(name, "@")
		}
		for idx := range r.Tags {
			if r.Tags[idx].Key == name {
				return TagIndexStart + idx
			}
		}
	}

	return -1
}

func (r *Record) At(idx int) Value {
	if len(r.Values) > idx {
		return r.Values[idx]
	}

	if idx >= TagIndexStart {
		idx = idx - TagIndexStart
		if len(r.Tags) < idx {
			return vm.StringToValue(r.Tags[idx].Value)
		}
	}
	// Columns 和 Values 的长度不一定一致, 看下面的 ToTable
	return vm.Null()
}

func (r *Record) Get(name string) (Value, bool) {
	idx := columnSearchByName(r.Columns, name)
	if idx >= 0 {
		if len(r.Values) <= idx {
			// Columns 和 Values 的长度不一定一致, 看下面的 ToTable
			return vm.Null(), true
		}
		return r.Values[idx], true
	}

	if strings.HasPrefix(name, "@") {
		name = strings.TrimPrefix(name, "@")
	}
	for idx := range r.Tags {
		if r.Tags[idx].Key == name {
			return vm.StringToValue(r.Tags[idx].Value), true
		}
	}
	return vm.Null(), false
}

func (r *Record) GetByQualifierName(tableAs, name string) (Value, bool) {
	idx := columnSearchByQualifierName(r.Columns, tableAs, name)
	if idx >= 0 {
		if len(r.Values) <= idx {
			// Columns 和 Values 的长度不一定一致, 看下面的 ToTable
			return vm.Null(), true
		}
		return r.Values[idx], true
	}

	if strings.HasPrefix(name, "@") {
		name = strings.TrimPrefix(name, "@")
	}
	for idx := range r.Tags {
		if r.Tags[idx].Key == name {
			return vm.StringToValue(r.Tags[idx].Value), true
		}
	}
	return vm.Null(), false
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

func (r *Record) marshalText(withTable bool) ([]byte, error) {
	var buf = make([]byte, 0, 256)
	buf = append(buf, '{')
	isFirst := true

	for idx := range r.Tags {
		if isFirst {
			isFirst = false
		} else {
			buf = append(buf, ',')
		}

		buf = append(buf, '"')
		buf = append(buf, r.Tags[idx].Key...)
		buf = append(buf, '"')
		buf = append(buf, ':')
		buf = append(buf, '"')
		buf = append(buf, r.Tags[idx].Value...)
		buf = append(buf, '"')
	}
	for idx := range r.Values {
		if r.Values[idx].IsNil() {
			continue
		}

		if isFirst {
			isFirst = false
		} else {
			buf = append(buf, ',')
		}
		buf = append(buf, '"')
		if withTable {
			if r.Columns[idx].TableName != "" {
				buf = append(buf, r.Columns[idx].TableName...)
				buf = append(buf, '.')
				if r.Columns[idx].TableAs != "" {
					buf = append(buf, r.Columns[idx].TableAs...)
					buf = append(buf, '.')
				}
			} else if r.Columns[idx].TableAs != "" {
				buf = append(buf, r.Columns[idx].TableAs...)
				buf = append(buf, '.')
			}
		}
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
	return r.marshalText(false)
}

func RenameTableToAlias(alias string) func(Context, Record) (Record, error) {
	return func(ctx Context, r Record) (Record, error) {
		columns := make([]Column, len(r.Columns))
		copy(columns, r.Columns)
		for idx := range columns {
			columns[idx].TableAs = alias
		}
		return Record{
			Tags:    r.Tags,
			Columns: columns,
			Values:  r.Values,
		}, nil
	}
}

var _ encoding.TextMarshaler = &Record{}

type recordValuer Record

func (r *recordValuer) GetValue(tableName, name string) (Value, error) {
	value, ok := (*Record)(r).Get(name)
	if ok {
		return value, nil
	}
	return vm.Null(), ColumnNotFound(tableName, name)
}

type recordValuerByQualifierName Record

func (r *recordValuerByQualifierName) GetValue(tableName, name string) (Value, error) {
	value, ok := (*Record)(r).GetByQualifierName(tableName, name)
	if ok {
		return value, nil
	}
	return vm.Null(), ColumnNotFound(tableName, name)
}

func ToRecordValuer(r *Record, withQualifier bool) GetValuer {
	if withQualifier {
		return (*recordValuerByQualifierName)(r)
	}
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

func MergeRecord(outerAs string, outer Record, innerAs string, inner Record) Record {
	// Columns 和 Values 并不一定数目相等
	result := Record{
		Columns: make([]Column, len(outer.Tags)+len(outer.Columns)+len(inner.Tags)+len(inner.Columns)),
		Values:  make([]Value, len(outer.Tags)+len(outer.Columns)+len(inner.Tags)+len(inner.Columns)),
	}

	if len(outer.Tags) > 0 {
		for idx := range outer.Tags {
			if len(outer.Columns) > 0 {
				result.Columns[idx].TableName = outer.Columns[0].TableName
				result.Columns[idx].TableAs = outer.Columns[0].TableAs
			}
			if outerAs != "" {
				result.Columns[idx].TableAs = outerAs
			}
			result.Columns[idx].Name = outer.Tags[idx].Key
		}
	}
	copy(result.Columns[len(outer.Tags):], outer.Columns)
	if outerAs != "" {
		for idx := range outer.Columns {
			result.Columns[len(outer.Tags)+idx].TableAs = outerAs
		}
	}

	if len(inner.Tags) > 0 {
		for idx := range inner.Tags {
			if len(inner.Columns) > 0 {
				result.Columns[len(outer.Tags)+len(outer.Columns)+idx].TableName = inner.Columns[0].TableName
				result.Columns[len(outer.Tags)+len(outer.Columns)+idx].TableAs = inner.Columns[0].TableAs
			}
			if innerAs != "" {
				result.Columns[len(outer.Tags)+len(outer.Columns)+idx].TableAs = innerAs
			}
			result.Columns[len(outer.Tags)+len(outer.Columns)+idx].Name = inner.Tags[idx].Key
		}
	}
	copy(result.Columns[len(outer.Tags)+len(outer.Columns)+len(inner.Tags):], inner.Columns)
	if innerAs != "" {
		for idx := range inner.Columns {
			result.Columns[len(outer.Tags)+len(outer.Columns)+len(inner.Tags)+idx].TableAs = innerAs
		}
	}

	// Columns 和 Values 并不一定数目相等
	for idx := range outer.Tags {
		result.Values[idx] = vm.StringToValue(outer.Tags[idx].Value)
	}
	copy(result.Values[len(outer.Tags):], outer.Values)

	for idx := range inner.Tags {
		result.Values[len(outer.Tags)+len(outer.Columns)+idx] = vm.StringToValue(inner.Tags[idx].Value)
	}
	copy(result.Values[len(outer.Tags)+len(outer.Columns)+len(inner.Tags):], inner.Values)

	// fmt.Println("=========")
	// fmt.Println(outer.GoString())
	// fmt.Println(inner.GoString())
	// fmt.Println(result.GoString())
	return result
}
