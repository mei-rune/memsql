package records

import (
	"fmt"
	"strconv"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/vm"
)

type Table struct {
	Columns []Column
	Records [][]Value
}

func (table *Table) ForEach(fn func(columns []Column, record []Value)) {
	for _, values := range table.Records {
		fn(table.Columns, values)
	}
}

func (table *Table) Add(columns []Column, values []Value) {
	if table.Columns == nil {
		table.Columns = make([]Column, len(columns))
		copy(table.Columns, columns)

		copyedValues := make([]Value, len(values))
		copy(copyedValues, values)

		table.Records = append(table.Records, copyedValues)
		return
	}

	record := make([]Value, len(table.Columns))
	for idx, value := range values {
		foundIndex := columnSearchByName(table.Columns, columns[idx].Name)
		if foundIndex < 0 {
			// 这里添加了一列，那么前几行的列值的数目会少于 Columns
			table.Columns = append(table.Columns, Column{Name: columns[idx].Name})
			record = append(record, value)
		} else {
			record[foundIndex] = value
		}
	}
	table.Records = append(table.Records, record)
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

type TableAlias struct {
	Name  string
	Alias string
}

func (a TableAlias) Equal(name string) bool {
	return a.Name == name || a.Alias == name
}

type TableName struct {
	Table string
	Tags  KeyValues
}

func (tn TableName) String() string {
	if len(tn.Tags) > 0 {
		return tn.Table + "(" + tn.Tags.ToKey() + ")"
	}
	return tn.Table
}

func ToMap(columns []Column, record []Value) map[string]interface{} {
	values := map[string]interface{} {}
	for idx, v := range record {
		if len(columns) <= idx {
			break
		}
		values[columns[idx].Name] = v.ToInterface()
	}
	return values
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
