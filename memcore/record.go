package memcore

import (
	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore/records"
	"github.com/runner-mei/memsql/vm"
)


var Wrap = errors.Wrap
var ErrNotFound = records.ErrNotFound

type Value = records.Value
type GetValuer = vm.GetValuer
type Record = records.Record
type RecordSet = records.RecordSet
type Context = interface{}
type Table = records.Table
type TableAlias = records.TableAlias
type TableName = records.TableName
type Column = records.Column
type KeyValue = records.KeyValue
type KeyValues = records.KeyValues


func mkColumn(name string) Column {
	return records.MkColumn(name)
}

type Measurement = records.Measurement
type Storage = records.Storage


func NewStorage() Storage {
	return records.NewStorage()
}

func ToTable(values []map[string]interface{}) (Table, error) {
	return records.ToTable(values)
}

func MergeRecord(outerAs string, outer Record, innerAs string, inner Record) Record {
	return records.MergeRecord(outerAs, outer, innerAs, inner)
}

func TagNotFound(tableName, tagName string) error {
	return records.TagNotFound(tableName, tagName)
}

func ColumnNotFound(tableName, columnName string) error {
	return records.ColumnNotFound(tableName, columnName)
}

func TableNotExists(table string, err ...error) error {
	return records.TableNotExists(table, err...)
}

func MapToTags(tags map[string]string) []KeyValue {
	return records.MapToTags(tags)
}

func SortByColumnName(r Record) Record {
	return records.SortByColumnName(r)
}

func FromStorage(s Storage, tablename string, f func(name TableName) (bool, error), trace func(TableName)) (Query, error) {
	list, err := s.From(tablename, f)
	if err != nil {
		return Query{}, err
	}
	if len(list) == 0 {
		return Query{}, TableNotExists(tablename)
	}
	if trace != nil {
		for i := 0; i < len(list); i++ {
			trace(list[i].Name)
		}
	}

	query := FromWithTags(list[0].Data, list[0].Name.Tags)
	for i := 1; i < len(list); i++ {
		query = query.UnionAll(FromWithTags(list[i].Data, list[i].Name.Tags))
	}
	return query, nil
}