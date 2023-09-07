package records

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/vm"
)

var ErrNotFound = vm.ErrNotFound

func TableNotExists(table string, err ...error) error {
	if len(err) > 0 && err[0] != nil {
		return errors.WithTitle(errors.ErrTableNotExists, "table '"+table+"' isnot exists: "+err[0].Error())
	}
	return errors.WithTitle(errors.ErrTableNotExists, "table '"+table+"' isnot exists")
}

func ColumnNotFound(tableName, columnName string) error {
	if tableName != "" {
		return errors.WithTitle(ErrNotFound, "column '"+tableName+"."+columnName+"' isnot found")
	}
	return errors.WithTitle(ErrNotFound, "column '"+columnName+"' isnot found")
}

func TagNotFound(tableName, tagName string) error {
	if tableName == "" {
		return errors.WithTitle(ErrNotFound, "tag '"+tagName+"' isnot found")
	}
	return errors.WithTitle(ErrNotFound, "tag '"+tableName+"."+tagName+"' isnot found")
}

// type Context interface{}

// type GetValuer = vm.GetValuer
// type GetValueFunc = vm.GetValueFunc

type KeyValue struct {
	Key   string
	Value string
}

func MapToTags(tags map[string]string) []KeyValue {
	results := make([]KeyValue, 0, len(tags))

	for key, value := range tags {
		results = append(results, KeyValue{
			Key:   key,
			Value: value,
		})
	}
	return results
}

type KeyValues []KeyValue

func (kvs KeyValues) Get(key string) (string, bool) {
	for idx := range kvs {
		if kvs[idx].Key == key {
			return kvs[idx].Value, true
		}
	}
	return "", false
}

func (kvs KeyValues) Equal(to KeyValues) bool {
	if len(kvs) != len(to) {
		return false
	}
	for idx := range kvs {
		value, ok := to.Get(kvs[idx].Key)
		if !ok {
			return false
		}
		if value != kvs[idx].Value {
			return false
		}
	}
	return true
}

func (kvs KeyValues) Len() int {
	return len(kvs)
}
func (kvs KeyValues) Less(i, j int) bool {
	return kvs[i].Key < kvs[j].Key
}
func (kvs KeyValues) Swap(i, j int) {
	tmp := kvs[i]
	kvs[i] = kvs[j]
	kvs[j] = tmp
}

func CloneKeyValues(keyValues []KeyValue) []KeyValue {
	if len(keyValues) == 0 {
		return nil
	}

	results := make([]KeyValue, len(keyValues))
	for idx := range keyValues {
		results[idx] = keyValues[idx]
	}
	return results
}

func (kvs KeyValues) ToKey() string {
	var sb strings.Builder
	for idx := range kvs {
		if idx > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(kvs[idx].Key)
		sb.WriteString("=")
		sb.WriteString(kvs[idx].Value)
	}
	return sb.String()
}

type Measurement struct {
	Name TableName
	Time time.Time
	Data Table

	ErrTime time.Time
	Err     error
}

// func toGetValuer(tags KeyValues) GetValuer {
// 	return GetValueFunc(func(tableName, name string) (Value, error) {
// 		tagName := name
// 		if strings.HasPrefix(tagName, "@") {
// 			tagName = strings.TrimPrefix(tagName, "@")
// 		}
// 		value, ok := tags.Get(tagName)
// 		if ok {
// 			return vm.StringToValue(value), nil
// 		}
// 		return vm.Null(), TagNotFound(tableName, name)
// 	})
// }

type Storage interface {
	From(tablename string, filter func(name TableName) (bool, error)) ([]Measurement, error)
	Set(name string, tags []KeyValue, t time.Time, table Table, err error) error
	Exists(name string, tags []KeyValue, predateLimit time.Time) bool
}

type storage struct {
	mu           sync.Mutex
	measurements map[string]map[string]Measurement
}

func NewStorage() Storage {
	return &storage{
		measurements: map[string]map[string]Measurement{},
	}
}

func (s *storage) From(tablename string, filter func(name TableName) (bool, error)) ([]Measurement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.measurements[tablename]
	if len(byKey) == 0 {
		return nil, TableNotExists(tablename)
	}

	var list []Measurement
	for _, m := range byKey {
		ok, err := filter(m.Name)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}

			return nil, TableNotExists(tablename, err)
		}
		if m.Err != nil {
			return nil, m.Err
		}
		if ok {
			list = append(list, m)
		}
	}
	if len(list) == 0 {
		return nil, TableNotExists(tablename)
	}
	return list, nil
}

// func (s *storage) From(ctx Context, tablename string, filter func(ctx GetValuer) (bool, error), trace func(TableName)) (Query, error) {
// 	return Query{
// 		Iterate: func() Iterator {
// 			q, err := s.from(ctx, tablename, filter, trace)
// 			if err != nil {
// 				return func(ctx Context) (Record, error) {
// 					return Record{}, err
// 				}
// 			}
// 			return q.Iterate()
// 		},
// 	}, nil
// }

// func (s *storage) from(ctx Context, tablename string, filter func(ctx GetValuer) (bool, error), trace func(TableName)) (Query, error) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	byKey := s.measurements[tablename]
// 	if len(byKey) == 0 {
// 		return Query{}, TableNotExists(tablename)
// 	}

// 	var list []measurement
// 	for _, m := range byKey {
// 		values := toGetValuer(m.tags)
// 		ok, err := filter(values)
// 		if err != nil {
// 			if errors.Is(err, ErrNotFound) {
// 				continue
// 			}

// 			return Query{}, TableNotExists(tablename, err)
// 		}
// 		if m.err != nil {
// 			return Query{}, m.err
// 		}
// 		if ok {
// 			if trace != nil {
// 				trace(TableName{
// 					Table: tablename,
// 					Tags:  m.tags,
// 				})
// 			}
// 			list = append(list, m)
// 		}
// 	}
// 	if len(list) == 0 {
// 		return Query{}, TableNotExists(tablename)
// 	}
// 	query := FromWithTags(list[0].data, list[0].tags)
// 	for i := 1; i < len(list); i++ {
// 		query = query.UnionAll(FromWithTags(list[i].data, list[i].tags))
// 	}
// 	return query, nil
// }

func (s *storage) Set(name string, tags []KeyValue, t time.Time, data Table, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.measurements[name]
	if byKey == nil {
		byKey = map[string]Measurement{}
		s.measurements[name] = byKey
	}

	for idx := range data.Columns {
		data.Columns[idx].TableName = name
	}

	copyed := KeyValues(CloneKeyValues(tags))
	sort.Sort(copyed)
	key := KeyValues(copyed).ToKey()

	m := Measurement{
		Name:    TableName{Table: name, Tags: copyed},
		Time:    t,
		Data:    data,
		ErrTime: t,
		Err:     err,
	}

	old, ok := byKey[key]
	if ok {
		if err != nil {
			m.Data = old.Data
			m.Time = old.Time
		}
	}
	byKey[key] = m
	return nil
}

func (s *storage) Exists(tablename string, tags []KeyValue, predateLimit time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.measurements[tablename]
	if len(byKey) == 0 {
		return false
	}

	key := KeyValues(tags).ToKey()
	old, ok := byKey[key]
	if !ok {
		return false
	}
	if predateLimit.Before(old.Time) {
		return true
	}
	if old.Err != nil {
		if predateLimit.Before(old.ErrTime) {
			return true
		}
	}
	return false
}
