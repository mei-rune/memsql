package memcore

import (
	"sort"
	"strings"
	"sync"

	"github.com/runner-mei/errors"
)

var ErrNotFound = errors.ErrNotFound

func TableNotExists(table string) error {
	return errors.WithTitle(errors.ErrTableNotExists, "table '"+table+"' isnot exists")
}

type ExecuteContext interface {}

type GetValuer interface{
	GetValue(tableName, name string) (Value, error)
}

type GetValueFunc  func(tableName, name string) (Value, error)

func (f GetValueFunc)	GetValue(tableName, name string) (Value, error) {
	return f(tableName, name)
}

type Storage interface {
	From(ctx ExecuteContext, tablename string, filter func(ctx ExecuteContext) (bool, error))
}

type KeyValue struct {
	Key   string
	Value string
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

type measurement struct {
	tags  KeyValues
	table Table
}

func toGetValuer(tags  KeyValues) GetValuer {
	return GetValueFunc(func(tableName, name string) (Value, error) {
		value, ok := tags.Get(name)
		if ok {
			return StringToValue(value), nil
		}

		return Null(), ErrNotFound
	})
}

type storage struct {
	mu           sync.Mutex
	measurements map[string]map[string]measurement
}

func (s *storage) From(ctx ExecuteContext, tablename string, filter func(ctx GetValuer) (bool, error)) (Query, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.measurements[tablename]
	if len(byKey) == 0 {
		return Query{}, TableNotExists(tablename)
	}

	var list []measurement 
	for _, m := range byKey {
		values := toGetValuer(m.tags)
		ok, err := filter(values)
		if err != nil {
			return Query{}, err
		}

		if ok {
			list = append(list, m)
		}
	}
	if len(list) == 0 {
		return Query{}, TableNotExists(tablename)
	}
	query := From(list[0].table)
	for i := 1; i < len(list); i ++ {
		query = query.UnionAll(From(list[i].table))
	}
	return query, nil
}

func (s *storage) Set(name string, tags []KeyValue, table Table) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.measurements[name]
	if byKey == nil {
		byKey = map[string]measurement{}
		s.measurements[name] = byKey
	}

	copyed := KeyValues(CloneKeyValues(tags))
	sort.Sort(copyed)
	key := KeyValues(copyed).ToKey()
	byKey[key] = measurement {
					tags: copyed,
					table: table,
				}
}
