package memsql

import (
	"sort"
	"strings"
	"errors"
	"sync"
)


func TableNotExists(table string, tags []KeyValue) error {
	return errors.New("table '"+table+"' isnot exists")
}

type KeyValue struct {
	Key   string
	Value string
}

type KeyValues []KeyValue

func (kvs KeyValues) Get(key string) (bool, string) {
	for idx := range kvs {
		if kvs[idx].Key == key {
			return true, kvs[idx].Value
		}
	}
	return false, ""
}

func (kvs KeyValues) Equal(to KeyValues) bool {
	if len(kvs) != len(to) {
		return false
	}
	for idx := range kvs {
		ok, value := to.Get(kvs[idx].Key)
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

func (kvs KeyValues) ToKey() string {
	sort.Sort(kvs)
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
	tags  []KeyValue
	table Table
}

type storage struct {
	mu           sync.Mutex
	measurements map[string]map[string]measurement
}

func (s *storage) Select(name string, tags []KeyValue) (Query, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.measurements[name]
	if len(byKey) == 0 {
		return Query{}, TableNotExists(name, nil)
	}
	key := KeyValues(tags).ToKey()
	m, ok := byKey[key]
	if !ok {
		return Query{}, TableNotExists(name, tags)
	}
	return From(m.table), nil
}

func (s *storage) Set(name string, tags []KeyValue, table Table) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byKey := s.measurements[name]
	if byKey == nil {
		byKey = map[string]measurement{}
		s.measurements[name] = byKey
	}

	key := KeyValues(tags).ToKey()
	byKey[key] = measurement {
					tags: tags,
					table: table,
				}
}
