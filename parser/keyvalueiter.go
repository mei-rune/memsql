package parser

import (
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/vm"
)

type KeyValueIterator interface {
	Next(ctx vm.Context) ([]memcore.KeyValue, error)
}

type keyValues struct {
	name  string
	query StringIterator
}

func (kvs *keyValues) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	value, err := kvs.query.Next(ctx)
	if err != nil {
		return nil, err
	}
	return []memcore.KeyValue{{Key: kvs.name, Value: value}}, nil
}

type kvList struct {
	list  [][]memcore.KeyValue
	index int
}

func (kl *kvList) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	if len(kl.list) >= kl.index {
		return nil, memcore.ErrNoRows
	}
	return kl.list[kl.index], nil
}

type simpleKv struct {
	values   []memcore.KeyValue
	readable bool
}

func (simple *simpleKv) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	if !simple.readable {
		return nil, memcore.ErrNoRows
	}
	simple.readable = false
	return simple.values, nil
}

type mergeIterator struct {
	query1, query2 KeyValueIterator

	done    bool
	readErr error
	inner   [][]memcore.KeyValue

	outer      []memcore.KeyValue
	innerLen   int
	innerIndex int
}

func (merge *mergeIterator) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	if !merge.done {
		if merge.readErr != nil {
			return nil, merge.readErr
		}
		for {
			kv, err := merge.query2.Next(ctx)
			if err != nil {
				if !memcore.IsNoRows(err) {
					merge.readErr = err
					return nil, err
				}
				break
			}
			merge.inner = append(merge.inner, kv)
		}

		merge.innerIndex = len(merge.inner)
		merge.done = true
	}

	if merge.innerIndex >= len(merge.inner) {
		outer, err := merge.query1.Next(ctx)
		if err != nil {
			return nil, err
		}

		merge.outer = outer
		merge.innerIndex = 0
	}

	items := append(merge.outer, merge.inner[merge.innerIndex]...)
	merge.innerIndex++
	return items, nil
}

func appendKeyValueIterator(query KeyValueIterator, kv ...memcore.KeyValue) KeyValueIterator {
	if query == nil {
		return &simpleKv{values: kv, readable: true}
	}
	switch q := query.(type) {
	case *kvList:
		for idx := range q.list {
			q.list[idx] = append(q.list[idx], kv...)
		}
		return q
	case *simpleKv:
		return &simpleKv{
			values:   append(q.values, kv...),
			readable: q.readable,
		}
	default:
		return &mergeIterator{
			query1: query,
			query2: &simpleKv{values: kv, readable: true},
		}
	}
}

func ToKeyValueArray(ctx vm.Context, iter KeyValueIterator) ([][]memcore.KeyValue, error) {
	var results [][]memcore.KeyValue
	for {
		item, err := iter.Next(ctx)
		if err != nil {
			if memcore.IsNoRows(err) {
				return results, nil
			}
			return nil, err
		}
		results = append(results, item)
	}
}
