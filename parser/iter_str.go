package parser

import (
	"fmt"

	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/vm"
	"github.com/xwb1989/sqlparser"
)

type StringIterator interface {
	Next(ctx vm.Context) (string, error)
}

type simpleStringIterator struct {
	value    string
	readable bool
}

func (simple *simpleStringIterator) Next(ctx vm.Context) (string, error) {
	if !simple.readable {
		return "", memcore.ErrNoRows
	}
	simple.readable = false
	return simple.value, nil
}

func toStringIterator(s string) StringIterator {
	return &simpleStringIterator{
		value:    s,
		readable: true,
	}
}

type stringList struct {
	list  []string
	index int
}

func (kl *stringList) Next(ctx vm.Context) (string, error) {
	if len(kl.list) <= kl.index {
		return "", memcore.ErrNoRows
	}
	s := kl.list[kl.index]
	kl.index++
	return s, nil
}

func cloneStrings(ss []string) []string {
	c := make([]string, len(ss))
	copy(c, ss)
	return c
}

type unionStrs struct {
	query1, query2 StringIterator
	query1Done     bool
}

func (us *unionStrs) Next(ctx vm.Context) (string, error) {
	if !us.query1Done {
		s, err := us.query1.Next(ctx)
		if err == nil {
			return s, nil
		}
		if !memcore.IsNoRows(err) {
			return "", err
		}
		us.query1Done = true
	}
	return us.query2.Next(ctx)
}

func appendStringIterator(query1, query2 StringIterator) StringIterator {
	// fmt.Println("=====1", fmt.Sprintf("%T %s", query1, query1))
	// fmt.Println("=====2", fmt.Sprintf("%T %s", query2, query2))

	switch q1 := query1.(type) {
	case *stringList:
		switch q2 := query2.(type) {
		case *stringList:
			return &stringList{
				list: append(cloneStrings(q1.list[q1.index:]), q2.list[q2.index:]...),
			}
		case *simpleStringIterator:
			if !q2.readable {
				return q1
			}
			return &stringList{
				list: append(cloneStrings(q1.list[q1.index:]), q2.value),
			}
		}
	case *simpleStringIterator:
		if !q1.readable {
			return query2
		}
		switch q2 := query2.(type) {
		case *stringList:
			return &stringList{
				list: append(cloneStrings(q2.list[q2.index:]), q1.value),
			}
		case *simpleStringIterator:
			if !q2.readable {
				return q1
			}
			return &stringList{
				list: []string{q1.value, q2.value},
			}
		}
	}
	return &unionStrs{
		query1: query1,
		query2: query2,
	}
}

type subqueryStringIterator struct {
	key      string
	fctx     FilterContext
	subquery sqlparser.SelectStatement

	done    bool
	records []memcore.Record
	err     error

	index int
}

func (iter *subqueryStringIterator) Next(ctx vm.Context) (string, error) {
	if !iter.done {
		if iter.err != nil {
			return "", iter.err
		}
		fmt.Println(sqlparser.String(iter.subquery))
		q, err := iter.fctx.ExecuteSelect(iter.subquery)
		if err != nil {
		fmt.Println(sqlparser.String(iter.subquery), err)
			iter.err = err
			return "", err
		}
		records, err := q.Results(ctx)
		if err != nil {
		fmt.Println(sqlparser.String(iter.subquery), err)
			iter.err = err
			return "", err
		}
		fmt.Println(sqlparser.String(iter.subquery), "===",records)

		iter.records = records
		iter.done = true
		iter.index = 0

		iter.fctx.SetResultSet(iter.key, iter.records)
	}

	if len(iter.records) <= iter.index {
		return "", memcore.ErrNoRows
	}

	s := iter.records[iter.index].At(0)
	iter.index++

	return s.AsString(true)
}

type queryIterator struct {
	Qualifier string
	Query     memcore.Query
	field     string

	done    bool
	records []memcore.Record
	err     error

	index int
}

func (iter *queryIterator) Next(ctx vm.Context) (string, error) {
	if !iter.done {
		if iter.err != nil {
			return "", iter.err
		}
		records, err := iter.Query.Results(ctx)
		if err != nil {
			iter.err = err
			return "", err
		}
		iter.records = records
		iter.done = true
		iter.index = 0
	}

	for {
		if len(iter.records) <= iter.index {
			return "", memcore.ErrNoRows
		}
		item := iter.records[iter.index]
		iter.index++

		s, ok := item.Get(iter.field)
		if ok {
			return s.AsString(true)
		}
	}
}
