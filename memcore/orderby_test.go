package memcore

import (
	"testing"

	"github.com/runner-mei/memsql/vm"
)

func TestEmpty(t *testing.T) {
	q := fromStrings([]string{}...).OrderByAscending(func(in Record) (Value, error) {
		return vm.Null(), nil
	})

	_, err := q.Iterate()(mkCtx())
	if err != nil {
		if !IsNoRows(err) {
			t.Errorf("Iterator for empty collection must return ok=false %+v", err)
		}
	}
}

func TestOrderBy(t *testing.T) {
	slice := make([]int64, 100)

	for i := len(slice) - 1; i >= 0; i-- {
		slice[i] = int64(i)
	}

	q := fromInts(slice...).OrderByAscending(func(item Record) (Value, error) {
		return item.Values[0], nil
	})

	items, err := q.Results(mkCtx())
	if err != nil {
		t.Error(err)
		return
	}

	for j, item := range items {
		if item.Values[0].Int64 != int64(j) {
			t.Errorf("OrderByAscending()[%v]=%v expected %v", j, item, foo{f1: j})
		}
	}
}

func TestOrderByDescending(t *testing.T) {
	slice := make([]int64, 100)
	for i := 0; i < len(slice); i++ {
		slice[i] = int64(i)
	}

	q := fromInts(slice...).OrderByDescending(func(item Record) (Value, error) {
		return item.Values[0], nil
	})

	items, err := q.Results(mkCtx())
	if err != nil {
		t.Error(err)
		return
	}

	j := len(slice) - 1

	for _, item := range items {
		if item.Values[0].Int64 != int64(j) {
			t.Errorf("OrderByAscending()[%v]=%v expected %v", j, item, foo{f1: j})
		}
		j--
	}
}

func TestThenByAscending(t *testing.T) {
	slice := make([][2]int64, 1000)

	for i := len(slice) - 1; i >= 0; i-- {
		slice[i][0] = int64(i)
		if i%2 == 0 {
			slice[i][1] = int64(1)
		} else {
			slice[i][1] = int64(0)
		}
	}

	q := fromInt2(slice).OrderByAscending(func(item Record) (Value, error) {
		return item.Values[1], nil
	}).ThenByAscending(func(item Record) (Value, error) {
		return item.Values[0], nil
	})

	items, err := q.Results(mkCtx())
	if err != nil {
		t.Error(err)
		return
	}

	for _, item := range items {
		if (item.Values[1].Int64 == 1) != (item.Values[0].Int64%2 == 0) {
			t.Errorf("OrderByAscending().ThenBy()=%v", item)
		}
	}
}

func TestThenByDescending(t *testing.T) {
	slice := make([][2]int64, 1000)

	for i := len(slice) - 1; i >= 0; i-- {
		slice[i][0] = int64(i)
		if i%2 == 0 {
			slice[i][1] = int64(1)
		} else {
			slice[i][1] = int64(0)
		}
	}

	q := fromInt2(slice).OrderByAscending(func(item Record) (Value, error) {
		return item.Values[1], nil
	}).ThenByDescending(func(item Record) (Value, error) {
		return item.Values[0], nil
	})

	items, err := q.Results(mkCtx())
	if err != nil {
		t.Error(err)
		return
	}

	for _, item := range items {
		if (item.Values[1].Int64 == 1) != (item.Values[0].Int64%2 == 0) {
			t.Errorf("OrderBy().ThenByDescending()=%v", item)
		}
	}
}

func TestSort(t *testing.T) {
	slice := make([]int64, 100)

	for i := len(slice) - 1; i >= 0; i-- {
		slice[i] = int64(i)
	}

	q := fromInts(slice...).Sort(func(i, j Record) bool {
		return i.Values[0].Int64 < j.Values[0].Int64
	})

	items, err := q.Results(mkCtx())
	if err != nil {
		t.Error(err)
		return
	}

	for j, item := range items {
		if item.Values[0].Int64 != int64(j) {
			t.Errorf("Sort()[%v]=%v expected %v", j, item, foo{f1: j})
		}
	}
}
