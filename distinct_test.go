package memsql

import "testing"

func TestDistinct(t *testing.T) {
	tests := []struct {
		input  []int64
		output []Record
	}{
		{[]int64{1, 2, 2, 3, 1}, makeRecords(1, 2, 3)},
		{[]int64{1, 1, 1, 2, 1, 2, 3, 4, 2}, makeRecords(1, 2, 3, 4)},
		// {"sstr", []interface{}{'s', 't', 'r'}},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).Distinct(); !validateQuery(q, test.output) {
			t.Errorf("From(%v).Distinct()=%v expected %v", test.input, toSlice(q), test.output)
		}
	}
}

func TestDistinctForOrderedQuery(t *testing.T) {
	tests := []struct {
		input  []int64
		output []Record
	}{
		{[]int64{1, 2, 2, 3, 1}, makeRecords(1, 2, 3)},
		{[]int64{1, 1, 1, 2, 1, 2, 3, 4, 2}, makeRecords(1, 2, 3, 4)},
		// {"sstr", []interface{}{'r', 's', 't'}},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).OrderBy(func(i Record) Value {
			return i.Values[0]
		}).Distinct(); !validateQuery(q.Query, test.output) {
			t.Errorf("From(%v).Distinct()=%v expected %v", test.input, toSlice(q.Query), test.output)
		}
	}
}

func TestDistinctBy(t *testing.T) {
	type user struct {
		id   int
		name string
	}

	columns := []Column{
		{Name: "name"},
		{Name: "value"},
	}
	users := []Record{
		{Columns: columns, Values: []Value{MustToValue("Foo"), MustToValue(1)}},
		{Columns: columns, Values: []Value{MustToValue("Bar"), MustToValue(2)}},
		{Columns: columns, Values: []Value{MustToValue("Foo"), MustToValue(3)}},
	}
	want := []Record{
		{Columns: columns, Values: []Value{MustToValue("Foo"), MustToValue(1)}},
		{Columns: columns, Values: []Value{MustToValue("Bar"), MustToValue(2)}},
	}

	if q := FromRecords(users).DistinctBy(func(u Record) Value {
		v, _ := u.Get("name")
		return v
	}); !validateQuery(q, want) {
		t.Errorf("From(%v).DistinctBy()=%v expected %v", users, toSlice(q), want)
	}
}
