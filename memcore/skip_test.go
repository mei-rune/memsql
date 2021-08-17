package memcore

import "testing"

func TestSkip(t *testing.T) {
	tests := []struct {
		input  []int64
		output []Record
	}{
		{[]int64{1, 2}, []Record{}},
		{[]int64{1, 2, 2, 3, 1}, makeRecords(3, 1)},
		{[]int64{1, 1, 1, 2, 1, 2, 3, 4, 2}, makeRecords(2, 1, 2, 3, 4, 2)},
		// {"sstr", []interface{}{'r'}},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).Skip(3); !validateQuery(q, test.output) {
			t.Errorf("From(%v).Skip(3)=%v expected %v", test.input, toSlice(q), test.output)
		}
	}
}

func TestSkipWhileIndexed(t *testing.T) {
	tests := []struct {
		input     []int64
		predicate func(int, Record) bool
		output    []Record
	}{
		{[]int64{1, 2}, func(i int, x Record) bool {
			return x.Values[0].Int64 < 3
		}, []Record{}},
		{[]int64{4, 1, 2}, func(i int, x Record) bool {
			return x.Values[0].Int64 < 3
		}, makeRecords(4, 1, 2)},
		{[]int64{1, 1, 1, 2, 1, 2, 3, 4, 2}, func(i int, x Record) bool {
			return x.Values[0].Int64 < 2 || i < 5
		}, makeRecords(2, 3, 4, 2)},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).SkipWhile(test.predicate); !validateQuery(q, test.output) {
			t.Errorf("From(%v).SkipWhile()=%v expected %v", test.input, toSlice(q), test.output)
		}
	}
}
