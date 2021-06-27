package memsql

import "testing"

func TestTake(t *testing.T) {
	tests := []struct {
		input  []int64
		output []Record
	}{
		{[]int64{1, 2, 2, 3, 1}, makeRecords(1, 2, 2)},
		{[]int64{1, 1, 1, 2, 1, 2, 3, 4, 2}, makeRecords(1, 1, 1)},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).Take(3); !validateQuery(q, test.output) {
			t.Errorf("From(%v).Take(3)=%v expected %v", test.input, toSlice(q), test.output)
		}
	}
}

func TestTakeWhile(t *testing.T) {
	tests := []struct {
		input     []int64
		predicate func(int, Record) bool
		output    []Record
	}{
		{[]int64{1, 1, 1, 2}, func(i int, x Record) bool {
			return x.Values[0].Int64 < 2 || i < 5
		}, makeRecords(1, 1, 1, 2)},
		{[]int64{1, 1, 1, 2, 1, 2, 3, 4, 2}, func(i int, x Record) bool {
			return x.Values[0].Int64 < 2 || i < 5
		}, makeRecords(1, 1, 1, 2, 1)},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).TakeWhile(test.predicate); !validateQuery(q, test.output) {
			t.Errorf("From(%v).TakeWhileIndexed()=%v expected %v", test.input, toSlice(q), test.output)
		}
	}
}
