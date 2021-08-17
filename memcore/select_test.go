package memcore

import (
	"testing"
)

func TestSelectIndexed(t *testing.T) {
	tests := []struct {
		input    []int64
		selector func(int, Record) Record
		output   []Record
	}{
		{[]int64{1, 2, 3}, func(i int, x Record) Record {
			x.Values[0].Int64 = x.Values[0].Int64 * int64(i)
			return x
		}, makeRecords(0, 2, 6)},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).Select(test.selector); !validateQuery(q, test.output) {
			t.Errorf("From(%v).SelectIndexed()=%v expected %v", test.input, toSlice(q), test.output)
		}
	}
}
