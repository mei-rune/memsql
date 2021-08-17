package memcore

import "testing"

func TestReverse(t *testing.T) {
	tests := []struct {
		input []int64
		want  []Record
	}{
		{[]int64{1, 2, 3}, makeRecords(3, 2, 1)},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).Reverse(); !validateQuery(q, test.want) {
			t.Errorf("From(%v).Reverse()=%v expected %v", test.input, toSlice(q), test.want)
		}
	}
}
