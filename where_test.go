package memsql

import "testing"

func TestWhereIndexed(t *testing.T) {
	tests := []struct {
		input     []int64
		predicate func(int, Record) bool
		output    []Record
	}{
		{[]int64{1, 1, 1, 2, 1, 2, 3, 4, 2}, func(i int, x Record) bool {
			return x.Values[0].Int64 < 4 && i > 4
		}, makeRecords(2, 3, 2)},
		// {"sstr", func(i int, x interface{}) bool {
		// 	return x.(rune) != 's' || i == 1
		// }, []interface{}{'s', 't', 'r'}},
		// {"abcde", func(i int, _ interface{}) bool {
		// 	return i < 2
		// }, []interface{}{'a', 'b'}},
	}

	for _, test := range tests {
		if q := fromInts(test.input...).Where(test.predicate); !validateQuery(q, test.output) {
			t.Errorf("From(%v).WhereIndexed()=%v expected %v", test.input, toSlice(q), test.output)
		}
	}
}
