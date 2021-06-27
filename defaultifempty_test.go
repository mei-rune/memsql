package memsql

import (
	"testing"
)

func TestDefaultIfEmpty(t *testing.T) {
	defaultValue := int64(0)
	tests := []struct {
		input []int64
		want  []Record
	}{
		{[]int64{}, makeRecords(defaultValue)},
		{[]int64{1, 2, 3, 4, 5}, makeRecords(1, 2, 3, 4, 5)},
	}

	for _, test := range tests {
		q := fromInts(test.input...).DefaultIfEmpty(makeRecord(defaultValue))

		if !validateQuery(q, test.want) {
			t.Errorf("From(%v).DefaultIfEmpty(%v)=%v expected %v", test.input, defaultValue, toSlice(q), test.want)
		}
	}

}
