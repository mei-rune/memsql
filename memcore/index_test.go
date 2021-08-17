package memcore

import (
	"testing"
)

func TestIndexOf(t *testing.T) {
	tests := []struct {
		input     []Record
		predicate func(Record) bool
		expected  int
	}{
		{
			input: makeRecords(1, 2, 3, 4, 5, 6, 7, 8, 9),
			predicate: func(i Record) bool {
				return i.Values[0].Int64 == 3
			},
			expected: 2,
		},
	}

	for _, test := range tests {
		index := FromRecords(test.input).IndexOf(test.predicate)
		if index != test.expected {
			t.Errorf("From(%v).IndexOf() expected %v received %v", test.input, test.expected, index)
		}
	}
}
