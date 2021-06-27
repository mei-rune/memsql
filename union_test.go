package memsql

import "testing"

func TestUnion(t *testing.T) {
	input1 := []int64{1, 2, 3}
	input2 := []int64{2, 4, 5, 1}
	want := makeRecords(1, 2, 3, 4, 5)

	if q := fromInts(input1...).Union(fromInts(input2...)); !validateQuery(q, want) {
		t.Errorf("From(%v).Union(%v)=%v expected %v", input1, input2, toSlice(q), want)
	}
}
