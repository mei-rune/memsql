package memsql

import "testing"

func TestAppend(t *testing.T) {
	input := []int64{1, 2, 3, 4}
	want := makeRecords(1, 2, 3, 4, 5)

	if q := fromInts(input...).Append(makeRecord(5)); !validateQuery(q, want) {
		t.Errorf("From(%v).Append()=%v expected %v", input, toSlice(q), want)
	}
}

func TestConcat(t *testing.T) {
	input1 := []int64{1, 2, 3}
	input2 := []int64{4, 5}
	want := makeRecords(1, 2, 3, 4, 5)

	if q := fromInts(input1...).Concat(fromInts(input2...)); !validateQuery(q, want) {
		t.Errorf("From(%v).Concat(%v)=%v expected %v", input1, input2, toSlice(q), want)
	}
}

func TestPrepend(t *testing.T) {
	input := []int64{1, 2, 3, 4}
	want := makeRecords(0, 1, 2, 3, 4)

	if q := fromInts(input...).Prepend(makeRecord(0)); !validateQuery(q, want) {
		t.Errorf("From(%v).Prepend()=%v expected %v", input, toSlice(q), want)
	}
}
