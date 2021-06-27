package memsql

import "testing"

func TestZip(t *testing.T) {
	input1 := []int64{1, 2, 3}
	input2 := []int64{2, 4, 5, 1}
	want := makeRecords(3, 6, 8)

	if q := fromInts(input1...).Zip(fromInts(input2...), func(i, j Record) Record {
		return makeRecord(i.Values[0].Int64 + j.Values[0].Int64)
	}); !validateQuery(q, want) {
		t.Errorf("From(%v).Zip(%v)=%v expected %v", input1, input2, toSlice(q), want)
	}
}
