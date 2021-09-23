package memcore

import "testing"

func TestMap(t *testing.T) {
	input1 := []int64{1, 2, 3}
	want := makeRecords(2, 4, 6)

	if q := fromInts(input1...).Map(func(c Context, r Record) (Record, error) {
		return makeRecord(r.Values[0].Int64 * 2), nil
	}); !validateQuery(q, want) {
		t.Errorf("From(%v).Zip(v*2)=%v expected %v", input1, toSlice(q), want)
	}
}
