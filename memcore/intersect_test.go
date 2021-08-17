package memcore

import "testing"

func TestIntersect(t *testing.T) {
	input1 := []int64{1, 2, 3}
	input2 := []int64{1, 4, 7, 9, 12, 3}
	want := makeRecords(1, 3)

	if q := fromInts(input1...).Intersect(fromInts(input2...)); !validateQuery(q, want) {
		t.Errorf("From(%v).Intersect(%v)=%v expected %v", input1, input2, toSlice(q), want)
	}
}

func TestIntersectBy(t *testing.T) {
	input1 := []int64{5, 7, 8}
	input2 := []int64{1, 4, 7, 9, 12, 3}
	want := makeRecords(5, 8)

	if q := fromInts(input1...).IntersectBy(fromInts(input2...), func(i Record) Value {
		return IntToValue(i.Values[0].Int64 % 2)
	}); !validateQuery(q, want) {
		t.Errorf("From(%v).IntersectBy(%v)=%v expected %v", input1, input2, toSlice(q), want)
	}
}
