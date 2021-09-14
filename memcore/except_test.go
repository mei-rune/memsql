package memcore

import (
	"testing"

	"github.com/runner-mei/memsql/vm"
)

func TestExcept(t *testing.T) {
	input1 := []int64{1, 2, 3, 4, 5, 1, 2, 5}
	input2 := []int64{1, 2}
	want := makeRecords(3, 4, 5, 5)

	if q := fromInts(input1...).Except(fromInts(input2...)); !validateQuery(q, want) {
		t.Errorf("From(%v).Except(%v)=%v expected %v", input1, input2, toSlice(q), want)
	}
}

func TestExceptBy(t *testing.T) {
	input1 := []int64{1, 2, 3, 4, 5, 1, 2, 5}
	input2 := []int64{1}
	want := makeRecords(2, 4, 2)

	if q := fromInts(input1...).ExceptBy(fromInts(input2...), func(i Record) Value {
		return vm.IntToValue(i.Values[0].Int64 % 2)
	}); !validateQuery(q, want) {
		t.Errorf("From(%v).ExceptBy(%v)=%v expected %v", input1, input2, toSlice(q), want)
	}
}
