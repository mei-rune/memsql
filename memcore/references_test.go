package memcore

import (
	"reflect"
	"testing"
)

func TestToReference(t *testing.T) {

	input := []int64{1, 2, 3}
	want := makeRecords(1, 2, 3)
	source := fromInts(input...).ToReference()
	source.IsCopy = true

	query1 := source.Query
	query2 := source.Query

	r, err := query1.Results(mkCtx())
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("From(%v).Raw()=%v expected %v", input, r, want)
	}

	r, err = query2.Results(mkCtx())
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(r, want) {
		t.Errorf("From(%v).Raw()=%v expected %v", input, r, want)
	}
}
