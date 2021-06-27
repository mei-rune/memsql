package memsql

import "testing"

func TestFrom(t *testing.T) {
	if q := FromRecords(makeRecords(1, 2, 3)); !validateQuery(q, makeRecords(1, 2, 3)) {
		t.Errorf("From(%v)=%v expected %v", makeRecords(1, 2, 3), toSlice(q), makeRecords(1, 2, 3))
	}

	if q := FromRecords(makeRecords(1, 2, 4)); validateQuery(q, makeRecords(1, 2, 3)) {
		t.Errorf("From(%v)=%v expected not equal", makeRecords(1, 2, 4), makeRecords(1, 2, 3))
	}

	excepted := []Record{
		Record{
			Columns: []Column{{Name: "c1"}},
			Values:  []Value{{Type: ValueInt64, Int64: 1}},
		},
		Record{
			Columns: []Column{{Name: "c1"}},
			Values:  []Value{{Type: ValueBool, Bool: true}},
		},
		Record{
			Columns: []Column{{Name: "c1"}},
			Values:  []Value{{Type: ValueString, Str: "string"}},
		},
	}
	fooinput := foo{f1: 1, f2: true, f3: "string"}
	if q := FromIterable(fooinput); validateQuery(q, excepted) != true {
		t.Errorf("From(%v)=%v expected %v", fooinput, toSlice(q), excepted)
	}
}

func TestFromChannel(t *testing.T) {
	c := make(chan Record, 3)
	c <- makeRecord(10)
	c <- makeRecord(15)
	c <- makeRecord(-3)
	close(c)

	w := []Record{makeRecord(10), makeRecord(15), makeRecord(-3)}

	if q := FromChannel(c); !validateQuery(q, w) {
		t.Errorf("FromChannel() failed expected %v", w)
	}
}