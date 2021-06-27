package memsql

import "testing"

func TestJoin(t *testing.T) {
	outer := []int64{0, 1, 2, 3, 4, 5, 8}
	inner := []int64{1, 2, 1, 4, 7, 6, 7, 2}

	columns := []Column{
		{Name: "c1"},
		{Name: "c1"},
	}

	want := []Record{
		{Columns: columns, Values: []Value{MustToValue(1), MustToValue(1)}},
		{Columns: columns, Values: []Value{MustToValue(1), MustToValue(1)}},
		{Columns: columns, Values: []Value{MustToValue(2), MustToValue(2)}},
		{Columns: columns, Values: []Value{MustToValue(2), MustToValue(2)}},
		{Columns: columns, Values: []Value{MustToValue(4), MustToValue(4)}},
	}

	q := fromInts(outer...).Join(
		fromInts(inner...),
		func(i Record) Value { return i.Values[0] },
		func(i Record) Value { return i.Values[0] },
		func(outer Record, inner Record) Record {
			return Record{
				Columns: append(outer.Columns, inner.Columns...),
				Values:  append(outer.Values, inner.Values...),
			}
		})

	if !validateQuery(q, want) {
		t.Errorf("From().Join()=%v expected %v", toSlice(q), want)
	}
}
