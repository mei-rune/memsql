package memsql

import (
	"fmt"
	"testing"
)

func makeRecord(value int64) Record {
	return Record{
		Columns: []Column{{Name: "c1"}},
		Values:  []Value{{Type: ValueInt64, Int64: value}},
	}
}

func makeRecords(value ...int64) []Record {
	results := []Record{}
	for _, v := range value {
		results = append(results, makeRecord(v))
	}
	return results
}

func fromInts(input ...int64) Query {
	return FromRecords(makeRecords(input...))
}

func makeRecordWithStr(value string) Record {
	return Record{
		Columns: []Column{{Name: "c1"}},
		Values:  []Value{{Type: ValueString, Str: value}},
	}
}

func makeRecordsWithStrings(value ...string) []Record {
	results := []Record{}
	for _, v := range value {
		results = append(results, Record{
			Columns: []Column{{Name: "c1"}},
			Values:  []Value{{Type: ValueString, Str: v}},
			})
	}
	return results
}

func fromStrings(input ...string) Query {
	return FromRecords(makeRecordsWithStrings(input...))
}

type foo struct {
	f1 int
	f2 bool
	f3 string
}

func (f foo) Iterate() Iterator {
	i := 0

	return func() (item Record, ok bool) {
		switch i {
		case 0:
			item = Record{
				Columns: []Column{{Name: "c1"}},
				Values:  []Value{{Type: ValueInt64, Int64: int64(f.f1)}},
			}
			ok = true
		case 1:
			item = Record{
				Columns: []Column{{Name: "c1"}},
				Values:  []Value{{Type: ValueBool, Bool: f.f2}},
			}
			ok = true
		case 2:
			item = Record{
				Columns: []Column{{Name: "c1"}},
				Values:  []Value{{Type: ValueString, Str: f.f3}},
			}
			ok = true
		default:
			ok = false
		}

		i++
		return
	}
}

func (f foo) CompareTo(c Comparable) int {
	a, b := f.f1, c.(foo).f1

	if a < b {
		return -1
	} else if a > b {
		return 1
	}

	return 0
}

func toSlice(q Query) (result []Record) {
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		result = append(result, item)
	}

	return
}

func validateQuery(q Query, output []Record) bool {
	next := q.Iterate()

	for _, oitem := range output {
		qitem, _ := next()

		if !oitem.EqualTo(qitem, emptyCompareOption) {
			return false
		}
	}

	_, ok := next()
	_, ok2 := next()
	return !(ok || ok2)
}

func mustPanicWithError(t *testing.T, expectedErr string, f func()) {
	defer func() {
		r := recover()
		err := fmt.Sprintf("%s", r)
		if err != expectedErr {
			t.Fatalf("got=[%v] expected=[%v]", err, expectedErr)
		}
	}()
	f()
}
