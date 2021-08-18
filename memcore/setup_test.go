package memcore

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

	return func() (item Record, err error) {
		switch i {
		case 0:
			item = Record{
				Columns: []Column{{Name: "c1"}},
				Values:  []Value{{Type: ValueInt64, Int64: int64(f.f1)}},
			}
			err = nil
		case 1:
			item = Record{
				Columns: []Column{{Name: "c1"}},
				Values:  []Value{{Type: ValueBool, Bool: f.f2}},
			}
			err = nil
		case 2:
			item = Record{
				Columns: []Column{{Name: "c1"}},
				Values:  []Value{{Type: ValueString, Str: f.f3}},
			}
			err = nil
		default:
			err = ErrNoRows
		}

		i++
		return
	}
}

// func (f foo) CompareTo(c Comparable) int {
// 	a, b := f.f1, c.(foo).f1

// 	if a < b {
// 		return -1
// 	} else if a > b {
// 		return 1
// 	}

// 	return 0
// }

func toSlice(q Query) (result []Record) {
	result, err := q.Results()
	if err != nil {
		panic(err)
	}
	return result
}

func validateQuery(q Query, output []Record) bool {
	next := q.Iterate()

	for _, oitem := range output {
		qitem, err := next()
		if err != nil {
			panic(err)
		}

		ok, err := oitem.EqualTo(qitem, emptyCompareOption)
		if err != nil {
			panic(err)
		}
		if !ok {
			return false
		}
	}

	_, err := next()
	if err != nil {
		if !IsNoRows(err) {
			panic(err)
		}
	} else {
		return false
	}

	_, err = next()
	if err != nil {
		if IsNoRows(err) {
			return true
		}
		panic(err)
	} else {
		return false
	}
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
