package memcore

import (
	"strings"
	"testing"
)

func TestAggregate(t *testing.T) {
	tests := []struct {
		input []string
		want  string
	}{
		{[]string{"apple", "mango", "orange", "passionfruit", "grape"}, "passionfruit"},
		{[]string{}, "<nil>"},
	}

	for _, test := range tests {
		r, ok := fromStrings(test.input...).Aggregate(func(r Record, i Record) Record {
			if len(r.Values[0].Str) > len(i.Values[0].Str) {
				return r
			}
			return i
		})

		if !ok && test.want == "<nil>" {
			continue
		}

		if r.Values[0].Str != test.want {
			t.Errorf("From(%v).Aggregate()=%v expected %v", r.Values[0].Str, r, test.want)
		}
	}
}

func TestAggregateWithSeed(t *testing.T) {
	input := []string{"apple", "mango", "orange", "banana", "grape"}
	want := "passionfruit"

	r := fromStrings(input...).AggregateWithSeed(makeRecordWithStr(want),
		func(r Record, i Record) Record {
			if len(r.Values[0].Str) > len(i.Values[0].Str) {
				return r
			}
			return i
		})

	if r.Values[0].Str != want {
		t.Errorf("From(%v).AggregateWithSeed()=%v expected %v", input, r, want)
	}
}

func TestAggregateWithSeedBy(t *testing.T) {
	input := []string{"apple", "mango", "orange", "passionfruit", "grape"}
	want := "PASSIONFRUIT"

	r := fromStrings(input...).AggregateWithSeedBy(makeRecordWithStr("banana"),
		func(r Record, i Record) Record {
			if len(r.Values[0].Str) > len(i.Values[0].Str) {
				return r
			}
			return i
		},
		func(r Record) Record {
			r.Values[0].Str = strings.ToUpper(r.Values[0].Str)
			return r
		},
	)

	if r.Values[0].Str != want {
		t.Errorf("From(%v).AggregateWithSeed()=%v expected %v", input, r, want)
	}
}
