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
		r, err := fromStrings(test.input...).Aggregate(mkCtx(), func(c Context, r Record, i Record) (Record, error) {
			if len(r.Values[0].Str) > len(i.Values[0].Str) {
				return r, nil
			}
			return i, nil
		})
		if err != nil {
			t.Error(err)
			return
		}

		if test.want == "<nil>" {
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

	r, err := fromStrings(input...).AggregateWithSeed(mkCtx(), makeRecordWithStr(want),
		func(c Context, r Record, i Record) (Record, error) {
			if len(r.Values[0].Str) > len(i.Values[0].Str) {
				return r, nil
			}
			return i, nil
		})
	if err != nil {
		t.Error(err)
		return
	}

	if r.Values[0].Str != want {
		t.Errorf("From(%v).AggregateWithSeed()=%v expected %v", input, r, want)
	}
}

func TestAggregateWithSeedBy(t *testing.T) {
	input := []string{"apple", "mango", "orange", "passionfruit", "grape"}
	want := "PASSIONFRUIT"

	r, err := fromStrings(input...).AggregateWithSeedBy(mkCtx(), makeRecordWithStr("banana"),
		func(c Context, r Record, i Record) (Record, error) {
			if len(r.Values[0].Str) > len(i.Values[0].Str) {
				return r, nil
			}
			return i, nil
		},
		func(c Context, r Record) (Record, error) {
			r.Values[0].Str = strings.ToUpper(r.Values[0].Str)
			return r, nil
		},
	)
	if err != nil {
		t.Error(err)
		return
	}

	if r.Values[0].Str != want {
		t.Errorf("From(%v).AggregateWithSeed()=%v expected %v", input, r, want)
	}
}
