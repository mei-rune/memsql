package parser

import (
	"reflect"
	"testing"

	"github.com/xwb1989/sqlparser"
)

func TestSplit(t *testing.T) {
	for _, test := range []struct {
		s       string
		results []string
	}{
		{
			s: "select a from a where a=1",
			results: []string{
				"a = 1",
			},
		},
		{
			s: "select a from a where a=1 or b=2",
			results: []string{
				"a = 1",
				"b = 2",
			},
		},
		{
			s: "select a from a where a=1 or b=2 or c=3",
			results: []string{
				"a = 1",
				"b = 2",
				"c = 3",
			},
		},
		{
			s: "select a from a where a=1 or b=2 or c=3 or d=4",
			results: []string{
				"a = 1",
				"b = 2",
				"c = 3",
				"d = 4",
			},
		},
		{
			s: "select a from a where a=1 and b=2 or c=3",
			results: []string{
				"a = 1 and b = 2",
				"c = 3",
			},
		},
		{
			s: "select a from a where a=1 or b=2 and c=3",
			results: []string{
				"a = 1",
				"b = 2 and c = 3",
			},
		},
		{
			s: "select a from a where a=1 and b=2 or c=3 and d=4",
			results: []string{
				"a = 1 and b = 2",
				"c = 3 and d = 4",
			},
		},
		{
			s: "select a from a where a=1 and b=2 and c=3 or d=4",
			results: []string{
				"a = 1 and b = 2 and c = 3",
				"d = 4",
			},
		},
		{
			s: "select a from a where a=1 or b=2 and c=3 and d=4",
			results: []string{
				"a = 1",
				"b = 2 and c = 3 and d = 4",
			},
		},
		{
			s: "select a from a where a=1 and b=2 or c=3 and d=4 or e=5 and f=6",
			results: []string{
				"a = 1 and b = 2",
				"c = 3 and d = 4",
				"e = 5 and f = 6",
			},
		},
	} {
		stmt, err := sqlparser.Parse(test.s)
		if err != nil {
			t.Error(test.s)
			t.Error(err)
			continue
		}
		sel, _ := stmt.(*sqlparser.Select)

		exprs, err := SplitByOr(sel.Where.Expr)
		if err != nil {
			t.Error(test.s)
			t.Error(err)
			continue
		}

		ss := make([]string, len(exprs))
		for i := range exprs {
			ss[i] = sqlparser.String(exprs[i])
		}

		if !reflect.DeepEqual(test.results, ss) {
			t.Error(test.s)
			t.Error("want:", test.results)
			t.Error(" got:", ss)
			continue
		}
	}
}
