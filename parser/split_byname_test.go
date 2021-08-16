package parser

import (
	"reflect"
	"testing"

	"github.com/xwb1989/sqlparser"
)

func TestSplitByName(t *testing.T) {
	for _, test := range []struct {
		s       string
		results map[string]string
	}{
		{
			s: "select a from a where a.a = b.b and (b.c=1 or b.d = 2) and b.e = 3",
			results: map[string]string{
				"a": "",
				"b": "(b.c = 1 or b.d = 2) and b.e = 3",
			},
		},
		{
			s: "select a from a where a.b = 2 and a.a = b.b and (b.c=1 or b.d = 2) and b.e = 3",
			results: map[string]string{
				"a": "a.b = 2",
				"b": "(b.c = 1 or b.d = 2) and b.e = 3",
			},
		},
		{
			s: "select a from a where a.a = b.b and (b.c=1 or (b.d = 2 and a.b = 2)) and b.e = 3",
			results: map[string]string{
				"a": "((a.b = 2))",
				"b": "(b.c = 1 or (b.d = 2)) and b.e = 3",
			},
		},
		{
			s: "select a from a where a.a = b.b and ((b.c=1 and a.b = 2) or (b.d = 2 and a.b = 3)) and b.e = 3",
			results: map[string]string{
				"a": "((a.b = 2) or (a.b = 3))",
				"b": "((b.c = 1) or (b.d = 2)) and b.e = 3",
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

		for key, txt := range test.results {
			expr, err := SplitByTableName(sel.Where.Expr, key)
			if err != nil {
				t.Error(test.s)
				t.Error(err)
				continue
			}

			if expr == nil {
				if txt != "" {
					t.Error(test.s)
					t.Error("["+key+"] want:", txt)
					t.Error("["+key+"]  got:", "''")
				}
				continue
			}
			s := sqlparser.String(expr)
			if !reflect.DeepEqual(txt, s) {
				t.Error(test.s)
				t.Error("["+key+"] want:", txt)
				t.Error("["+key+"]  got:", s)
				continue
			}
		}
	}
}
