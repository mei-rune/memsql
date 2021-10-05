package memsql

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/runner-mei/memsql/parser"
	"github.com/xwb1989/sqlparser"
)

func TestKeyValues(t *testing.T) {

	opts := cmp.Options{
		cmpopts.EquateApproxTime(1 * time.Second),
	}

	for _, test := range []struct {
		sql       string
		qualifier string
		keyvalues [][]KeyValue
	}{
		{
			sql:       "select a from abc where @mo=1",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
				},
			},
		},
		{
			sql:       "select a from abc where @mo=1 and @b=2",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
					KeyValue{
						Key:   "b",
						Value: "2",
					},
				},
			},
		},
		{
			sql:       "select a from abc where @mo in (1, 2)",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
				},
			},
		},
		{
			sql:       "select a from abc where @mo in (1, 2) and @a=2",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
					KeyValue{
						Key:   "a",
						Value: "2",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
					KeyValue{
						Key:   "a",
						Value: "2",
					},
				},
			},
		},
		{
			sql:       "select a from abc where @a=2 and @mo in (1, 2)",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "a",
						Value: "2",
					},
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "a",
						Value: "2",
					},
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
				},
			},
		},
	} {
		t.Run(test.sql, func(t *testing.T) {
			stmt, err := sqlparser.Parse(test.sql)
			if err != nil {
				t.Error(test.sql)
				t.Error(err)
				return
			}
			sel, _ := stmt.(*sqlparser.Select)
			iter, err := parser.ToKeyValues(nil, sel.Where.Expr, test.qualifier, nil)
			if err != nil {
				t.Error(err)
				return
			}
			if iter == nil {
				t.Error("iter is null")
				return
			}
			results, err := parser.ToKeyValueArray(nil, iter)
			if err != nil {
				t.Error(err)
				return
			}

			if !cmp.Equal(results, test.keyvalues, opts) {
				txt := cmp.Diff(results, test.keyvalues, opts)
				t.Error(txt)
			}
		})
	}
}
