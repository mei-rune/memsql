package memsql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/parser"
	"github.com/runner-mei/memsql/vm"
	"github.com/xwb1989/sqlparser"
)

func TestKeyValues(t *testing.T) {
	conn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
		return
	}
	defer conn.Close()

	storage := memcore.NewStorage()

	table, err := memcore.ToTable([]map[string]interface{}{
		map[string]interface{}{
			"id":   1,
			"name": "1",
		},
		map[string]interface{}{
			"id":   2,
			"name": "3",
		},
	})
	if err != nil {
		t.Error(err)
		return 
	}
	storage.Set("mo_list", nil, time.Now(), table, nil)

	ctx := &Context{
		Ctx:     context.Background(),
		Storage: WrapStorage(storage),
		Foreign: NewDbForeign("sqlite3", conn),
	}
	fctx := &SessionContext{
		Context:    ctx,
		alias:      map[string]string{},
		resultSets: map[string][]memcore.Record{},
	}

	query, _, err := storage.From(ctx, "mo_list", func(ctx memcore.GetValuer) (bool, error){
		return true, nil
	})
	fctx.addQuery("mo_list", "mo", query)

	opts := cmp.Options{
		cmpopts.EquateApproxTime(1 * time.Second),
	}

	for _, test := range []struct {
		sql       string
		qualifier string
		keyvalues [][]KeyValue
		ctx       vm.Context
		fctx      parser.FilterContext
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
			sql:       "select a from abc where 1=@mo",
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
		{
			fctx:      fctx,
			sql:       "select a from abc where @mo in (select id from mo_list)",
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
			fctx:      fctx,
			sql:       "select a from abc where @a=3 and @mo in (select id from mo_list)",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "a",
						Value: "3",
					},
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "a",
						Value: "3",
					},
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
				},
			},
		},
		{
			fctx:      fctx,
			sql:       "select a from abc where @mo in (select id from mo_list) and @a=3",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
					KeyValue{
						Key:   "a",
						Value: "3",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
					KeyValue{
						Key:   "a",
						Value: "3",
					},
				},
			},
		},
		{
			fctx: fctx,
			sql:       "select a from abc, mo_list where @mo = mo_list.id",
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
			fctx: fctx,
			sql:       "select a from abc, mo_list where mo_list.id = @mo",
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
			fctx: fctx,
			sql:       "select a from abc, mo_list where mo_list.id = @mo and @abc in (1,2)",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
					KeyValue{
						Key:   "abc",
						Value: "1",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
					KeyValue{
						Key:   "abc",
						Value: "2",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
					KeyValue{
						Key:   "abc",
						Value: "1",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
					KeyValue{
						Key:   "abc",
						Value: "2",
					},
				},
			},
		},
		{
			fctx:      fctx,
			sql:       "select a from abc, mo_list where mo_list.id = @mo and @abc in (select id from mo_list)",
			qualifier: "",
			keyvalues: [][]KeyValue{
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
					KeyValue{
						Key:   "abc",
						Value: "1",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "1",
					},
					KeyValue{
						Key:   "abc",
						Value: "2",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
					KeyValue{
						Key:   "abc",
						Value: "1",
					},
				},
				[]KeyValue{
					KeyValue{
						Key:   "mo",
						Value: "2",
					},
					KeyValue{
						Key:   "abc",
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
			iter, err := parser.ToKeyValues(test.fctx, sel.Where.Expr, test.qualifier, nil)
			if err != nil {
				t.Error(err)
				return
			}
			if iter == nil {
				t.Error("iter is null")
				return
			}
			results, err := parser.ToKeyValueArray(test.ctx, iter)
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
