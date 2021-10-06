package memsql

import (
	"testing"

	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/parser"
	"github.com/runner-mei/memsql/vm"
	"github.com/xwb1989/sqlparser"
)

func TestFilter(t *testing.T) {
	// conn, err := sql.Open("sqlite3", ":memory:")
	// if err != nil {
	// 	t.Fatal(err)
	// 	return
	// }
	// defer conn.Close()

	// storage := memcore.NewStorage()

	// table, err := memcore.ToTable([]map[string]interface{}{
	// 	map[string]interface{}{
	// 		"id":   1,
	// 		"name": "1",
	// 	},
	// 	map[string]interface{}{
	// 		"id":   2,
	// 		"name": "3",
	// 	},
	// })
	// if err != nil {
	// 	t.Error(err)
	// 	return
	// }
	// storage.Set("mo_list", nil, time.Now(), table, nil)

	// ctx := &Context{
	// 	Ctx:     context.Background(),
	// 	Storage: WrapStorage(storage),
	// 	Foreign: NewDbForeign("sqlite3", conn),
	// }
	// fctx := &SessionContext{
	// 	Context:    ctx,
	// 	alias:      map[string]string{},
	// 	resultSets: map[string][]memcore.Record{},
	// }

	// query, _, err := storage.From(ctx, "mo_list", func(ctx memcore.GetValuer) (bool, error){
	// 	return true, nil
	// })
	// fctx.addQuery("mo_list", "mo", query)

	// opts := cmp.Options{
	// 	cmpopts.EquateApproxTime(1 * time.Second),
	// }

	for _, test := range []struct {
		fctx   parser.FilterContext
		sql    string
		values map[string]map[string]vm.Value
		result bool
	}{
		{
			sql: "select * from cpu where a = 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(2),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a = 2 and b = 3",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(2),
					"b": vm.IntToValue(3),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a = 2 or b = 3",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(3),
					"b": vm.IntToValue(3),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a in (1,2)",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(2),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a not in (1,2)",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(4),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a > 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(2),
				},
			},
			result: false,
		},
		{
			sql: "select * from cpu where a > 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(4),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a >= 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(2),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a >= 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(4),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a < 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(2),
				},
			},
			result: false,
		},
		{
			sql: "select * from cpu where a < 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(1),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a <= 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(2),
				},
			},
			result: true,
		},
		{
			sql: "select * from cpu where a <= 2",
			values: map[string]map[string]vm.Value{
				"": map[string]vm.Value{
					"a": vm.IntToValue(1),
				},
			},
			result: true,
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

			predicate, err := parser.ToFilter(test.fctx, sel.Where.Expr)
			if err != nil {
				t.Error(err)
				return
			}
			if predicate == nil {
				t.Error("predicate is null")
				return
			}

			ctx := vm.GetValueFunc(func(tableName, name string) (Value, error) {
				values := test.values[tableName]
				if values == nil {
					return vm.Null(), memcore.ColumnNotFound(tableName, name)
				}
				value, ok := values[name]
				if !ok {
					return vm.Null(), memcore.ColumnNotFound(tableName, name)
				}
				return value, nil
			})
			result, err := predicate(ctx)
			if err != nil {
				t.Error(err)
				return
			}

			if result != test.result {
				t.Error("want", test.result, "got", result)
			}
		})
	}

}
