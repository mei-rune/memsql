package memsql

import (
	"encoding/json"

	"github.com/runner-mei/memsql/memcore"
	"github.com/xwb1989/sqlparser"
)

type ExecuteDebuger struct {
	tables []TableDebuger
}

func (d *ExecuteDebuger) String() string {
	bs, err := json.Marshal(d.tables)
	if err != nil {
		return "ExecuteDebuger:" + err.Error()
	}
	return string(bs)
}

func (d *ExecuteDebuger) Table(table, as string, expr sqlparser.Expr) *TableDebuger {
	d.tables = append(d.tables, TableDebuger{})
	t := &d.tables[len(d.tables)-1]
	t.Table = table
	t.As = as
	if expr != nil {
		t.TableFilter = sqlparser.String(expr)
	}
	return t
}

type TableDebuger struct {
	Table       string
	As          string
	TableFilter string
	TableNames  []memcore.TableName
	Where       string
}

func (d *TableDebuger) SetTableNames(tableNames []memcore.TableName) {
	d.TableNames = tableNames
}

func (d *TableDebuger) SetWhere(expr sqlparser.Expr) {
	if expr != nil {
		d.Where = sqlparser.String(expr)
	}
}
