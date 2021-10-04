package memsql

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/runner-mei/memsql/memcore"
	"github.com/xwb1989/sqlparser"
)

type Formater interface {
	Println(...interface{})
}

type StrFormater struct {
	sb strings.Builder
}

func (f *StrFormater) Println(args ...interface{}) {
	fmt.Fprintln(&f.sb, args...)
}

func (f *StrFormater) String() string {
	return f.sb.String()
}

type ExecuteDebuger struct {
	tables []*TableDebuger
	Results []string
}

func (d *ExecuteDebuger) String() string {
	var s StrFormater
	d.Format(&s)
	return s.String()
}

func (d *ExecuteDebuger) Format(formater Formater) {
	formater.Println("Tables: ")

	for idx := range d.tables {
		d.tables[idx].Format(formater)
	}

	formater.Println("Results: ")
	for idx := range d.Results {
		formater.Println("\t\t\t\t - ", d.Results[idx])
	}
}

func (d *ExecuteDebuger) Track(query memcore.Query) memcore.Query {
	return query.Map(func(ctx memcore.Context, r memcore.Record) (memcore.Record, error) {
		d.Results = append(d.Results, r.GoString())
		return r, nil
	})
}

func (d *ExecuteDebuger) NewTable(table, as string, expr sqlparser.Expr) *TableDebuger {
	t := &TableDebuger{}
	d.tables = append(d.tables, t)
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

	Results []string
}

func (d *TableDebuger) SetTableNames(tableNames []memcore.TableName) {
	d.TableNames = tableNames
}

func (d *TableDebuger) SetWhere(expr sqlparser.Expr) {
	if expr != nil {
		d.Where = sqlparser.String(expr)
	}
}

func (d *TableDebuger) Track(query memcore.Query) memcore.Query {
	return query.Map(func(ctx memcore.Context, r memcore.Record) (memcore.Record, error) {
		d.Results = append(d.Results, r.GoString())
		return r, nil
	})
}

func (d *TableDebuger) Format(formater Formater) {
	if d.As == "" || d.Table == d.As {
		formater.Println(uintptr(unsafe.Pointer(d)), "SELECT * FROM " + d.Table + " WHERE " + d.Where)
	} else {
		formater.Println(uintptr(unsafe.Pointer(d)), "SELECT * FROM " + d.Table + " AS " + d.As + " WHERE " + d.Where)
	}
	if len(d.TableNames) > 0 {
		formater.Println("\t\tTableFilter: " + d.TableFilter + ", Results:")
		for idx := range d.TableNames {
			formater.Println("\t\t\t\t - " + d.TableNames[idx].String())
		}
	}
	formater.Println("\t\tResults: ")
	for idx := range d.Results {
		formater.Println("\t\t\t\t - ", d.Results[idx])
	}
}