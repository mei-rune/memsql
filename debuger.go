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

type ReadInfo struct {
	Tags []memcore.KeyValue
	Method int
	Result interface{}
	Error error
}

const (
	ReadSkip = 0
	ReadOk = 1
	ReadError = 2
)

type ExecuteTracer struct {
	tables []*TableTracer
	Results []string

	Reads map[string][]ReadInfo
}

func (d *ExecuteTracer) String() string {
	var s StrFormater
	d.Format(&s)
	return s.String()
}

func (d *ExecuteTracer) Format(formater Formater) {
	formater.Println("Tables: ")
	for idx := range d.tables {
		d.tables[idx].Format(formater)
	}

	formater.Println("Results: ")
	for idx := range d.Results {
		formater.Println("\t\t\t\t - ", d.Results[idx])
	}

	if len(d.Reads) > 0 {
		formater.Println("Reads: ")
		for tableName, records := range d.Reads {
			formater.Println("\t\tTable: ", tableName)
			for _, record := range records {
				switch record.Method {
				case ReadSkip:
				formater.Println("\t\t\t\t Tags", memcore.KeyValues(record.Tags).ToKey(), "SKIPPED")
				case ReadOk:
				formater.Println("\t\t\t\t Tags", memcore.KeyValues(record.Tags).ToKey(), "READ OK:")
				formater.Println("\t\t\t\t\t\t",  record.Result)
				case ReadError:
				formater.Println("\t\t\t\t Tags", memcore.KeyValues(record.Tags).ToKey(), "READ ERROR:")
				formater.Println("\t\t\t\t\t\t", record.Error)
				default:
				formater.Println("\t\t\t\t Tags", memcore.KeyValues(record.Tags).ToKey(), "UNKNOWN")
				}
			}
		}
	}
}

func (d *ExecuteTracer) ReadSkip(tableName string, tags []memcore.KeyValue) {
   if d.Reads == nil {
  	d.Reads = map[string][]ReadInfo{}
  }
  d.Reads[tableName] = append(d.Reads[tableName], ReadInfo{
  	Tags: tags,
  	Method: ReadSkip,
  })
}
func (d *ExecuteTracer) ReadOk(tableName string, tags []memcore.KeyValue, value interface{}) {
   if d.Reads == nil {
  	d.Reads = map[string][]ReadInfo{}
  }
   d.Reads[tableName] = append(d.Reads[tableName], ReadInfo{
  	Tags: tags,
  	Method: ReadOk,
  	Result: value, 
  })
}
func (d *ExecuteTracer) ReadError(tableName string, tags []memcore.KeyValue, err error) {
  if d.Reads == nil {
  	d.Reads = map[string][]ReadInfo{}
  }
  d.Reads[tableName] = append(d.Reads[tableName], ReadInfo{
  	Tags: tags,
  	Method: ReadError,
  	Error: err, 
  })
}

func (d *ExecuteTracer) Track(query memcore.Query) memcore.Query {
	return query.Map(func(ctx memcore.Context, r memcore.Record) (memcore.Record, error) {
		d.Results = append(d.Results, r.GoString())
		return r, nil
	})
}

func (d *ExecuteTracer) NewTable(table, as string, expr sqlparser.Expr) *TableTracer {
	t := &TableTracer{}
	d.tables = append(d.tables, t)
	t.Table = table
	t.As = as
	if expr != nil {
		t.TableFilter = sqlparser.String(expr)
	}
	return t
}

type TableTracer struct {
	Table       string
	As          string
	TableFilter string
	TableNames  []memcore.TableName
	Where       string

	Results []string
}

func (d *TableTracer) SetTableNames(tableNames []memcore.TableName) {
	d.TableNames = tableNames
}

func (d *TableTracer) SetWhere(expr sqlparser.Expr) {
	if expr != nil {
		d.Where = sqlparser.String(expr)
	}
}

func (d *TableTracer) Track(query memcore.Query) memcore.Query {
	return query.Map(func(ctx memcore.Context, r memcore.Record) (memcore.Record, error) {
		d.Results = append(d.Results, r.GoString())
		return r, nil
	})
}

func (d *TableTracer) Format(formater Formater) {
	if d.As == "" || d.Table == d.As {
		if d.Where == "" {
			formater.Println(uintptr(unsafe.Pointer(d)), "SELECT * FROM " + d.Table)
		} else {
			formater.Println(uintptr(unsafe.Pointer(d)), "SELECT * FROM " + d.Table + " WHERE " + d.Where)
		}
	} else {
		if d.Where == "" {
		formater.Println(uintptr(unsafe.Pointer(d)), "SELECT * FROM " + d.Table + " AS " + d.As)
		} else{
		formater.Println(uintptr(unsafe.Pointer(d)), "SELECT * FROM " + d.Table + " AS " + d.As + " WHERE " + d.Where)
		}
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