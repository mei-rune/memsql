package memsql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/vm"
	"github.com/xwb1989/sqlparser"
)

func NewDbForeign(drv string, conn *sql.DB) Foreign {
	return &dbForeign{
		Drv:  drv,
		Conn: conn,
	}
}

type dbForeign struct {
	Drv  string
	Conn *sql.DB
}

func (f *dbForeign) From(ctx *SessionContext, tableName TableAlias, where *sqlparser.Where) (memcore.Query, error) {
	sqlstr := "SELECT * FROM " + tableName.Name
	if tableName.Alias != "" {
		sqlstr = sqlstr + " AS " + tableName.Alias
	}

	debuger := ctx.Debuger.NewTable(tableName.Name, tableName.Alias, nil)
	if where != nil && where.Expr != nil {
		sqlstr = sqlstr +" WHERE "+ sqlparser.String(where.Expr)
		if debuger != nil {
			debuger.SetWhere(where.Expr)
		}
		if f.Drv == "sqlite3" {
			sqlstr = strings.Replace(sqlstr, "true", "1", -1)
			sqlstr = strings.Replace(sqlstr, "false", "0", -1)
		}
	}

	query := memcore.Query{
		Iterate: func() memcore.Iterator {
			rows, err := f.Conn.QueryContext(ctx.Ctx, sqlstr)
			if err != nil {
				return func(memcore.Context) ( memcore.Record, error) {
					return memcore.Record{}, wrap(err, "execute '"+sqlstr+"' fail")
				}
			}

			columnNames, err := rows.Columns()
			if err != nil {
				rows.Close()

				return func(memcore.Context) ( memcore.Record, error) {
					return memcore.Record{}, wrap(err, fmt.Sprintf("execute %q fail", sqlstr))
				}
			}

			// columnTypes, err := rows.ColumnTypes()
			// if err != nil {
			// 	return nil, memcore.Query{}, err
			// }

			var initFuncs = make([]func(*memcore.Value) interface{}, len(columnNames))
			var columns = make([]memcore.Column, len(columnNames))
			for idx := range columns {
				columns[idx].TableName = tableName.Name
				columns[idx].TableAs = tableName.Alias
				columns[idx].Name = columnNames[idx]
				initFuncs[idx] = func(value *memcore.Value) interface{} {
					return scanValue{
						value: value,
					}
				}
			}

			ctx.OnClosing(rows)

			var done = false
			var lastErr error

			return func(memcore.Context) (item memcore.Record, err error) {
				if done {
					err = lastErr
					return
				}
				if !rows.Next() {
					err = rows.Close()
					done = true
					if err != nil {
						lastErr = err
					} else {
						lastErr = memcore.ErrNoRows
						err = memcore.ErrNoRows
					}
					return
				}
				destValues := make([]memcore.Value, len(columns))
				dest := make([]interface{}, len(columns))
				for idx := range columns {
					dest[idx] = initFuncs[idx](&destValues[idx])
				}
				err = rows.Scan(dest...)
				if err != nil {
					rows.Close()
					done = true
					lastErr = err
					return
				}

				item.Columns = columns
				item.Values = destValues
				return
			}
		},
	}

	if tableName.Alias != "" {
		query = query.Map(RenameTableToAlias(tableName.Alias))
	}
	if debuger != nil {
		query = debuger.Track(query)
	}
	return query, nil
}

type scanValue struct {
	value *memcore.Value
}

func (sv scanValue) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case int8:
		sv.value.SetInt64(int64(v))
	case int16:
		sv.value.SetInt64(int64(v))
	case int32:
		sv.value.SetInt64(int64(v))
	case int64:
		sv.value.SetInt64(v)
	case int:
		sv.value.SetInt64(int64(v))
	case uint8:
		sv.value.SetUint64(uint64(v))
	case uint16:
		sv.value.SetUint64(uint64(v))
	case uint32:
		sv.value.SetUint64(uint64(v))
	case uint64:
		sv.value.SetUint64(v)
	case uint:
		sv.value.SetUint64(uint64(v))
	case float32:
		sv.value.SetFloat64(float64(v))
	case float64:
		sv.value.SetFloat64(v)
	case string:
		sv.value.SetString(v)
	case bool:
		sv.value.SetBool(v)
	case []byte:
		sv.value.SetString(string(v))
	default:
		return fmt.Errorf("unsupported type %T", v)
	}
	return nil
}

func RenameTableToAlias(alias string) func(memcore.Context, memcore.Record) (memcore.Record, error) {
	return func(ctx memcore.Context, r Record) (Record, error) {
		columns := make([]Column, len(r.Columns))
		copy(columns, r.Columns)
		for idx := range columns {
			columns[idx].TableAs = alias
		}
		return memcore.Record{
			Tags:    r.Tags,
			Columns: columns,
			Values:  r.Values,
		}, nil
	}
}

type recordValuer Record

func (r *recordValuer) GetValue(tableName, name string) (Value, error) {
	value, ok := (*Record)(r).Get(name)
	if ok {
		return value, nil
	}
	return vm.Null(), memcore.ColumnNotFound(tableName, name)
}

type recordValuerByQualifierName Record

func (r *recordValuerByQualifierName) GetValue(tableName, name string) (Value, error) {
	value, ok := (*Record)(r).GetByQualifierName(tableName, name)
	if ok {
		return value, nil
	}
	return vm.Null(), memcore.ColumnNotFound(tableName, name)
}

func ToRecordValuer(r *memcore.Record, withQualifier bool) vm.GetValuer {
	if withQualifier {
		return (*recordValuerByQualifierName)(r)
	}
	return (*recordValuer)(r)
}