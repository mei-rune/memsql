package memsql

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/filter"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/parser"
	"github.com/xwb1989/sqlparser"
)

type Table = memcore.Table
type Record = memcore.Record
type RecordSet = memcore.RecordSet
type Storage = memcore.Storage

type Context struct {
	Ctx     context.Context
	Storage Storage
}

func Execute(ctx *Context, sqlstmt string) (RecordSet, error) {
	stmt, err := parse(sqlstmt)
	if err != nil {
		return nil, err
	}

	query, err := ExecuteSelectStatement(ctx, stmt)
	if err != nil {
		return nil, err
	}

	results, err := query.Results(ctx)
	if err != nil {
		return nil, err
	}

	return RecordSet(results), nil
}

func parse(sqlstr string) (sqlparser.SelectStatement, error) {
	stmt, err := sqlparser.Parse(sqlstr)
	if err != nil {
		return nil, err
	}
	// Otherwise do something with stmt
	selectStmt, ok := stmt.(sqlparser.SelectStatement)
	if !ok {
		return nil, errors.New("only support select statement")
	}
	return selectStmt, nil
}

type Datasource struct {
	Table string
	As    string
}

func ExecuteSelectStatement(ec *Context, stmt sqlparser.SelectStatement) (memcore.Query, error) {
	switch expr := stmt.(type) {
	case *sqlparser.Select:
		return ExecuteSelect(ec, expr)
	case *sqlparser.Union:
		return ExecuteUnion(ec, expr)
	case *sqlparser.ParenSelect:
		return ExecuteSelectStatement(ec, expr.Select)
	default:
		return memcore.Query{}, fmt.Errorf("invalid select %+v of type %T", stmt, stmt)
	}
}

func ExecuteUnion(ec *Context, stmt *sqlparser.Union) (memcore.Query, error) {
	left, err := ExecuteSelectStatement(ec, stmt.Left)
	if err != nil {
		return memcore.Query{}, err
	}
	right, err := ExecuteSelectStatement(ec, stmt.Right)
	if err != nil {
		return memcore.Query{}, err
	}

	var query memcore.Query
	switch stmt.Type {
	case sqlparser.UnionStr:
		query = left.Union(right)
	case sqlparser.UnionAllStr:
		query = left.UnionAll(right)
	// case sqlparser.UnionDistinctStr:
	default:
		return memcore.Query{}, fmt.Errorf("invalid union type %s", stmt.Type)
	}

	if len(stmt.OrderBy) > 0 {
		query, err = ExecuteOrderBy(ec, query, stmt.OrderBy)
		if err != nil {
			return memcore.Query{}, err
		}
	}

	if stmt.Limit != nil {
		query, err = ExecuteLimit(ec, query, stmt.Limit)
		if err != nil {
			return memcore.Query{}, err
		}
	}
	// if 	len(stmt.OrderBy) > 0 {
	// 	for idx := range stmt.OrderBy {
	// 		// Order represents an ordering expression.
	// 		type Order struct {
	// 			Expr      Expr
	// 			Direction string
	// 		}

	// 		// Order.Direction
	// 		const (
	// 			AscScr  = "asc"
	// 			DescScr = "desc"
	// 		)
	// 	}
	// }

	// if stmt.Limit != nil {
	// 	Offset,
	// 	Rowcount Expr
	// }

	return query, nil
}

func ExecuteSelect(ec *Context, stmt *sqlparser.Select) (memcore.Query, error) {
	if len(stmt.From) != 1 {
		return memcore.Query{}, fmt.Errorf("currently only one expression in from supported, got %v", len(stmt.From))
	}

	if stmt.Hints != "" {
		return memcore.Query{}, errors.New("currently unsupport hints")
	}

	if stmt.Lock != "" {
		return memcore.Query{}, errors.New("currently unsupport lock")
	}
	if stmt.Distinct != "" {
		return memcore.Query{}, errors.New("currently unsupport distinct")
	}

	// Where       *Where

	query, err := ExecuteTableExpression(ec, stmt.From[0], stmt.Where)
	if err != nil {
		return memcore.Query{}, errors.Wrap(err, "couldn't parse from expression")
	}

	if stmt.GroupBy != nil {
		query, err = ExecuteGroupBy(ec, query, stmt.GroupBy)
		if err != nil {
			return memcore.Query{}, err
		}

		if stmt.Having != nil {
			query, err = ExecuteHaving(ec, query, stmt.Having)
			if err != nil {
				return memcore.Query{}, err
			}
		}
	} else {
		if stmt.Having != nil {
			return memcore.Query{}, errors.New("currently unsupport having")
		}
	}

	if stmt.OrderBy != nil {
		query, err = ExecuteOrderBy(ec, query, stmt.OrderBy)
		if err != nil {
			return memcore.Query{}, err
		}
	}

	if stmt.Limit != nil {
		query, err = ExecuteLimit(ec, query, stmt.Limit)
		if err != nil {
			return memcore.Query{}, err
		}
	}

	return query, nil
}

func ExecuteTableExpression(ec *Context, expr sqlparser.TableExpr, where *sqlparser.Where) (memcore.Query, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ExecuteAliasedTableExpression(ec, expr, where)
	//case *sqlparser.JoinTableExpr:
	//	return ParseJoinTableExpression(ec, expr, where)
	// case *sqlparser.ParenTableExpr:
	// 	return ParseTableExpression(expr.Exprs[0])
	default:
		return memcore.Query{}, fmt.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

/*
func ExecuteJoinTableExpression(ec *Context, expr *sqlparser.JoinTableExpr, where *sqlparser.Where) error {
// 	type JoinTableExpr struct {
// 	LeftExpr  TableExpr
// 	Join      string
// 	RightExpr TableExpr
// 	Condition JoinCondition
// }

  err := ExecuteAliasedTableExpression(ec, expr.LeftExpr, where)
  if err != nil {
  	return err
  }

  err = ExecuteAliasedTableExpression(ec, expr.RightExpr, where)
  if err != nil {
  	return err
  }

  return nil
}
*/

func ExecuteAliasedTableExpression(ec *Context, expr *sqlparser.AliasedTableExpr, where *sqlparser.Where) (memcore.Query, error) {
	if len(expr.Partitions) > 0 {
		return memcore.Query{}, fmt.Errorf("invalid partitions in the table expression %+v", expr.Expr)
	}
	if expr.Hints != nil {
		return memcore.Query{}, fmt.Errorf("invalid index hits in the table expression %+v", expr.Expr)
	}
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		var ds Datasource
		ds.Table = subExpr.Name.String()
		if expr.As.IsEmpty() {
			ds.As = ds.Table
		} else {
			ds.As = expr.As.String()
		}

		if where == nil || where.Expr == nil {
			return ExecuteTable(ec, ds, nil)
		}
		return ExecuteTable(ec, ds, where.Expr)
		// if expr.As.IsEmpty() {
		// 	return nil, fmt.Errorf("table \"%v\" must have unique alias", subExpr.Name)
		// }
		// return logical.NewDataSource(subExpr.Name.String(), expr.As.String()), nil
	case *sqlparser.Subquery:
		return ExecuteSelectStatement(ec, subExpr.Select)
	default:
		return memcore.Query{}, fmt.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ExecuteTable(ec *Context, ds Datasource, expr sqlparser.Expr) (memcore.Query, error) {
	_, tableExpr, err := parser.SplitByColumnName(expr, parser.ByTag())
	if err != nil {
		return memcore.Query{}, errors.Wrap(err, "couldn't resolve where '"+sqlparser.String(expr)+"'")
	}

	var f = func(filter.Context) (bool, error) {
		return true, nil
	}
	if tableExpr != nil {
		f, err = parser.ToFilter(nil, tableExpr)
		if err != nil {
			return memcore.Query{}, errors.Wrap(err, "couldn't convert where '"+sqlparser.String(expr)+"'")
		}
	}

	query, err := ec.Storage.From(ec, ds.Table, f)
	if err != nil {
		return memcore.Query{}, err
	}

	return ExecuteWhere(ec, query, expr)
}

func ExecuteWhere(ec *Context, query memcore.Query, expr sqlparser.Expr) (memcore.Query, error) {
	if expr == nil {
		return query, nil
	}

	f, err := parser.ToFilter(ec, expr)
	if err != nil {
		return memcore.Query{}, errors.Wrap(err, "couldn't convert where '"+sqlparser.String(expr)+"'")
	}
	query = query.Where(func(idx int, r memcore.Record) (bool, error) {
		return f(memcore.ToRecordValuer(&r))
	})

	// type Where Expr
	return query, nil
}

func ExecuteGroupBy(ec *Context, query memcore.Query, groupBy sqlparser.GroupBy) (memcore.Query, error) {
	// type GroupBy []Expr

	// TODO: XXX
	return query, nil
}

func ExecuteHaving(ec *Context, query memcore.Query, having *sqlparser.Where) (memcore.Query, error) {
	if having == nil {
		return query, nil
	}
	return ExecuteWhere(ec, query, having.Expr)
}

func ExecuteOrderBy(ec *Context, query memcore.Query, orderBy sqlparser.OrderBy) (memcore.Query, error) {
	if len(orderBy) == 0 {
		return query, nil
	}

	read, err := parser.ToGetValue(ec, orderBy[0].Expr)
	if err != nil {
		return memcore.Query{}, err
	}

	var orderedQuery memcore.OrderedQuery
	switch orderBy[0].Direction {
	case sqlparser.AscScr, "":
		orderedQuery = query.OrderByAscending(func(r memcore.Record) (memcore.Value, error) {
			return read(memcore.ToRecordValuer(&r))
		})
	case sqlparser.DescScr:
		orderedQuery = query.OrderByDescending(func(r memcore.Record) (memcore.Value, error) {
			return read(memcore.ToRecordValuer(&r))
		})
	default:
		return memcore.Query{}, errors.New("invalid order by " + sqlparser.String(orderBy[0]))
	}

	for idx := 1; idx < len(orderBy); idx++ {
		read, err := parser.ToGetValue(ec, orderBy[idx].Expr)
		if err != nil {
			return memcore.Query{}, err
		}
		switch orderBy[idx].Direction {
		case sqlparser.AscScr, "":
			orderedQuery = orderedQuery.ThenByAscending(func(r memcore.Record) (memcore.Value, error) {
				return read(memcore.ToRecordValuer(&r))
			})
		case sqlparser.DescScr:
			orderedQuery = orderedQuery.ThenByDescending(func(r memcore.Record) (memcore.Value, error) {
				return read(memcore.ToRecordValuer(&r))
			})
		default:
			return memcore.Query{}, errors.New("invalid order by " + sqlparser.String(orderBy[0]))
		}
	}
	return orderedQuery.Query, nil
}

func asUint(value memcore.Value) (uint64, error) {
	switch value.Type {
	case memcore.ValueString:
		return strconv.ParseUint(value.Str, 10, 64)
	case memcore.ValueInt64:
		if value.Int64 < 0 {
			return 0, nil
		}
		return uint64(value.Int64), nil
	case memcore.ValueUint64:
		return value.Uint64, nil
	default:
		return 0, memcore.NewTypeMismatch(value.Type.String(), "unknown")
	}
}
func ExecuteLimit(ec *Context, query memcore.Query, limit *sqlparser.Limit) (memcore.Query, error) {
	if limit == nil {
		return query, nil
	}

	if limit.Offset != nil {
		readOffset, err := parser.ToGetValue(nil, limit.Offset)
		if err != nil {
			return query, err
		}

		offset, err := readOffset(nil)
		if err != nil {
			return query, err
		}

		i64, err := asUint(offset)
		if err != nil {
			return query, err
		}
		query = query.Skip(int(i64))
	}

	if limit.Rowcount != nil {
		readRowcount, err := parser.ToGetValue(nil, limit.Rowcount)
		if err != nil {
			return query, err
		}

		rowCount, err := readRowcount(nil)
		if err != nil {
			return query, err
		}

		i64, err := asUint(rowCount)
		if err != nil {
			return query, err
		}
		query = query.Take(int(i64))
	}

	return query, nil
}
