package memsql

import (
	"context"
	"fmt"
	"reflect"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/vm"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/parser"
	"github.com/xwb1989/sqlparser"
)

type Value = memcore.Value
type Column = memcore.Column
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

	_, query, err := ExecuteTableExpression(ec, stmt.From[0], stmt.Where)
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

	if stmt.SelectExprs != nil {
		query, err = ExecuteSelectExprs(ec, query, stmt.SelectExprs)
		if err != nil {
			return memcore.Query{}, err
		}
	}
	return query, nil
}

func ExecuteTableExpression(ec *Context, expr sqlparser.TableExpr, where *sqlparser.Where) (string, memcore.Query, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ExecuteAliasedTableExpression(ec, expr, where)
	case *sqlparser.JoinTableExpr:
		return ExecuteJoinTableExpression(ec, expr, where)
	case *sqlparser.ParenTableExpr:
		query, err := ParseParenTableExpression(ec, expr, where)
	 	return "", query, err
	default:
		return "", memcore.Query{}, fmt.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}


func ExecuteJoinTableExpression(ec *Context, expr *sqlparser.JoinTableExpr, where *sqlparser.Where) (string, memcore.Query, error) {
  leftAs, query1, err := ExecuteTableExpression(ec, expr.LeftExpr, where)
  if err != nil {
  	return "", memcore.Query{}, err
  }
  rightAs, query2, err := ExecuteTableExpression(ec, expr.RightExpr, where)
  if err != nil {
  	return "", memcore.Query{}, err
  }

  switch expr.Join {
  case sqlparser.JoinStr:
  	left, right, err := ParseJoinOn(ec, expr.Condition.On)
	  if err != nil {
	  	return "", memcore.Query{}, err
	  }
	  resultSelector := func(outer memcore.Record, inner Record) memcore.Record {
	  	return MergeRecord(leftAs, outer, rightAs, inner)
	  }
	  return "", query1.Join(false, query2, left, right, resultSelector), nil

	// case sqlparser.StraightJoinStr:
	case sqlparser.LeftJoinStr:
  	left, right, err := ParseJoinOn(ec, expr.Condition.On)
	  if err != nil {
	  	return "", memcore.Query{}, err
	  }
	  resultSelector := func(outer memcore.Record, inner Record) memcore.Record {
	  	return MergeRecord(leftAs, outer, rightAs, inner)
	  }
	  return "", query1.Join(true, query2, left, right, resultSelector), nil
	case sqlparser.RightJoinStr:
  	left, right, err := ParseJoinOn(ec, expr.Condition.On)
	  if err != nil {
	  	return "", memcore.Query{}, err
	  }
	  resultSelector := func(outer memcore.Record, inner Record) memcore.Record {
	  	return MergeRecord(rightAs, inner, leftAs, outer)
	  }
	  return "", query2.Join(true, query1, right, left, resultSelector), nil
	// case sqlparser.NaturalJoinStr:
	// case sqlparser.NaturalLeftJoinStr:
	// case sqlparser.NaturalRightJoinStr:
	default:		
		return "", memcore.Query{}, fmt.Errorf("invalid join table expression %+v of type %v", expr, reflect.TypeOf(expr))
  }
}

func ParseJoinOn(ctx *Context, on sqlparser.Expr) (left, right func(memcore.Record) (memcore.Value, error), err error) {
	cmp, ok := on.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil, nil, fmt.Errorf("invalid On expression %+v", on)
	}

	leftValue, err := parser.ToGetValue(ctx, cmp.Left)
	if err != nil {
		return nil, nil, err
	}
	rightValue, err := parser.ToGetValue(ctx, cmp.Right)
	if err != nil {
		return nil, nil, err
	}
	if cmp.Operator != sqlparser.EqualStr {
		return nil, nil, fmt.Errorf("invalid On expression %+v", on)
	}
	return func(r memcore.Record) (memcore.Value, error) {
		return leftValue(memcore.ToRecordValuer(&r))
	}, func(r memcore.Record) (memcore.Value, error) {
		return rightValue(memcore.ToRecordValuer(&r))
	}, nil
}

func ParseParenTableExpression(ec *Context, expr *sqlparser.ParenTableExpr, where *sqlparser.Where) (memcore.Query, error) {
	_, query, err := ExecuteTableExpression(ec, expr.Exprs[0], where)
	if err != nil {
		return memcore.Query{}, err
	}

	for idx := 1; idx < len(expr.Exprs); idx ++ {
		_, query1, err := ExecuteTableExpression(ec, expr.Exprs[idx], where)
		if err != nil {
			return memcore.Query{}, err
		}

	  resultSelector := func(outer memcore.Record, inner Record) memcore.Record {
	  	return MergeRecord("", outer, "", inner)
	  }
		query = query.FullJoin(query1, resultSelector)
	}
	return query, nil
}

func ExecuteAliasedTableExpression(ec *Context, expr *sqlparser.AliasedTableExpr, where *sqlparser.Where) (string, memcore.Query, error) {
	if len(expr.Partitions) > 0 {
		return "", memcore.Query{}, fmt.Errorf("invalid partitions in the table expression %+v", expr.Expr)
	}
	if expr.Hints != nil {
		return "", memcore.Query{}, fmt.Errorf("invalid index hits in the table expression %+v", expr.Expr)
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

		if where == nil {
			query, err := ExecuteTable(ec, ds, nil)
			return ds.As, query, err
		}
		query, err := ExecuteTable(ec, ds, where.Expr)
		return ds.As, query, err
	case *sqlparser.Subquery:
		query, err := ExecuteSelectStatement(ec, subExpr.Select)
		return "", query, err
	default:
		return "", memcore.Query{}, fmt.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ExecuteTable(ec *Context, ds Datasource, expr sqlparser.Expr) (memcore.Query, error) {
	var f = func(vm.Context) (bool, error) {
		return true, nil
	}
	if expr != nil {
		_, tableExpr, err := parser.SplitByColumnName(expr, parser.ByTag())
		if err != nil {
			return memcore.Query{}, errors.Wrap(err, "couldn't resolve where '"+sqlparser.String(expr)+"'")
		}
		if tableExpr != nil {
			f, err = parser.ToFilter(nil, tableExpr)
			if err != nil {
				return memcore.Query{}, errors.Wrap(err, "couldn't convert where '"+sqlparser.String(expr)+"'")
			}
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

		i64, err := offset.AsUint(true)
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

		i64, err := rowCount.AsUint(true)
		if err != nil {
			return query, err
		}
		query = query.Take(int(i64))
	}

	return query, nil
}


func ExecuteSelectExprs(ec *Context, query memcore.Query, selectExprs sqlparser.SelectExprs) (memcore.Query, error) {
	switch len(selectExprs) {
	case 0:
		return query, nil
	case 1:
		_, ok := selectExprs[0].(*sqlparser.StarExpr)
		if ok {
			return query, nil
		}
	}

	var aggAsNames []string
	var aggFuncs []memcore.AggregatorFactory
	var selectFuncs []func(vm.Context, Record) (Record, error)
	for idx := range selectExprs {
		subexpr := selectExprs[idx]
		switch v := subexpr.(type) {
		case *sqlparser.StarExpr:
			return query, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
		case *sqlparser.AliasedExpr:
			if subexpr, ok := v.Expr.(*sqlparser.FuncExpr); ok {
				aggFunc, ok := vm.AggFuncs[subexpr.Name.String()]
				if ok {
					if len(subexpr.Exprs) == 0 {
						return query, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
					}
					if len(subexpr.Exprs) == 1 {
						var readValue func(vm.Context) (vm.Value, error)
						if _, ok := subexpr.Exprs[0].(*sqlparser.StarExpr); ok {
							readValue = func(vm.Context) (vm.Value, error) {
								return vm.IntToValue(1), nil
							}
						} else {
							var err error
							readValue, err = parser.ToGetSelectValue(ec, subexpr.Exprs[0])
							if err != nil {
								return query, err
							}
						}
						if v.As.IsEmpty() {
							aggAsNames = append(aggAsNames, sqlparser.String(v))
						} else {
							aggAsNames = append(aggAsNames, v.As.String())
						}

						aggFunc, err := toSelectAggOneFunc(idx, v.As.String(), subexpr.Name.String(), aggFunc, readValue)
						if err != nil {
							return query, err
						}

						aggFuncs = append(aggFuncs, aggFunc)
						break
					}

					readValues, err := parser.ToGetValues(ec, subexpr.Exprs)
					if err != nil {
						return query, err
					}

					if v.As.IsEmpty() {
						aggAsNames = append(aggAsNames, sqlparser.String(v))
					} else {
						aggAsNames = append(aggAsNames, v.As.String())
					}

					aggFunc, err := toSelectAggFunc(idx, v.As.String(), subexpr.Name.String(), aggFunc, readValues)
					if err != nil {
						return query, err
					}
					aggFuncs = append(aggFuncs, aggFunc)
					break
				}
				f, err := parser.ToFuncGetValue(ec, subexpr)
				if err != nil {
					return query, err
				}
				selectFuncs = append(selectFuncs, toSelectFunc(v.As.String(), f))
				break
			}

			f, err := parser.ToGetValue(ec, v.Expr)
			if err != nil {
				return query, err
			}
			selectFuncs = append(selectFuncs, toSelectFunc(v.As.String(), f))
		case sqlparser.Nextval:
			return query, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
		default:
			return query, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
		}
	}

	if len(selectFuncs) > 0 {
		if len(aggFuncs) > 0 {
			return query, errors.New("agg function and nonagg function exist simultaneously")
		}
		selector := func(index int, r Record) (result Record, err error){
			valuer := memcore.ToRecordValuer(&r)
			for _, f := range selectFuncs {
				result, err = f(valuer, result)
			}
			return result, nil
		}
		return query.Select(selector), nil 
	}

	if len(aggFuncs) > 0 {
		return query.AggregateWith(aggAsNames, aggFuncs), nil 
	}

	return query, nil
}

func toSelectFunc(as string, f func(vm.Context) (Value, error)) func(ctx vm.Context, result Record) (Record, error)  {
	return func(ctx vm.Context, result Record) (Record, error) {
				value, err := f(ctx)
				if err != nil {
					return Record{}, err
				}
				result.Columns = append(result.Columns, Column{Name: as})
				result.Values = append(result.Values, value)
				return result, nil 
		}
}

func toSelectAggFunc(idx int, as string, funcName string,
	f func() vm.Aggregator, 
	readValues func(vm.Context) ([]Value, error)) (memcore.AggregatorFactoryFunc, error) {
	return nil, errors.New(funcName+"'"+as+"' is unsupported")
}

func toSelectAggOneFunc(idx int, as string, funcName string,
	f func() vm.Aggregator, 
	readValue func(vm.Context) (Value, error)) (memcore.AggregatorFactoryFunc, error)  {
	return memcore.AggregatorFunc(f, func(ctx memcore.Context, r memcore.Record) (vm.Value, error) {
		 return readValue(memcore.ToRecordValuer(&r))
	}), nil 
}

func MergeRecord(outerAs string, outer memcore.Record, innerAs string, inner Record) memcore.Record {
	result := memcore.Record{}
	result.Columns = make([]memcore.Column, len(outer.Columns) + len(inner.Columns))
	copy(result.Columns, outer.Columns)
	if outerAs != "" {
		for idx := range outer.Columns {
			result.Columns[idx].TableAs = outerAs
		}
	}
	copy(result.Columns[len(outer.Columns):], inner.Columns)
	if innerAs != "" {
		for idx := range outer.Columns {
			result.Columns[len(outer.Columns) + idx].TableAs = innerAs
		}
	}

	result.Values = make([]memcore.Value, len(outer.Values) + len(inner.Values))
	copy(result.Values, outer.Values)
	copy(result.Values[len(outer.Values):], inner.Values)
	return result
}