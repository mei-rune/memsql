package memsql

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/parser"
	"github.com/runner-mei/memsql/vm"
	"github.com/xwb1989/sqlparser"
)

type Value = memcore.Value
type Column = memcore.Column
type KeyValue = memcore.KeyValue
type Table = memcore.Table
type Record = memcore.Record
type RecordSet = memcore.RecordSet

type Foreign interface {
	From(ctx *SessionContext, tableName, tableAs string, where *sqlparser.Where) (memcore.Query, error)
}

type Storage interface {
	From(ctx *SessionContext, tableName, tableAs string, tableExpr sqlparser.Expr) (memcore.Query, []memcore.TableName, error)
	// Set(name string, tags []KeyValue, t time.Time, table Table, err error)
	// Exists(name string, tags []KeyValue) bool
}

func WrapStorage(storage memcore.Storage) Storage {
	return storageWrapper{storage: storage}
}

type storageWrapper struct {
	storage memcore.Storage
}

func (s storageWrapper) From(ctx *SessionContext, tableName, tableAs string, tableExpr sqlparser.Expr) (memcore.Query, []memcore.TableName, error) {
	return fromRun(ctx, s.storage, tableName, tableAs, tableExpr)
}

func fromRun(ctx *SessionContext, storage memcore.Storage, tableName, tableAs string, tableExpr sqlparser.Expr) (memcore.Query, []memcore.TableName, error) {
	var f = func(vm.Context) (bool, error) {
		return true, nil
	}
	if tableExpr != nil {
		ff, err := parser.ToFilter(ctx, tableExpr)
		if err != nil {
			return memcore.Query{}, nil, errors.Wrap(err, "couldn't convert tableExpr '"+sqlparser.String(tableExpr)+"'")
		}
		f = ff
	}

	return storage.From(ctx, tableName, f)
}

type Context struct {
	Ctx     context.Context
	Debuger ExecuteDebuger
	Storage Storage
	Foreign Foreign
}


type SessionContext struct {
	*Context

	closers []io.Closer
	alias map[string]string
	resultSets map[string][]memcore.Record
}

func (sc *SessionContext) SetResultSet(stmt string, records []memcore.Record) {
	sc.resultSets[stmt] = records
}

func (sc *SessionContext) GetResultSet(stmt string) ([]memcore.Record, bool) {
	results, ok := sc.resultSets[stmt]
	return results, ok
}

func (sc *SessionContext) ExecuteSelect(stmt sqlparser.SelectStatement) (memcore.Query, error) {
	return ExecuteSelectStatement(sc, stmt, false)
}

func (sc *SessionContext) addAlias(tableAlias, tableName string) error {
	_, ok := sc.alias[tableAlias]
	if ok {
		return errors.New("alias '"+tableAlias+"' is already exists")		
	}
	sc.alias[tableAlias] = tableName
	return nil
}

func (sc *SessionContext) OnClosing(closers ...io.Closer) {
	sc.closers = append(sc.closers, closers...)
}

func (session *SessionContext) Close() error {
	var errList []error
	for _, closer := range session.closers {
		if e := closer.Close(); e != nil {
			errList = append(errList, e)
		}
	}
	if len(errList) == 0 {
		return nil
	}
	if len(errList) == 1 {
		return errList[0]
	}
	var sb strings.Builder
	sb.WriteString("Multiple errors occur:")
	for _, err := range errList {
		sb.WriteString("\r\n\t")
		sb.WriteString(err.Error())
	}
	return errors.New(sb.String())
}

func Execute(ctx *Context, sqlstmt string) (rset RecordSet, err error) {
	stmt, e := parse(sqlstmt)
	if e != nil {
		return nil, e
	}
	sessctx := &SessionContext{
		Context: ctx,
		alias: map[string]string{},
	}
	defer func() {
		if e := sessctx.Close(); e != nil {
			err = e
		}
	}()

	query, e := ExecuteSelectStatement(sessctx, stmt, false)
	if e != nil {
		return nil, e
	}

	results, e := query.Results(sessctx)
	if e != nil {
		return nil, e
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
	Qualifier string
	Table     string
	As        string
}

func ExecuteSelectStatement(ec *SessionContext, stmt sqlparser.SelectStatement, hasJoin bool) (memcore.Query, error) {
	switch expr := stmt.(type) {
	case *sqlparser.Select:
		return ExecuteSelect(ec, expr, hasJoin)
	case *sqlparser.Union:
		return ExecuteUnion(ec, expr, hasJoin)
	case *sqlparser.ParenSelect:
		return ExecuteSelectStatement(ec, expr.Select, hasJoin)
	default:
		return memcore.Query{}, fmt.Errorf("invalid select %+v of type %T", stmt, stmt)
	}
}

func ExecuteUnion(ec *SessionContext, stmt *sqlparser.Union, hasJoin bool) (memcore.Query, error) {
	left, err := ExecuteSelectStatement(ec, stmt.Left, hasJoin)
	if err != nil {
		return memcore.Query{}, err
	}
	right, err := ExecuteSelectStatement(ec, stmt.Right, hasJoin)
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

func ExecuteSelect(ec *SessionContext, stmt *sqlparser.Select, hasJoin bool) (memcore.Query, error) {
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

	_, query, err := ExecuteTableExpression(ec, stmt.From[0], stmt.Where, hasJoin)
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

func ExecuteTableExpression(ec *SessionContext, expr sqlparser.TableExpr, where *sqlparser.Where, hasJoin bool) (string, memcore.Query, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ExecuteAliasedTableExpression(ec, expr, where, hasJoin)
	case *sqlparser.JoinTableExpr:
		return ExecuteJoinTableExpression(ec, expr, where)
	case *sqlparser.ParenTableExpr:
		query, err := ParseParenTableExpression(ec, expr, where)
		return "", query, err
	default:
		return "", memcore.Query{}, fmt.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ExecuteJoinTableExpression(ec *SessionContext, expr *sqlparser.JoinTableExpr, where *sqlparser.Where) (string, memcore.Query, error) {
	leftAs, query1, err := ExecuteTableExpression(ec, expr.LeftExpr, where, true)
	if err != nil {
		return "", memcore.Query{}, err
	}
	rightAs, query2, err := ExecuteTableExpression(ec, expr.RightExpr, where, true)
	if err != nil {
		return "", memcore.Query{}, err
	}

	leftOnAs, left, rightOnAs, right, err := ParseJoinOn(ec, expr.Condition.On)
	if err != nil {
		return "", memcore.Query{}, err
	}

	if leftAs == leftOnAs {
		if rightAs != rightOnAs {
			return "", memcore.Query{}, fmt.Errorf("invalid join table expression %+v: %s isnot exists", expr, rightOnAs)
		}
	} else if leftAs == rightOnAs {
		if rightAs != leftOnAs {
			return "", memcore.Query{}, fmt.Errorf("invalid join table expression %+v: %s isnot exists", expr, leftOnAs)
		}

		left, right = right, left
	} else {
		return "", memcore.Query{}, fmt.Errorf("invalid join table expression %+v: %s isnot exists", expr, leftAs)
	}

	switch expr.Join {
	case sqlparser.JoinStr:
		resultSelector := func(outer memcore.Record, inner Record) memcore.Record {
			return MergeRecord(leftAs, outer, rightAs, inner)
		}
		return "", query1.Join(false, query2, left, right, resultSelector), nil
	// case sqlparser.StraightJoinStr:
	case sqlparser.LeftJoinStr:
		resultSelector := func(outer memcore.Record, inner Record) memcore.Record {
			return MergeRecord(leftAs, outer, rightAs, inner)
		}
		return "", query1.Join(true, query2, left, right, resultSelector), nil
	case sqlparser.RightJoinStr:
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

func ParseJoinOn(ctx *SessionContext, on sqlparser.Expr) (
	leftAs string, left func(memcore.Record) (memcore.Value, error),
	rightAs string, right func(memcore.Record) (memcore.Value, error), err error) {
	cmp, ok := on.(*sqlparser.ComparisonExpr)
	if !ok {
		return "", nil, "", nil, fmt.Errorf("invalid On expression %+v", on)
	}
	leftCol, ok := cmp.Left.(*sqlparser.ColName)
	if !ok {
		return "", nil, "", nil, fmt.Errorf("invalid On expression %+v", on)
	}
	rightCol, ok := cmp.Right.(*sqlparser.ColName)
	if !ok {
		return "", nil, "", nil, fmt.Errorf("invalid On expression %+v", on)
	}

	leftValue, err := parser.ToGetValue(ctx, leftCol)
	if err != nil {
		return "", nil, "", nil, err
	}
	rightValue, err := parser.ToGetValue(ctx, rightCol)
	if err != nil {
		return "", nil, "", nil, err
	}
	if cmp.Operator != sqlparser.EqualStr {
		return "", nil, "", nil, fmt.Errorf("invalid On expression %+v", on)
	}
	return sqlparser.String(leftCol.Qualifier), func(r memcore.Record) (memcore.Value, error) {
			return leftValue(memcore.ToRecordValuer(&r, false))
		}, sqlparser.String(rightCol.Qualifier), func(r memcore.Record) (memcore.Value, error) {
			return rightValue(memcore.ToRecordValuer(&r, false))
		}, nil
}

func ParseParenTableExpression(ec *SessionContext, expr *sqlparser.ParenTableExpr, where *sqlparser.Where) (memcore.Query, error) {
	tableAs, query, err := ExecuteTableExpression(ec, expr.Exprs[0], where, true)
	if err != nil {
		return memcore.Query{}, err
	}

	for idx := 1; idx < len(expr.Exprs); idx++ {
		queryAs, query1, err := ExecuteTableExpression(ec, expr.Exprs[idx], where, true)
		if err != nil {
			return memcore.Query{}, err
		}

		resultSelector := func(outer memcore.Record, inner Record) memcore.Record {
			if idx == 1 {
				return MergeRecord(tableAs, outer, queryAs, inner)
			}
			return MergeRecord("", outer, queryAs, inner)
		}
		query = query.FullJoin(query1, resultSelector)
	}
	return query, nil
}

func ExecuteAliasedTableExpression(ec *SessionContext, expr *sqlparser.AliasedTableExpr, where *sqlparser.Where, hasJoin bool) (string, memcore.Query, error) {
	if len(expr.Partitions) > 0 {
		return "", memcore.Query{}, fmt.Errorf("invalid partitions in the table expression %+v", expr.Expr)
	}
	if expr.Hints != nil {
		return "", memcore.Query{}, fmt.Errorf("invalid index hits in the table expression %+v", expr.Expr)
	}
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		var ds Datasource
		ds.Qualifier = subExpr.Qualifier.String()
		ds.Table = subExpr.Name.String()
		if expr.As.IsEmpty() {
			ds.As = ds.Table
		} else {
			ds.As = expr.As.String()

			if err := ec.addAlias(ds.As, ds.Table); err != nil {
				return "", memcore.Query{}, err
			}
		}

		if where == nil {
			query, err := ExecuteTable(ec, ds, nil, hasJoin)
			if err == nil && !expr.As.IsEmpty() {
				query = query.Map(memcore.RenameTableToAlias(ds.As))
			}
			return ds.As, query, err
		}
		query, err := ExecuteTable(ec, ds, where, hasJoin)		
		if err == nil && !expr.As.IsEmpty() {
			query = query.Map(memcore.RenameTableToAlias(ds.As))
		}
		return ds.As, query, err
	case *sqlparser.Subquery:
		query, err := ExecuteSelectStatement(ec, subExpr.Select, hasJoin)
		return "", query, err
	default:
		return "", memcore.Query{}, fmt.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ExecuteTable(ec *SessionContext, ds Datasource, where *sqlparser.Where, hasJoin bool) (memcore.Query, error) {
	if ds.Qualifier == "db" {
		if where == nil || !hasJoin {
			return ec.Foreign.From(ec, strings.TrimPrefix(ds.Table, "db."), ds.As, where)
		}

		whereExpr, err := parser.SplitByTableName(where.Expr, ds.As)
		if err != nil {
			return memcore.Query{}, err
		}

		return ec.Foreign.From(ec, strings.TrimPrefix(ds.Table, "db."), ds.As, &sqlparser.Where{Expr: whereExpr})
	}

	var expr sqlparser.Expr
	if where != nil {
		expr = where.Expr
	}
	var tableExpr sqlparser.Expr
	if expr != nil {
		var err error
		if hasJoin {
			_, tableExpr, err = parser.SplitByColumnName(expr, parser.ByTableTag(ds.As))
		} else {
			_, tableExpr, err = parser.SplitByColumnName(expr, parser.ByTag())
		}
		if err != nil {
			return memcore.Query{}, errors.Wrap(err, "couldn't resolve where '"+sqlparser.String(expr)+"'")
		}
	}

	debuger := ec.Debuger.Table(ds.Table, ds.As, tableExpr)

	query, tableNames, err := ec.Storage.From(ec, ds.Table, ds.As, tableExpr)
	if err != nil {
		return memcore.Query{}, err
	}
	debuger.SetTableNames(tableNames)

	if !hasJoin {
		debuger.SetWhere(expr)
		return ExecuteWhere(ec, query, expr)
	}

	if expr == nil {
		debuger.SetWhere(nil)
		return ExecuteWhere(ec, query, nil)
	}
	
	whereExpr, err := parser.SplitByTableName(expr, ds.As)
	if err != nil {
		return memcore.Query{}, err
	}
	debuger.SetWhere(whereExpr)
	return ExecuteWhere(ec, query, whereExpr)
}

func ExecuteWhere(ec *SessionContext, query memcore.Query, expr sqlparser.Expr) (memcore.Query, error) {
	if expr == nil {
		return query, nil
	}

	f, err := parser.ToFilter(ec, expr)
	if err != nil {
		return memcore.Query{}, errors.Wrap(err, "couldn't convert where '"+sqlparser.String(expr)+"'")
	}
	query = query.Where(func(idx int, r memcore.Record) (bool, error) {
		return f(memcore.ToRecordValuer(&r, false))
	})

	// type Where Expr
	return query, nil
}

func ExecuteGroupBy(ec *SessionContext, query memcore.Query, groupBy sqlparser.GroupBy) (memcore.Query, error) {
	// type GroupBy []Expr

	// TODO: XXX
	return query, nil
}

func ExecuteHaving(ec *SessionContext, query memcore.Query, having *sqlparser.Where) (memcore.Query, error) {
	if having == nil {
		return query, nil
	}
	return ExecuteWhere(ec, query, having.Expr)
}

func ExecuteOrderBy(ec *SessionContext, query memcore.Query, orderBy sqlparser.OrderBy) (memcore.Query, error) {
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
			return read(memcore.ToRecordValuer(&r, false))
		})
	case sqlparser.DescScr:
		orderedQuery = query.OrderByDescending(func(r memcore.Record) (memcore.Value, error) {
			return read(memcore.ToRecordValuer(&r, false))
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
				return read(memcore.ToRecordValuer(&r, false))
			})
		case sqlparser.DescScr:
			orderedQuery = orderedQuery.ThenByDescending(func(r memcore.Record) (memcore.Value, error) {
				return read(memcore.ToRecordValuer(&r, false))
			})
		default:
			return memcore.Query{}, errors.New("invalid order by " + sqlparser.String(orderBy[0]))
		}
	}
	return orderedQuery.Query, nil
}

func ExecuteLimit(ec *SessionContext, query memcore.Query, limit *sqlparser.Limit) (memcore.Query, error) {
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

func ExecuteSelectExprs(ec *SessionContext, query memcore.Query, selectExprs sqlparser.SelectExprs) (memcore.Query, error) {
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
		selector := func(index int, r Record) (result Record, err error) {
			valuer := memcore.ToRecordValuer(&r, true)
			// valuer = vm.WrapAlias(valuer, ec.alias)
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

func toSelectFunc(as string, f func(vm.Context) (Value, error)) func(ctx vm.Context, result Record) (Record, error) {
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
	return nil, errors.New(funcName + "'" + as + "' is unsupported")
}

func toSelectAggOneFunc(idx int, as string, funcName string,
	f func() vm.Aggregator,
	readValue func(vm.Context) (Value, error)) (memcore.AggregatorFactoryFunc, error) {
	return memcore.AggregatorFunc(f, func(ctx memcore.Context, r memcore.Record) (vm.Value, error) {
		return readValue(memcore.ToRecordValuer(&r, false))
	}), nil
}

func MergeRecord(outerAs string, outer memcore.Record, innerAs string, inner Record) memcore.Record {
	result := memcore.Record{}
	result.Columns = make([]memcore.Column, len(outer.Columns)+len(inner.Columns))
	copy(result.Columns, outer.Columns)
	if outerAs != "" {
		for idx := range outer.Columns {
			result.Columns[idx].TableAs = outerAs
		}
	}
	copy(result.Columns[len(outer.Columns):], inner.Columns)
	if innerAs != "" {
		for idx := range outer.Columns {
			result.Columns[len(outer.Columns)+idx].TableAs = innerAs
		}
	}

	result.Values = make([]memcore.Value, len(outer.Values)+len(inner.Values))
	copy(result.Values, outer.Values)
	copy(result.Values[len(outer.Values):], inner.Values)
	return result
}
