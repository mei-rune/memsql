package parser

import (
	"fmt"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/vm"
	"github.com/xwb1989/sqlparser"
)

type StringIterator interface {
	Next(ctx vm.Context) (string, error)
}

type simpleStringIterator struct {
	value    string
	readable bool
}

func (simple *simpleStringIterator) Next(ctx vm.Context) (string, error) {
	if !simple.readable {
		return "", memcore.ErrNoRows
	}
	simple.readable = false
	return simple.value, nil
}

func toStringIterator(s string) StringIterator {
	return &simpleStringIterator{
		value:    s,
		readable: true,
	}
}

type stringList struct {
	list  []string
	index int
}

func (kl *stringList) Next(ctx vm.Context) (string, error) {
	if len(kl.list) >= kl.index {
		return "", memcore.ErrNoRows
	}
	return kl.list[kl.index], nil
}

func cloneStrings(ss []string) []string {
	c := make([]string, len(ss))
	copy(c, ss)
	return c
}

type unionStrs struct {
	query1, query2 StringIterator
	query1Done     bool
}

func (us *unionStrs) Next(ctx vm.Context) (string, error) {
	if !us.query1Done {
		s, err := us.query1.Next(ctx)
		if err == nil {
			return s, nil
		}
		if !memcore.IsNoRows(err) {
			return "", err
		}
		us.query1Done = true
	}
	return us.query2.Next(ctx)
}

func appendStringIterator(query1, query2 StringIterator) StringIterator {
	switch q1 := query1.(type) {
	case *stringList:
		switch q2 := query2.(type) {
		case *stringList:
			return &stringList{
				list: append(cloneStrings(q1.list[q1.index:]), q2.list[q2.index:]...),
			}
		case *simpleStringIterator:
			if !q2.readable {
				return q1
			}
			return &stringList{
				list: append(cloneStrings(q1.list[q1.index:]), q2.value),
			}
		}
	case *simpleStringIterator:
		if !q1.readable {
			return query2
		}
		switch q2 := query2.(type) {
		case *stringList:
			return &stringList{
				list: append(cloneStrings(q2.list[q2.index:]), q1.value),
			}
		case *simpleStringIterator:
			if !q2.readable {
				return q1
			}
			return &stringList{
				list: []string{q1.value, q2.value},
			}
		}
	}
	return &unionStrs{
		query1: query1,
		query2: query2,
	}
}

type KeyValueIterator interface {
	Next(ctx vm.Context) ([]memcore.KeyValue, error)
}

type keyValues struct {
	name  string
	query StringIterator
}

func (kvs *keyValues) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	value, err := kvs.query.Next(ctx)
	if err != nil {
		return nil, err
	}
	return []memcore.KeyValue{{Key: kvs.name, Value: value}}, nil
}

type kvList struct {
	list  [][]memcore.KeyValue
	index int
}

func (kl *kvList) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	if len(kl.list) >= kl.index {
		return nil, memcore.ErrNoRows
	}
	return kl.list[kl.index], nil
}

type simpleKv struct {
	values   []memcore.KeyValue
	readable bool
}

func (simple *simpleKv) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	if !simple.readable {
		return nil, memcore.ErrNoRows
	}
	simple.readable = false
	return simple.values, nil
}

type mergeIterator struct {
	query1, query2 KeyValueIterator

	done    bool
	readErr error
	inner   [][]memcore.KeyValue

	outer      []memcore.KeyValue
	innerLen   int
	innerIndex int
}

func (merge *mergeIterator) Next(ctx vm.Context) ([]memcore.KeyValue, error) {
	if !merge.done {
		if merge.readErr != nil {
			return nil, merge.readErr
		}
		for {
			kv, err := merge.query2.Next(ctx)
			if err != nil {
				if !memcore.IsNoRows(err) {
					merge.readErr = err
					return nil, err
				}
				break
			}
			merge.inner = append(merge.inner, kv)
		}
		merge.done = true
	}

	if merge.innerIndex >= merge.innerLen {
		has := false
		for !has {
			outer, err := merge.query1.Next(ctx)
			if err != nil {
				return nil, err
			}

			merge.outer = outer
			merge.innerLen = len(merge.inner)
			merge.innerIndex = 0
		}
	}

	items := append(merge.outer, merge.inner[merge.innerIndex]...)
	merge.innerIndex++
	return items, nil
}

func appendKeyValueIterator(query KeyValueIterator, kv ...memcore.KeyValue) KeyValueIterator {
	switch q := query.(type) {
	case *kvList:
		for idx := range q.list {
			q.list[idx] = append(q.list[idx], kv...)
		}
		return q
	case *simpleKv:
		return &simpleKv{
			values:   append(q.values, kv...),
			readable: q.readable,
		}
	default:
		return &mergeIterator{
			query1: query,
			query2: &simpleKv{values: kv, readable: true},
		}
	}
}

func ToKeyValues(fctx filterContext, expr sqlparser.Expr, qualifier string, results KeyValueIterator) (KeyValueIterator, error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		tmp, err := ToKeyValues(fctx, v.Left, qualifier, results)
		if err != nil {
			return nil, err
		}
		tmp, err = ToKeyValues(fctx, v.Right, qualifier, tmp)
		if err != nil {
			return nil, err
		}
		return tmp, nil
	// case *sqlparser.OrExpr:
	// 	leftFilter, err := ToFilter(ctx, v.Left)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	rightFilter, err := ToFilter(ctx, v.Right)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return vm.Or(leftFilter, rightFilter), nil
	// case *sqlparser.NotExpr:
	// 	f, err := ToFilter(ctx, v.Expr)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return vm.Not(f), nil
	case *sqlparser.ParenExpr:
		return ToKeyValues(fctx, v.Expr, qualifier, results)
	case *sqlparser.ComparisonExpr:
		if v.Operator == sqlparser.InStr {
			tableAs, iter, err := ToInKeyValue(fctx, v)
			if err != nil {
				return nil, err
			}
			if qualifier == tableAs {
				return results, nil
			}
			return &mergeIterator{
				query1: results,
				query2: iter,
			}, nil
		}
		if v.Operator != sqlparser.EqualStr {
			return nil, fmt.Errorf("invalid key value expression %+v", expr)
		}
		iter, err := ToEqualValues(fctx, v, qualifier)
		if err != nil {
			return nil, err
		}
		return &mergeIterator{
			query1: results,
			query2: iter,
		}, nil
		// case *sqlparser.RangeCond:
		// 	return nil, ErrUnsupportedExpr("RangeCond")
		// case *sqlparser.IsExpr:
		// 	return nil, ErrUnsupportedExpr("IsExpr")
		// case *sqlparser.ExistsExpr:
		// 	return nil, ErrUnsupportedExpr("ExistsExpr")
		// case *sqlparser.SQLVal:
		// 	return nil, ErrUnsupportedExpr("SQLVal")
		// case *sqlparser.NullVal:
		// 	return nil, ErrUnsupportedExpr("NullVal")
		// case sqlparser.BoolVal:
		// 	return nil, ErrUnsupportedExpr("BoolVal")
		// case *sqlparser.ColName:
		// 	return nil, ErrUnsupportedExpr("ColName")
		// case sqlparser.ValTuple:
		// 	return nil, ErrUnsupportedExpr("ValTuple")
		// case *sqlparser.Subquery:
		// 	return nil, ErrUnsupportedExpr("Subquery")
		// case sqlparser.ListArg:
		// 	return nil, ErrUnsupportedExpr("ListArg")
		// case *sqlparser.BinaryExpr:
		// 	return nil, ErrUnsupportedExpr("BinaryExpr")
		// case *sqlparser.UnaryExpr:
		// 	return nil, ErrUnsupportedExpr("UnaryExpr")
		// case *sqlparser.IntervalExpr:
		// 	return nil, ErrUnsupportedExpr("IntervalExpr")
		// case *sqlparser.CollateExpr:
		// 	return nil, ErrUnsupportedExpr("CollateExpr")
		// case *sqlparser.FuncExpr:
		// 	return nil, ErrUnsupportedExpr("FuncExpr")
		// case *sqlparser.CaseExpr:
		// 	return nil, ErrUnsupportedExpr("CaseExpr")
		// case *sqlparser.ValuesFuncExpr:
		// 	return nil, ErrUnsupportedExpr("ValuesFuncExpr")
		// case *sqlparser.ConvertExpr:
		// 	return nil, ErrUnsupportedExpr("ConvertExpr")
		// case *sqlparser.SubstrExpr:
		// 	return nil, ErrUnsupportedExpr("SubstrExpr")
		// case *sqlparser.ConvertUsingExpr:
		// 	return nil, ErrUnsupportedExpr("ConvertUsingExpr")
		// case *sqlparser.MatchExpr:
		// 	return nil, ErrUnsupportedExpr("MatchExpr")
		// case *sqlparser.GroupConcatExpr:
		// 	return nil, ErrUnsupportedExpr("GroupConcatExpr")
		// case *sqlparser.Default:
		// 	return nil, ErrUnsupportedExpr("Default")
	}
	return nil, fmt.Errorf("invalid key value expression %+v", expr)
}

func ToEqualValues(fctx filterContext, expr *sqlparser.ComparisonExpr, qualifier string) (KeyValueIterator, error) {
	left, leftok := expr.Left.(*sqlparser.ColName)
	right, rightok := expr.Right.(*sqlparser.ColName)

	if leftok && rightok {
		leftQualifier := sqlparser.String(left.Qualifier)
		rightQualifier := sqlparser.String(right.Qualifier)

		if qualifier == leftQualifier {
			if qualifier == rightQualifier {
				return nil, fmt.Errorf("invalid ComparisonExpr, left and right qualifier is same")
			}

			query, ok := fctx.GetQuery(rightQualifier)
			if !ok {
				return nil, fmt.Errorf("invalid key value expression %+v, %q is notfound", expr, rightQualifier)
			}

			return &keyValues{
				name: sqlparser.String(right.Name),
				query: &queryIterator{
					Qualifier: rightQualifier,
					Query:     query,
					key:       sqlparser.String(left.Name),
					field:     sqlparser.String(right.Name),
				},
			}, nil
		}
		if qualifier == rightQualifier {
			query, ok := fctx.GetQuery(leftQualifier)
			if !ok {
				return nil, fmt.Errorf("invalid key value expression %+v, %q is notfound", expr, rightQualifier)
			}

			return &keyValues{
				name: sqlparser.String(left.Name),
				query: &queryIterator{
					Qualifier: leftQualifier,
					Query:     query,
					key:       sqlparser.String(right.Name),
					field:     sqlparser.String(left.Name),
				},
			}, nil
		}

		return nil, fmt.Errorf("invalid key value expression %+v, %q is notfound", expr, rightQualifier)
	}

	if leftok {
		leftQualifier := sqlparser.String(left.Qualifier)
		if qualifier != leftQualifier {
			return nil, nil
		}

		_, key, value, err := ToKeyValue(fctx, left, expr.Right)
		if err != nil {
			return nil, err
		}

		return &simpleKv{
			values: []memcore.KeyValue{memcore.KeyValue{Key: key, Value: value}},
		}, nil
	}

	if rightok {
		rightQualifier := sqlparser.String(right.Qualifier)
		if qualifier != rightQualifier {
			return nil, nil
		}

		_, key, value, err := ToKeyValue(fctx, right, expr.Left)
		if err != nil {
			return nil, err
		}
		return &simpleKv{
			values: []memcore.KeyValue{memcore.KeyValue{Key: key, Value: value}},
		}, nil
	}
	return nil, fmt.Errorf("invalid key value expression %+v", expr)
}

func ToKeyValue(fctx filterContext, colName *sqlparser.ColName, expr sqlparser.Expr) (string, string, string, error) {
	value, err := ToValueLiteral(fctx, expr)
	if err != nil {
		return "", "", "", fmt.Errorf("invalid key value expression %+v, %+v", expr, err)
	}
	simple, ok := value.(*simpleStringIterator)
	if !ok {
		return "", "", "", fmt.Errorf("invalid key value expression %+v, %+v", expr, err)
	}
	return sqlparser.String(colName.Qualifier), sqlparser.String(colName.Name), simple.value, err
}

func ToInKeyValue(fctx filterContext, expr *sqlparser.ComparisonExpr) (string, KeyValueIterator, error) {
	left, ok := expr.Left.(*sqlparser.ColName)
	if ok {
		value, err := ToValueLiteral(fctx, expr.Right)
		if err != nil {
			return "", nil, fmt.Errorf("invalid key values expression %+v, %+v", expr, err)
		}
		return sqlparser.String(left.Qualifier), &keyValues{name: left.Name.String(), query: value}, nil
	}

	right, ok := expr.Right.(*sqlparser.ColName)
	if ok {
		value, err := ToValueLiteral(fctx, expr.Left)
		if err != nil {
			return "", nil, fmt.Errorf("invalid key values expression %+v, %+v", expr, err)
		}
		return sqlparser.String(right.Qualifier), &keyValues{name: right.Name.String(), query: value}, nil
	}
	return "", nil, fmt.Errorf("invalid key values expression %+v", expr)
}

func ToValueLiteral(fctx filterContext, expr sqlparser.Expr) (StringIterator, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return toStringIterator(string(v.Val)), nil
		case sqlparser.IntVal:
			return toStringIterator(string(v.Val)), nil
		case sqlparser.FloatVal:
			return toStringIterator(string(v.Val)), nil
		case sqlparser.HexNum:
			return toStringIterator(string(v.Val)), nil
		case sqlparser.HexVal:
			return toStringIterator(string(v.Val)), nil
		case sqlparser.BitVal:
			return toStringIterator(string(v.Val)), nil
		case sqlparser.ValArg:
			return toStringIterator(string(v.Val)), nil
		default:
			return nil, fmt.Errorf("invalid sqlval expression %+v", expr)
		}
	case *sqlparser.NullVal:
		return toStringIterator("null"), nil
	case sqlparser.BoolVal:
		if bool(v) {
			return toStringIterator("true"), nil
		}
		return toStringIterator("false"), nil
	// case *sqlparser.ColName:
	// 	return nil, ErrUnsupportedExpr("ColName")
	case sqlparser.ValTuple:
		var results StringIterator
		for idx := range []sqlparser.Expr(sqlparser.Exprs(v)) {
			strit, err := ToValueLiteral(fctx, v[idx])
			if err != nil {
				return nil, err
			}

			if results == nil {
				results = strit
			} else {
				results = appendStringIterator(results, strit)
			}
		}
		if results == nil {
			return nil, ErrUnsupportedExpr("ValTuple")
		}
		return results, nil
	case *sqlparser.Subquery:
		if fctx == nil {
			return nil, errors.New("fctx is nil")
		}
		return &subqueryStringIterator{
			fctx:     fctx,
			subquery: v.Select,
			key:      sqlparser.String(v.Select),
		}, nil
	// case sqlparser.ListArg:
	// 	return nil, ErrUnsupportedExpr("ListArg")
	// case *sqlparser.BinaryExpr:
	// 	return nil, ErrUnsupportedExpr("BinaryExpr")
	// case *sqlparser.UnaryExpr:
	// 	return nil, ErrUnsupportedExpr("UnaryExpr")
	// case *sqlparser.IntervalExpr:
	// 	return nil, ErrUnsupportedExpr("IntervalExpr")
	// case *sqlparser.CollateExpr:
	// 	return nil, ErrUnsupportedExpr("CollateExpr")
	// case *sqlparser.FuncExpr:
	// 	return nil, ErrUnsupportedExpr("FuncExpr")
	// case *sqlparser.CaseExpr:
	// 	return nil, ErrUnsupportedExpr("CaseExpr")
	// case *sqlparser.ValuesFuncExpr:
	// 	return nil, ErrUnsupportedExpr("ValuesFuncExpr")
	// case *sqlparser.ConvertExpr:
	// 	return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
	// case *sqlparser.SubstrExpr:
	// 	return nil, ErrUnsupportedExpr("SubstrExpr")
	// case *sqlparser.ConvertUsingExpr:
	// 	return nil, ErrUnsupportedExpr("ConvertUsingExpr")
	// case *sqlparser.MatchExpr:
	// 	return nil, ErrUnsupportedExpr("MatchExpr")
	// case *sqlparser.GroupConcatExpr:
	// 	return nil, ErrUnsupportedExpr("GroupConcatExpr")
	default:
		return nil, fmt.Errorf("invalid values expression %T %+v", expr, expr)
	}
}

type subqueryStringIterator struct {
	key      string
	fctx     filterContext
	subquery sqlparser.SelectStatement

	done    bool
	records []memcore.Record
	err     error

	index int
}

func (iter *subqueryStringIterator) Next(ctx vm.Context) (string, error) {
	if !iter.done {
		if iter.err != nil {
			return "", iter.err
		}
		q, err := iter.fctx.ExecuteSelect(iter.subquery)
		if err != nil {
			iter.err = err
			return "", err
		}
		records, err := q.Results(ctx)
		if err != nil {
			iter.err = err
			return "", err
		}
		iter.records = records
		iter.done = true
		iter.index = 0

		iter.fctx.SetResultSet(iter.key, iter.records)
	}

	if len(iter.records) >= iter.index {
		return "", memcore.ErrNoRows
	}

	s := iter.records[iter.index].At(0)
	iter.index++

	return s.AsString(true)
}

type queryIterator struct {
	Qualifier string
	Query     memcore.Query
	key       string
	field     string

	done    bool
	records []memcore.Record
	err     error

	index int
}

func (iter *queryIterator) Next(ctx vm.Context) (string, error) {
	if !iter.done {
		if iter.err != nil {
			return "", iter.err
		}
		records, err := iter.Query.Results(ctx)
		if err != nil {
			iter.err = err
			return "", err
		}
		iter.records = records
		iter.done = true
		iter.index = 0
	}

	for {
		if len(iter.records) >= iter.index {
			return "", memcore.ErrNoRows
		}
		item := iter.records[iter.index]
		iter.index++

		s, ok := item.Get(iter.field)
		if ok {
			return s.AsString(true)
		}
	}
}
