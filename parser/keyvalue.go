package parser

import (
	"fmt"
	"strings"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore"
	"github.com/xwb1989/sqlparser"
)

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
			// fmt.Println(qualifier, tableAs, iter, err)
			if err != nil {
				return nil, err
			}
			// fmt.Println("2", qualifier, tableAs)
			if qualifier != tableAs {
				return results, nil
			}
			// fmt.Println("3")
			if results == nil {
				// fmt.Println("3.1", iter)
				return iter, nil
			}
			// fmt.Println("4")
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
		if results == nil {
			return iter, nil
		}

		// fmt.Println(fmt.Sprintf("%T %v", results, results), fmt.Sprintf("%T %v", iter, iter))
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
					key:       strings.TrimPrefix(sqlparser.String(left.Name), "@"),
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
					key:       strings.TrimPrefix(sqlparser.String(right.Name), "@"),
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

		key = strings.TrimPrefix(key, "@")
		return &simpleKv{
			values:   []memcore.KeyValue{memcore.KeyValue{Key: key, Value: value}},
			readable: true,
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
		key = strings.TrimPrefix(key, "@")
		return &simpleKv{
			values:   []memcore.KeyValue{memcore.KeyValue{Key: key, Value: value}},
			readable: true,
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

		name := strings.TrimPrefix(left.Name.String(), "@")
		return sqlparser.String(left.Qualifier), &keyValues{name: name, query: value}, nil
	}

	right, ok := expr.Right.(*sqlparser.ColName)
	if ok {
		value, err := ToValueLiteral(fctx, expr.Left)
		if err != nil {
			return "", nil, fmt.Errorf("invalid key values expression %+v, %+v", expr, err)
		}

		name := strings.TrimPrefix(right.Name.String(), "@")
		return sqlparser.String(right.Qualifier), &keyValues{name: name, query: value}, nil
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
