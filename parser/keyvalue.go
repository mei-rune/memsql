package parser

import (
	"fmt"

	"github.com/runner-mei/memsql/memcore"
	"github.com/xwb1989/sqlparser"
)

func ToKeyValues(expr sqlparser.Expr, results []memcore.KeyValue) ([]memcore.KeyValue, error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		tmp, err := ToKeyValues(v.Left, results)
		if err != nil {
			return nil, err
		}
		tmp, err = ToKeyValues(v.Right, tmp)
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
		return ToKeyValues(v.Expr, results)

	case *sqlparser.ComparisonExpr:
		key, value, err := ToKeyValue(v)
		if err != nil {
			return nil, err
		}
		return append(results, memcore.KeyValue{Key: key, Value: value}), nil
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
	default:
		return nil, fmt.Errorf("invalid key value expression %+v", expr)
	}
}

func ToKeyValue(expr *sqlparser.ComparisonExpr) (string, string, error) {
	if expr.Operator != sqlparser.EqualStr {
		return "", "", fmt.Errorf("invalid key value expression %+v", expr)
	}

	left, ok := expr.Left.(*sqlparser.ColName)
	if ok {
		value, err := ToValueLiteral(expr.Right)
		if err != nil {
			return "", "", fmt.Errorf("invalid key value expression %+v, %+v", expr, err)
		}
		return sqlparser.String(left.Name), value, nil
	}

	right, ok := expr.Right.(*sqlparser.ColName)
	if ok {
		value, err := ToValueLiteral(expr.Left)
		if err != nil {
			return "", "", fmt.Errorf("invalid key value expression %+v, %+v", expr, err)
		}
		return sqlparser.String(right.Name), value, err
	}
	return "", "", fmt.Errorf("invalid key value expression %+v", expr)
}

func ToValueLiteral(expr sqlparser.Expr) (string, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			return string(v.Val), nil
		case sqlparser.IntVal:
			return string(v.Val), nil
		case sqlparser.FloatVal:
			return string(v.Val), nil
		case sqlparser.HexNum:
			return string(v.Val), nil
		case sqlparser.HexVal:
			return string(v.Val), nil
		case sqlparser.BitVal:
			return string(v.Val), nil
		case sqlparser.ValArg:
			return string(v.Val), nil
		default:
			return "", fmt.Errorf("invalid expression %+v", expr)
		}
	case *sqlparser.NullVal:
		return "null", nil
	case sqlparser.BoolVal:
		if bool(v) {
			return "true", nil
		}
		return "false", nil
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
		return "", fmt.Errorf("invalid expression %T %+v", expr, expr)
	}
}
