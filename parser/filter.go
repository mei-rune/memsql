package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/runner-mei/memsql"
  "github.com/runner-mei/memsql/filter"
	"github.com/xwb1989/sqlparser"
)

type filterContext interface {}

func ToFilter(ctx filterContext, expr sqlparser.Expr) (func(filter.Context) (bool, error), error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		leftFilter, err := ToFilter(ctx, v.Left)
		if err != nil {
			return nil, err
		}
		rightFilter, err := ToFilter(ctx, v.Right)
		if err != nil {
			return nil, err
		}
		return filter.And(leftFilter, rightFilter), nil
	case *sqlparser.OrExpr:
		leftFilter, err := ToFilter(ctx, v.Left)
		if err != nil {
			return nil, err
		}
		rightFilter, err := ToFilter(ctx, v.Right)
		if err != nil {
			return nil, err
		}
		return filter.Or(leftFilter, rightFilter), nil
	case *sqlparser.NotExpr:
		f, err := ToFilter(ctx, v.Expr)
		if err != nil {
			return nil, err
		}
		return filter.Not(f), nil
	case *sqlparser.ParenExpr:
		return ToFilter(ctx, v.Expr)
	case *sqlparser.ComparisonExpr:
		leftValue, err := ToGetValue(ctx, v.Left)
		if err != nil {
			return nil, err
		}
		if v.Operator == sqlparser.InStr {
			rightValues, err := ToGetValues(ctx, v.Right)
			if err != nil {
				return nil, err
			}
			return filter.In(leftValue, rightValues)
		}
		if v.Operator == sqlparser.NotInStr {
			rightValues, err := ToGetValues(ctx, v.Right)
			if err != nil {
				return nil, err
			}
			return filter.NotIn(leftValue, rightValues)
		}

		rightValue, err := ToGetValue(ctx, v.Right)
		if err != nil {
			return nil, err
		}
		switch v.Operator {
		case sqlparser.EqualStr:
			return filter.Equal(leftValue, rightValue)
		case sqlparser.LessThanStr:
			return filter.LessThan(leftValue, rightValue)
		case sqlparser.GreaterThanStr:
			return filter.GreaterThan(leftValue, rightValue)
		case sqlparser.LessEqualStr:
			return filter.LessEqual(leftValue, rightValue)
		case sqlparser.GreaterEqualStr:
			return filter.GreaterEqual(leftValue, rightValue)
		case sqlparser.NotEqualStr:
			return filter.NotEqual(leftValue, rightValue)
		// case sqlparser.InStr:
		// case sqlparser.NotInStr:
		case sqlparser.NullSafeEqualStr:
			return nil, errUnknownOperator(v.Operator)
		case sqlparser.LikeStr:
			return filter.Like(leftValue, rightValue)
		case sqlparser.NotLikeStr:
			return filter.NotLike(leftValue, rightValue)
		case sqlparser.RegexpStr:
			return filter.Regexp(leftValue, rightValue)
		case sqlparser.NotRegexpStr:
			return filter.NotRegexp(leftValue, rightValue)
		case sqlparser.JSONExtractOp:
			return nil, errUnknownOperator(v.Operator)
		case sqlparser.JSONUnquoteExtractOp:
			return nil, errUnknownOperator(v.Operator)
		default:
			return nil, errUnknownOperator(v.Operator)
		}
	case *sqlparser.RangeCond:
		leftValue, err := ToGetValue(ctx, v.Left)
		if err != nil {
			return nil, err
		}
		fromValue, err := ToGetValue(ctx, v.From)
		if err != nil {
			return nil, err
		}
		toValue, err := ToGetValue(ctx, v.To)
		if err != nil {
			return nil, err
		}

		if v.Operator == sqlparser.BetweenStr {
			return filter.Between(leftValue, fromValue, toValue)
		}
		if v.Operator == sqlparser.NotBetweenStr {
			return filter.NotBetween(leftValue, fromValue, toValue)
		}
		return nil, errUnknownOperator(v.Operator)
	case *sqlparser.IsExpr:
		value, err := ToGetValue(ctx, v.Left)
		if err != nil {
			return nil, err
		}
		switch v.Operator {
		case sqlparser.IsNullStr:
			return filter.IsNull(value)
		case sqlparser.IsNotNullStr:
			return filter.IsNotNull(value)
		case sqlparser.IsTrueStr:
			return filter.IsTrue(value)
		case sqlparser.IsNotTrueStr:
			return filter.IsNoTrue(value)
		case sqlparser.IsFalseStr:
			return filter.IsFalse(value)
		case sqlparser.IsNotFalseStr:
			return filter.IsNotFalse(value)
		}
		return nil, errUnknownOperator(v.Operator)
	case *sqlparser.ExistsExpr:
		return nil, ErrUnsupportedExpr("ExistsExpr")
	case *sqlparser.SQLVal:
		return nil, ErrUnsupportedExpr("SQLVal")
	case *sqlparser.NullVal:
		return nil, ErrUnsupportedExpr("NullVal")
	case sqlparser.BoolVal:
		return nil, ErrUnsupportedExpr("BoolVal")
	case *sqlparser.ColName:
		return nil, ErrUnsupportedExpr("ColName")
	case sqlparser.ValTuple:
		return nil, ErrUnsupportedExpr("ValTuple")
	case *sqlparser.Subquery:
		return nil, ErrUnsupportedExpr("Subquery")
	case sqlparser.ListArg:
		return nil, ErrUnsupportedExpr("ListArg")
	case *sqlparser.BinaryExpr:
		return nil, ErrUnsupportedExpr("BinaryExpr")
	case *sqlparser.UnaryExpr:
		return nil, ErrUnsupportedExpr("UnaryExpr")
	case *sqlparser.IntervalExpr:
		return nil, ErrUnsupportedExpr("IntervalExpr")
	case *sqlparser.CollateExpr:
		return nil, ErrUnsupportedExpr("CollateExpr")
	case *sqlparser.FuncExpr:
		return nil, ErrUnsupportedExpr("FuncExpr")
	case *sqlparser.CaseExpr:
		return nil, ErrUnsupportedExpr("CaseExpr")
	case *sqlparser.ValuesFuncExpr:
		return nil, ErrUnsupportedExpr("ValuesFuncExpr")
	case *sqlparser.ConvertExpr:
		return nil, ErrUnsupportedExpr("ConvertExpr")
	case *sqlparser.SubstrExpr:
		return nil, ErrUnsupportedExpr("SubstrExpr")
	case *sqlparser.ConvertUsingExpr:
		return nil, ErrUnsupportedExpr("ConvertUsingExpr")
	case *sqlparser.MatchExpr:
		return nil, ErrUnsupportedExpr("MatchExpr")
	case *sqlparser.GroupConcatExpr:
		return nil, ErrUnsupportedExpr("GroupConcatExpr")
	case *sqlparser.Default:
		return nil, ErrUnsupportedExpr("Default")
	default:
		return nil, fmt.Errorf("invalid expression %+v", expr)
	}
}

func ToGetValue(ctx filterContext, expr sqlparser.Expr) (func(filter.Context) (memsql.Value, error), error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch node.Type {
		case sqlparser.StrVal:
			s := string(v.Val)
			return func(filter.Context) (memsql.Value, error) {
				return memsql.StringToValue(s), nil
			}, nil
		case sqlparser.IntVal:
			s := string(v.Val)
			i64, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				return func(filter.Context) (memsql.Value, error) {
					return IntToValue(i64), nil
				}, nil
			}
			u64, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				return func(filter.Context) (memsql.Value, error) {
					return UintToValue(i64), nil
				}, nil
			}
			return nil, newTypeError(s, "int")
		case sqlparser.FloatVal:
			s := string(v.Val)
			f64, err := strconv.ParseFloat(s, 64)
			if err == nil {
				return func(filter.Context) (memsql.Value, error) {
					return FloatToValue(f64), nil
				}, nil
			}
			return nil, newTypeError(s, "float")
		case sqlparser.HexNum:
			return nil, newTypeError(s, "HexNum")
		case HexVal:
			return nil, newTypeError(s, "HexVal")
		case BitVal:
			return nil, newTypeError(s, "BitVal")
		case ValArg:
			return nil, newTypeError(s, "ValArg")
		default:
			return nil, fmt.Errorf("invalid expression %+v", expr)
		}
	case *sqlparser.NullVal:
		return func(filter.Context) (memsql.Value, error) {
			return memsql.Null(), nil
		}, nil
	case sqlparser.BoolVal:
		bValue := bool(v)
		return func(filter.Context) (memsql.Value, error) {
			return memsql.BoolToValue(bValue), nil
		}, nil

	case *sqlparser.ColName:
		var name = strings.ToLower(v.Name.String())
		var tableName = strings.ToLower(qualifier.Name.String())
		var tableQualifier = strings.ToLower(qualifier.Qualifier.String())
		if tableName == "" {
			return func(ctx filter.Context) (memsql.Value, error) {
				return ctx.GetValue(tableName, name)
			}, nil
		}
		if tableQualifier == "" {
			return func(ctx filter.Context) (memsql.Value, error) {
				return ctx.GetValue(tableQualifier, name)
			}, nil
		}
		return func(ctx filter.Context) (memsql.Value, error) {
			return ctx.GetValue("", name)
		}, nil
	default:
		return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
	}
}

func ToGetValues(ctx filterContext, expr sqlparser.Expr) (func(filter.Context) ([]memsql.Value, error), error) {
	switch v := expr.(type) {
	case sqlparser.SelectExprs:
		var funcs []func(filter.Context) (memsql.Value, error)
		for idx := range v {
			switch subexpr := v[idx].(type) {
			case *sqlparser.StarExpr:
				return nil, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
			case *sqlparser.AliasedExpr:
				readValue, err := ToGetValue(ctx, subexpr.Expr)
				if err != nil {
					return nil, err
				}
				funcs = append(funcs, readValue)
			case sqlparser.Nextval:
					return nil, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
			default:
					return nil, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
			}
		}
		return func(ctx filter.Context) ([]memsql.Value, error) {
				values := make([]memsql.Value, len(funcs))
				for idx, read := range funcs {
					value, err := read(ctx)
					if err != nil {
						return nil, err
					}
					values[idx] = value
				}
				return values, nil
		}, nil
	default:
		return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
	}
}
