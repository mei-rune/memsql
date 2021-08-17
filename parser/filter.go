package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/runner-mei/memsql/memcore"
  "github.com/runner-mei/memsql/filter"
  "github.com/runner-mei/errors"
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
			return filter.In(leftValue, rightValues), nil
		}
		if v.Operator == sqlparser.NotInStr {
			rightValues, err := ToGetValues(ctx, v.Right)
			if err != nil {
				return nil, err
			}
			return filter.NotIn(leftValue, rightValues), nil
		}

		rightValue, err := ToGetValue(ctx, v.Right)
		if err != nil {
			return nil, err
		}
		switch v.Operator {
		case sqlparser.EqualStr:
			return filter.Equal(leftValue, rightValue), nil
		case sqlparser.LessThanStr:
			return filter.LessThan(leftValue, rightValue), nil
		case sqlparser.GreaterThanStr:
			return filter.GreaterThan(leftValue, rightValue), nil
		case sqlparser.LessEqualStr:
			return filter.LessEqual(leftValue, rightValue), nil
		case sqlparser.GreaterEqualStr:
			return filter.GreaterEqual(leftValue, rightValue), nil
		case sqlparser.NotEqualStr:
			return filter.NotEqual(leftValue, rightValue), nil
		// case sqlparser.InStr:
		// case sqlparser.NotInStr:
		case sqlparser.NullSafeEqualStr:
			return nil, errUnknownOperator(v.Operator)
		case sqlparser.LikeStr:
			return filter.Like(leftValue, rightValue), nil
		case sqlparser.NotLikeStr:
			return filter.NotLike(leftValue, rightValue), nil
		case sqlparser.RegexpStr:
			return filter.Regexp(leftValue, rightValue), nil
		case sqlparser.NotRegexpStr:
			return filter.NotRegexp(leftValue, rightValue), nil
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
			return filter.Between(leftValue, fromValue, toValue), nil
		}
		if v.Operator == sqlparser.NotBetweenStr {
			return filter.NotBetween(leftValue, fromValue, toValue), nil
		}
		return nil, errUnknownOperator(v.Operator)
	case *sqlparser.IsExpr:
		value, err := ToGetValue(ctx, v.Expr)
		if err != nil {
			return nil, err
		}
		switch v.Operator {
		case sqlparser.IsNullStr:
			return filter.IsNull(value), nil
		case sqlparser.IsNotNullStr:
			return filter.IsNotNull(value), nil
		case sqlparser.IsTrueStr:
			return filter.IsTrue(value), nil
		case sqlparser.IsNotTrueStr:
			return filter.IsNotTrue(value), nil
		case sqlparser.IsFalseStr:
			return filter.IsFalse(value), nil
		case sqlparser.IsNotFalseStr:
			return filter.IsNotFalse(value), nil
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

func ToGetValue(ctx filterContext, expr sqlparser.Expr) (func(filter.Context) (memcore.Value, error), error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			s := string(v.Val)
			return func(filter.Context) (memcore.Value, error) {
				return memcore.StringToValue(s), nil
			}, nil
		case sqlparser.IntVal:
			s := string(v.Val)
			i64, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				return func(filter.Context) (memcore.Value, error) {
					return memcore.IntToValue(i64), nil
				}, nil
			}
			u64, err := strconv.ParseUint(s, 10, 64)
			if err == nil {
				return func(filter.Context) (memcore.Value, error) {
					return memcore.UintToValue(u64), nil
				}, nil
			}
			return nil, newTypeError(s, "int")
		case sqlparser.FloatVal:
			s := string(v.Val)
			f64, err := strconv.ParseFloat(s, 64)
			if err == nil {
				return func(filter.Context) (memcore.Value, error) {
					return memcore.FloatToValue(f64), nil
				}, nil
			}
			return nil, newTypeError(s, "float")
		case sqlparser.HexNum:
			s := string(v.Val)
			return nil, newTypeError(s, "HexNum")
		case sqlparser.HexVal:
			s := string(v.Val)
			return nil, newTypeError(s, "HexVal")
		case sqlparser.BitVal:
			s := string(v.Val)
			return nil, newTypeError(s, "BitVal")
		case sqlparser.ValArg:
			s := string(v.Val)
			return nil, newTypeError(s, "ValArg")
		default:
			return nil, fmt.Errorf("invalid expression %+v", expr)
		}
	case *sqlparser.NullVal:
		return func(filter.Context) (memcore.Value, error) {
			return memcore.Null(), nil
		}, nil
	case sqlparser.BoolVal:
		bValue := bool(v)
		return func(filter.Context) (memcore.Value, error) {
			return memcore.BoolToValue(bValue), nil
		}, nil

	case *sqlparser.ColName:
		var name = strings.ToLower(v.Name.String())
		var tableName = strings.ToLower(v.Qualifier.Name.String())
		var tableQualifier = strings.ToLower(v.Qualifier.Qualifier.String())
		if tableName == "" {
			return func(ctx filter.Context) (memcore.Value, error) {
				return ctx.GetValue(tableName, name)
			}, nil
		}
		if tableQualifier == "" {
			return func(ctx filter.Context) (memcore.Value, error) {
				return ctx.GetValue(tableQualifier, name)
			}, nil
		}
		return func(ctx filter.Context) (memcore.Value, error) {
			return ctx.GetValue("", name)
		}, nil
	default:
		return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
	}
}

func ToGetValues(ctx filterContext, expr sqlparser.SQLNode) (func(filter.Context) ([]memcore.Value, error), error) {
	switch v := expr.(type) {
	case sqlparser.SelectExprs:
		var funcs []func(filter.Context) (memcore.Value, error)
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
		return func(ctx filter.Context) ([]memcore.Value, error) {
				values := make([]memcore.Value, len(funcs))
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


func errUnknownOperator(op string) error {
	return errors.New("'"+op+"' is unknown operator")
}

func ErrUnsupportedExpr(op string) error {
	return errors.New("unsupported expression '"+op+"'")
}

func newTypeError(s, typ string) error {
	return errors.New("invalid '"+typ+"': '"+s+"'")
}