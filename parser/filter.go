package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

  "github.com/runner-mei/memsql/vm"
  "github.com/runner-mei/errors"
	"github.com/xwb1989/sqlparser"
)

type filterContext interface {}

func ToFilter(ctx filterContext, expr sqlparser.Expr) (func(vm.Context) (bool, error), error) {
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
		return vm.And(leftFilter, rightFilter), nil
	case *sqlparser.OrExpr:
		leftFilter, err := ToFilter(ctx, v.Left)
		if err != nil {
			return nil, err
		}
		rightFilter, err := ToFilter(ctx, v.Right)
		if err != nil {
			return nil, err
		}
		return vm.Or(leftFilter, rightFilter), nil
	case *sqlparser.NotExpr:
		f, err := ToFilter(ctx, v.Expr)
		if err != nil {
			return nil, err
		}
		return vm.Not(f), nil
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
			return vm.In(leftValue, rightValues), nil
		}
		if v.Operator == sqlparser.NotInStr {
			rightValues, err := ToGetValues(ctx, v.Right)
			if err != nil {
				return nil, err
			}
			return vm.NotIn(leftValue, rightValues), nil
		}

		rightValue, err := ToGetValue(ctx, v.Right)
		if err != nil {
			return nil, err
		}
		switch v.Operator {
		case sqlparser.EqualStr:
			return vm.Equal(leftValue, rightValue), nil
		case sqlparser.LessThanStr:
			return vm.LessThan(leftValue, rightValue), nil
		case sqlparser.GreaterThanStr:
			return vm.GreaterThan(leftValue, rightValue), nil
		case sqlparser.LessEqualStr:
			return vm.LessEqual(leftValue, rightValue), nil
		case sqlparser.GreaterEqualStr:
			return vm.GreaterEqual(leftValue, rightValue), nil
		case sqlparser.NotEqualStr:
			return vm.NotEqual(leftValue, rightValue), nil
		// case sqlparser.InStr:
		// case sqlparser.NotInStr:
		case sqlparser.NullSafeEqualStr:
			return nil, errUnknownOperator(v.Operator)
		case sqlparser.LikeStr:
			return vm.Like(leftValue, rightValue), nil
		case sqlparser.NotLikeStr:
			return vm.NotLike(leftValue, rightValue), nil
		case sqlparser.RegexpStr:
			return vm.Regexp(leftValue, rightValue), nil
		case sqlparser.NotRegexpStr:
			return vm.NotRegexp(leftValue, rightValue), nil
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
			return vm.Between(leftValue, fromValue, toValue), nil
		}
		if v.Operator == sqlparser.NotBetweenStr {
			return vm.NotBetween(leftValue, fromValue, toValue), nil
		}
		return nil, errUnknownOperator(v.Operator)
	case *sqlparser.IsExpr:
		value, err := ToGetValue(ctx, v.Expr)
		if err != nil {
			return nil, err
		}
		switch v.Operator {
		case sqlparser.IsNullStr:
			return vm.IsNull(value), nil
		case sqlparser.IsNotNullStr:
			return vm.IsNotNull(value), nil
		case sqlparser.IsTrueStr:
			return vm.IsTrue(value), nil
		case sqlparser.IsNotTrueStr:
			return vm.IsNotTrue(value), nil
		case sqlparser.IsFalseStr:
			return vm.IsFalse(value), nil
		case sqlparser.IsNotFalseStr:
			return vm.IsNotFalse(value), nil
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

func ToGetValue(ctx filterContext, expr sqlparser.Expr) (func(vm.Context) (vm.Value, error), error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.StrVal:
			s := string(v.Val)
			return func(vm.Context) (vm.Value, error) {
				return vm.StringToValue(s), nil
			}, nil
		case sqlparser.IntVal:
			s := string(v.Val)
			i64, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				return func(vm.Context) (vm.Value, error) {
					return vm.IntToValue(i64), nil
				}, nil
			}
			u64, err := strconv.ParseUint(s, 10, 64)
			if err == nil {
				return func(vm.Context) (vm.Value, error) {
					return vm.UintToValue(u64), nil
				}, nil
			}
			return nil, newTypeError(s, "int")
		case sqlparser.FloatVal:
			s := string(v.Val)
			f64, err := strconv.ParseFloat(s, 64)
			if err == nil {
				return func(vm.Context) (vm.Value, error) {
					return vm.FloatToValue(f64), nil
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
		return func(vm.Context) (vm.Value, error) {
			return vm.Null(), nil
		}, nil
	case sqlparser.BoolVal:
		bValue := bool(v)
		return func(vm.Context) (vm.Value, error) {
			return vm.BoolToValue(bValue), nil
		}, nil
	case *sqlparser.ColName:
		var name = strings.ToLower(v.Name.String())
		var tableName = strings.ToLower(v.Qualifier.Name.String())
		var tableQualifier = strings.ToLower(v.Qualifier.Qualifier.String())
		if tableName != "" {
			return func(ctx vm.Context) (vm.Value, error) {
				return ctx.GetValue(tableName, name)
			}, nil
		}
		if tableQualifier != "" {
			return func(ctx vm.Context) (vm.Value, error) {
				return ctx.GetValue(tableQualifier, name)
			}, nil
		}
		return func(ctx vm.Context) (vm.Value, error) {
			return ctx.GetValue("", name)
		}, nil

	case sqlparser.ValTuple:
		return nil, ErrUnsupportedExpr("ValTuple")
	case *sqlparser.Subquery:
		return nil, ErrUnsupportedExpr("Subquery")
	case sqlparser.ListArg:
		return nil, ErrUnsupportedExpr("ListArg")
	case *sqlparser.BinaryExpr:
		leftValue, err := ToGetValue(ctx, v.Left)
		if err != nil {
			return nil, err
		}
		rightValue, err := ToGetValue(ctx, v.Right)
		if err != nil {
			return nil, err
		}

		switch v.Operator{
		// case sqlparser.BitAndStr:
		// case sqlparser.BitOrStr:
		// case sqlparser.BitXorStr:
		case sqlparser.PlusStr:
			return vm.PlusFunc(leftValue, rightValue), nil
		case sqlparser.MinusStr:
			return vm.MinusFunc(leftValue, rightValue), nil
		case sqlparser.MultStr:
		 	return vm.MultFunc(leftValue, rightValue), nil
		case sqlparser.DivStr:
		 	return vm.DivFunc(leftValue, rightValue), nil
		// case sqlparser.IntDivStr:
		// 	return vm.IntDiv(leftValue, rightValue), nil
		case sqlparser.ModStr:
		 	return vm.ModFunc(leftValue, rightValue), nil
		// case sqlparser.ShiftLeftStr:
		// case sqlparser.ShiftRightStr:
		default:
			return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
		}
	case *sqlparser.UnaryExpr:
		readValue, err := ToGetValue(ctx, v.Expr)
		if err != nil {
			return nil, err
		}

		switch v.Operator{
		// case sqlparser.UPlusStr:
		case sqlparser.UMinusStr:
			return vm.UminusFunc(readValue), nil
		// case sqlparser.TildaStr:
		// case sqlparser.BangStr:
		// case sqlparser.IntDivStr:
		// case sqlparser.BinaryStr:
		// case sqlparser.UBinaryStr:
		default:
			return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
		}

	case *sqlparser.IntervalExpr:
		readValue, err := ToGetValue(ctx, v.Expr)
		if err != nil {
			return nil, err
		}
		unit := strings.ToLower(v.Unit)
		switch unit {
		case "years", "year", "months", "month", "weeks", "week", "days", "day", "hours", "hour", "minutes", "minute", "seconds", "second":
		default:
			return nil, fmt.Errorf("invalid interval expression %+v", expr)
		}

		return func(ctx vm.Context) (vm.Value, error) {
		    value, err := readValue(ctx)
		    if err != nil {
		      return vm.Null(), err
		    }
		    i64, err := value.AsInt(false)
		    if err != nil {
		      return vm.Null(), err
		    }

			switch unit {
			case "years", "year":
				return vm.IntervalToValue(time.Duration(i64) * 365 * 24 * 60 * 60 * time.Second), nil
			case "months", "month": 
				return vm.IntervalToValue(time.Duration(i64) * 30 * 24 * 60 * 60 * time.Second), nil
			case "weeks", "week": 
				return vm.IntervalToValue(time.Duration(i64) * 7 * 24 * 60 * 60 * time.Second), nil
			case "days", "day": 
				return vm.IntervalToValue(time.Duration(i64) * 24 * 60 * 60 * time.Second), nil
			case "hours", "hour": 
				return vm.IntervalToValue(time.Duration(i64) * 60 * 60 * time.Second), nil
			case "minutes", "minute": 
				return vm.IntervalToValue(time.Duration(i64) * 60 * time.Second), nil
			case "seconds", "second":
				return vm.IntervalToValue(time.Duration(i64) * time.Second), nil
			default:
				return vm.Null(), fmt.Errorf("invalid interval expression %+v", expr)
			}
		}, nil
	case *sqlparser.CollateExpr:
		return nil, ErrUnsupportedExpr("CollateExpr")
	case *sqlparser.FuncExpr:
		return ToFuncGetValue(ctx, v)
	case *sqlparser.CaseExpr:
		return nil, ErrUnsupportedExpr("CaseExpr")
	case *sqlparser.ValuesFuncExpr:
		return nil, ErrUnsupportedExpr("ValuesFuncExpr")
	case *sqlparser.ConvertExpr:

		readValue, err := ToGetValue(ctx, v.Expr)
		if err != nil {
			return nil, err
		}

		switch strings.ToLower(v.Type.Type) {
		case "int", "integer", "signed", "signed integer":
			return vm.ConvertToInt(readValue), nil
		case "unsigned", "unsigned integer":
			return vm.ConvertToUint(readValue), nil
		case "bool", "boolean":
			return vm.ConvertToBool(readValue), nil
		// case "binary":
		// 	return vm.ConvertToBinary(readValue, v.Type.Length), nil
		// case "char":
		// 	return vm.ConvertToChar(readValue, v.Type.Length, v.Type.Charset), nil
		// case "date":
		// 	return vm.ConvertToDate(readValue), nil
		case "datetime":
		 	return vm.ConvertToDatetime(readValue), nil
		// case "time":
		// 	return vm.ConvertToTime(readValue), nil
		// case "decimal":
		// 	return vm.ConvertToDecimal(readValue, v.Type.Length), nil
		// case "json":
		// 	return vm.ConvertToJSON(readValue), nil
		// case "nchar":
		// 	return vm.ConvertToNChar(readValue, v.Type.Length, v.Type.Charset), nil
		}
		return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
	case *sqlparser.SubstrExpr:
		return nil, ErrUnsupportedExpr("SubstrExpr")
	case *sqlparser.ConvertUsingExpr:
		return nil, ErrUnsupportedExpr("ConvertUsingExpr")
	case *sqlparser.MatchExpr:
		return nil, ErrUnsupportedExpr("MatchExpr")
	case *sqlparser.GroupConcatExpr:
		return nil, ErrUnsupportedExpr("GroupConcatExpr")

	default:
		return nil, fmt.Errorf("invalid expression %T %+v", expr, expr)
	}
}

func ToFuncGetValue(ctx filterContext, expr *sqlparser.FuncExpr) (func(vm.Context) (vm.Value, error), error) {		
	// // FuncExpr represents a function call.
	// type FuncExpr struct {
	// 	Qualifier TableIdent
	// 	Name      ColIdent
	// 	Distinct  bool
	// 	Exprs     SelectExprs
	// }

	f, ok := vm.Funcs[expr.Name.String()]
	if !ok {
		return nil, errors.New("func '"+expr.Name.String()+"' isnot exists")
	}

	values, err := ToGetValues(ctx, expr.Exprs)
	if err != nil {
		return nil, err
	}
	return vm.CallFunc(f, values), nil
}

func ToGetValues(ctx filterContext, expr sqlparser.SQLNode) (func(vm.Context) ([]vm.Value, error), error) {
	switch v := expr.(type) {
	case sqlparser.SelectExprs:
		var funcs []func(vm.Context) (vm.Value, error)
		for idx := range v {
			f, err := ToGetSelectValue(ctx, v[idx])
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, f)
		}
		return func(ctx vm.Context) ([]vm.Value, error) {
				values := make([]vm.Value, len(funcs))
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


func ToGetSelectValue(ctx filterContext, expr sqlparser.SelectExpr) (func(vm.Context) (vm.Value, error), error) {
	switch subexpr := expr.(type) {
	case *sqlparser.StarExpr:
		return nil, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
	case *sqlparser.AliasedExpr:
		return ToGetValue(ctx, subexpr.Expr)
	case sqlparser.Nextval:
		return nil, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
	default:
		return nil, fmt.Errorf("invalid expression %T %+v", subexpr, subexpr)
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