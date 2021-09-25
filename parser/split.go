package parser

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func SplitByOr(expr sqlparser.Expr) ([]sqlparser.Expr, error) {
	return splitByOr(expr, nil)
}

func splitByOr(expr sqlparser.Expr, results []sqlparser.Expr) ([]sqlparser.Expr, error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		leftList, err := splitByOr(v.Left, nil)
		if err != nil {
			return nil, err
		}
		rightList, err := splitByOr(v.Right, nil)
		if err != nil {
			return nil, err
		}
		if len(leftList) == 1 && len(rightList) == 1 {
			return append(results, expr), nil
		}

		if len(leftList) > 1 {
			results = append(results, leftList[:len(leftList)-1]...)
		}
		results = append(results, &sqlparser.AndExpr{
			Left:  leftList[len(leftList)-1],
			Right: rightList[0],
		})
		if len(rightList) > 1 {
			results = append(results, rightList[1:]...)
		}
		return nil, nil
	case *sqlparser.OrExpr:
		leftList, err := splitByOr(v.Left, nil)
		if err != nil {
			return nil, err
		}
		rightList, err := splitByOr(v.Right, nil)
		if err != nil {
			return nil, err
		}
		return append(append(results, leftList...), rightList...), nil
	case *sqlparser.NotExpr:
		return append(results, expr), nil
	case *sqlparser.ParenExpr:
		return append(results, expr), nil
	case *sqlparser.ComparisonExpr:
		return append(results, expr), nil
	case *sqlparser.RangeCond:
		return append(results, expr), nil
	case *sqlparser.IsExpr:
		return append(results, expr), nil
	case *sqlparser.ExistsExpr:
		return append(results, expr), nil
	case *sqlparser.SQLVal:
		return append(results, expr), nil
	case *sqlparser.NullVal:
		return append(results, expr), nil
	case sqlparser.BoolVal:
		return append(results, expr), nil
	case *sqlparser.ColName:
		return append(results, expr), nil
	case sqlparser.ValTuple:
		return append(results, expr), nil
	case *sqlparser.Subquery:
		return append(results, expr), nil
	case sqlparser.ListArg:
		return append(results, expr), nil
	case *sqlparser.BinaryExpr:
		return append(results, expr), nil
	case *sqlparser.UnaryExpr:
		return append(results, expr), nil
	case *sqlparser.IntervalExpr:
		return append(results, expr), nil
	case *sqlparser.CollateExpr:
		return append(results, expr), nil
	case *sqlparser.FuncExpr:
		return append(results, expr), nil
	case *sqlparser.CaseExpr:
		return append(results, expr), nil
	case *sqlparser.ValuesFuncExpr:
		return append(results, expr), nil
	case *sqlparser.ConvertExpr:
		return append(results, expr), nil
	case *sqlparser.SubstrExpr:
		return append(results, expr), nil
	case *sqlparser.ConvertUsingExpr:
		return append(results, expr), nil
	case *sqlparser.MatchExpr:
		return append(results, expr), nil
	case *sqlparser.GroupConcatExpr:
		return append(results, expr), nil
	case *sqlparser.Default:
		return append(results, expr), nil
	default:
		return nil, fmt.Errorf("splitByOr: invalid expression %+v", expr)
	}
}
