package parser

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func ByTag() ExprFilter {
	return ExprFilter{
			filter: func(expr *sqlparser.ColName) bool {
		return strings.HasPrefix(expr.Name.String(), "@")
	},
}
}

func ByTableTag(tableAs TableAlias) ExprFilter {
	return ExprFilter{
			filter:  func(expr *sqlparser.ColName) bool {
		if expr.Qualifier.IsEmpty() {
			return strings.HasPrefix(expr.Name.String(), "@")
		}

		qualifier := strings.ToLower(sqlparser.String(expr.Qualifier));
		if tableAs.Equal(qualifier){
			return strings.HasPrefix(expr.Name.String(), "@")
		}
		return false
	},
}
}

func SplitByTableName(expr sqlparser.Expr, tableName, tableAs string) (sqlparser.Expr, error) {
	_, expr, err := SplitBy(expr, ByTable(tableName, tableAs))
	return expr, err
}

func ByTable(tableName, tableAs string) ExprFilter {
	tableName = strings.ToLower(tableName)
	tableAs = strings.ToLower(tableAs)

	return ExprFilter{
			filter: func(expr *sqlparser.ColName) bool {
			if expr.Qualifier.IsEmpty() {
				return true
			}
			if qualifier := strings.ToLower(sqlparser.String(expr.Qualifier)); tableName == qualifier ||
				tableAs == qualifier {
				return true
			}
			return false
		},
	}
}

type ExprFilter struct {
	isTableFilter bool
	filter func(*sqlparser.ColName) bool
}

func (ef *ExprFilter) TableFilter() {
	ef.isTableFilter  = true
}

func SplitBy(expr sqlparser.Expr, filter ExprFilter) (bool, sqlparser.Expr, error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr:
		leftChanged, left, err := SplitBy(v.Left, filter)
		if err != nil {
			return false, nil, err
		}
		rightChanged, right, err := SplitBy(v.Right, filter)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, right, nil
		}
		if right == nil {
			return true, left, nil
		}
		if !leftChanged && !rightChanged {
			return false, expr, nil
		}
		return true, &sqlparser.AndExpr{
			Left:  left,
			Right: right,
		}, nil
	case *sqlparser.OrExpr:
		leftChanged, left, err := SplitBy(v.Left, filter)
		if err != nil {
			return false, nil, err
		}
		rightChanged, right, err := SplitBy(v.Right, filter)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, right, nil
		}
		if right == nil {
			return true, left, nil
		}
		if !leftChanged && !rightChanged {
			return false, expr, nil
		}
		return true, &sqlparser.OrExpr{
			Left:  left,
			Right: right,
		}, nil
	case *sqlparser.NotExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, err
		}
		return true, &sqlparser.NotExpr{Expr: x}, nil
	case *sqlparser.ParenExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, err
		}
		return true, &sqlparser.ParenExpr{Expr: x}, nil
	case *sqlparser.ComparisonExpr:
		if filter.isTableFilter && v.Operator == sqlparser.EqualStr {
			left, leftok := v.Left.(*sqlparser.ColName)
			right, rightok := v.Right.(*sqlparser.ColName)
			if leftok && rightok {
				if filter.filter(left) {
					return false, nil, nil
				}
				if filter.filter(right) {
					return false, nil, nil
				}
			}
		}
		leftChanged, left, err := SplitBy(v.Left, filter)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, nil, nil
		}

		escape := v.Escape
		escapeChanged := false
		if escape != nil {
			escapeChanged, escape, err = SplitBy(escape, filter)
			if err != nil {
				return false, nil, err
			}
			if escape == nil {
				return true, nil, nil
			}
		}

		rightChanged, right, err := SplitBy(v.Right, filter)
		if err != nil {
			return false, nil, err
		}
		if right == nil {
			return true, nil, nil
		}
		if !leftChanged && !rightChanged && !escapeChanged {
			return false, expr, nil
		}
		return true, &sqlparser.ComparisonExpr{
			Operator: v.Operator,
			Left:     left,
			Right:    right,
			Escape:   escape,
		}, nil
	case *sqlparser.RangeCond:
		leftChanged, left, err := SplitBy(v.Left, filter)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, nil, nil
		}

		fromChanged, from, err := SplitBy(v.From, filter)
		if err != nil {
			return false, nil, err
		}
		if from == nil {
			return true, nil, nil
		}

		toChanged, to, err := SplitBy(v.To, filter)
		if err != nil {
			return false, nil, err
		}
		if to == nil {
			return true, nil, nil
		}

		if !leftChanged && !fromChanged && !toChanged {
			return false, expr, nil
		}
		return true, &sqlparser.RangeCond{
			Operator: v.Operator,
			Left:     left,
			From:     from,
			To:       to,
		}, nil
	case *sqlparser.IsExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.IsExpr{
			Operator: v.Operator,
			Expr:     x,
		}, nil
	case *sqlparser.ExistsExpr:
		return false, expr, nil

		// changed, x, err :=  splitSubqueryByTableName(v.Expr, filter)
		// if err != nil {
		// 	return false, nil, err
		// }
		// if x == nil {
		// 	return true, nil, nil
		// }
		// if !changed {
		// 	return false, expr, nil
		// }
		// return true, &sqlparser.ExistsExpr{
		// 	Subquery: x,
		// }, nil
	case *sqlparser.SQLVal:
		return false, expr, nil
	case *sqlparser.NullVal:
		return false, expr, nil
	case sqlparser.BoolVal:
		return false, expr, nil
	case *sqlparser.ColName:
		ok := filter.filter(v)
		if !ok {
			return true, nil, nil
		}
		return false, v, nil
	case sqlparser.ValTuple:
		var results = make([]sqlparser.Expr, 0, len(v))
		for idx := range []sqlparser.Expr(sqlparser.Exprs(v)) {
			changed, x, err := SplitBy(v[idx], filter)
			if err != nil {
				return false, nil, err
			}
			if !changed {
				results = append(results, x)
				continue
			}
			results = append(results, v[idx])
		}
		return true, sqlparser.ValTuple(results), nil
	case *sqlparser.Subquery:
		return false, expr, nil
		// return splitSubqueryByTableName(v, filter)
	case sqlparser.ListArg:
		return false, expr, nil
	case *sqlparser.BinaryExpr:
		leftChanged, left, err := SplitBy(v.Left, filter)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, nil, nil
		}

		rightChanged, right, err := SplitBy(v.Right, filter)
		if err != nil {
			return false, nil, err
		}
		if right == nil {
			return true, nil, nil
		}
		if !leftChanged && !rightChanged {
			return false, expr, nil
		}
		return true, &sqlparser.BinaryExpr{
			Operator: v.Operator,
			Left:     left,
			Right:    right,
		}, nil
	case *sqlparser.UnaryExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.UnaryExpr{
			Operator: v.Operator,
			Expr:     x,
		}, nil
	case *sqlparser.IntervalExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.IntervalExpr{
			Expr: x,
			Unit: v.Unit,
		}, nil
	case *sqlparser.CollateExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.CollateExpr{
			Charset: v.Charset,
			Expr:    x,
		}, nil
	case *sqlparser.FuncExpr:
		changed, x, err := splitSelectExprsByTableName(v.Exprs, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.FuncExpr{
			Qualifier: v.Qualifier,
			Name:      v.Name,
			Distinct:  v.Distinct,
			Exprs:     x,
		}, nil
	case *sqlparser.CaseExpr:
		return true, nil, nil
	case *sqlparser.ValuesFuncExpr:
		return true, nil, nil
	case *sqlparser.ConvertExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.ConvertExpr{
			Type: v.Type,
			Expr: x,
		}, nil
	case *sqlparser.SubstrExpr:
		nameok := filter.filter(v.Name)
		if !nameok {
			return true, nil, nil
		}

		fromChanged, from, err := SplitBy(v.From, filter)
		if err != nil {
			return false, nil, err
		}
		if from == nil {
			return true, nil, nil
		}

		toChanged, to, err := SplitBy(v.To, filter)
		if err != nil {
			return false, nil, err
		}
		if to == nil {
			return true, nil, nil
		}

		if !fromChanged && !toChanged {
			return false, expr, nil
		}
		return true, &sqlparser.SubstrExpr{
			Name: v.Name,
			From: from,
			To:   to,
		}, nil
	case *sqlparser.ConvertUsingExpr:
		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.ConvertUsingExpr{
			Type: v.Type,
			Expr: x,
		}, nil
	case *sqlparser.MatchExpr:
		columnsChanged, columns, err := splitSelectExprsByTableName(v.Columns, filter)
		if err != nil {
			return false, nil, err
		}
		if columns == nil {
			return true, nil, nil
		}
		if !columnsChanged {
			return false, expr, nil
		}

		changed, x, err := SplitBy(v.Expr, filter)
		if err != nil {
			return false, nil, err
		}
		if x == nil {
			return true, nil, nil
		}
		if !changed {
			return false, expr, nil
		}
		return true, &sqlparser.MatchExpr{
			Columns: columns,
			Expr:    x,
			Option:  v.Option,
		}, nil
	case *sqlparser.GroupConcatExpr:
		return true, nil, nil
	case *sqlparser.Default:
		return true, nil, nil
	default:
		panic("")
		return false, nil, fmt.Errorf("SplitBy: invalid expression %+v", expr)
	}
}

func splitSelectExprsByTableName(expr sqlparser.SelectExprs, filter ExprFilter) (bool, sqlparser.SelectExprs, error) {
	var selectExprs []sqlparser.SelectExpr
	allchanged := false
	for idx := range expr {
		switch v := expr[idx].(type) {
		case *sqlparser.StarExpr:
			return true, nil, nil
		case *sqlparser.AliasedExpr:
			changed, x, err := SplitBy(v.Expr, filter)
			if err != nil {
				return true, nil, err
			}
			if changed {
				allchanged = true
			}
			if x == nil {
				return true, nil, nil
			}
			selectExprs = append(selectExprs, &sqlparser.AliasedExpr{Expr: x, As: v.As})
		case sqlparser.Nextval:
			changed, x, err := SplitBy(v.Expr, filter)
			if err != nil {
				return true, nil, err
			}
			if changed {
				allchanged = true
			}
			if x == nil {
				return true, nil, nil
			}
			selectExprs = append(selectExprs, sqlparser.Nextval{Expr: x})
		default:
			return false, nil, fmt.Errorf("splitSelectExprsByTableName: invalid expression %+v", expr)
		}
	}

	if !allchanged {
		return false, expr, nil
	}

	return true, sqlparser.SelectExprs(selectExprs), nil
}

// func splitSubqueryByTableName(expr *sqlparser.Subquery, filter func(tablename string) bool) (bool, sqlparser.Expr, error) {
// 	_, x, err splitSelectStatementByTableName(expr.Select, filter)

// }

// func splitSelectStatementByTableName(expr sqlparser.SelectStatement, filter func(tablename string) bool) (bool, sqlparser.SelectStatement, error) {
// 	switch sel := expr.(type) {
// 	case *sqlparser.Select:
// 		changed, x, err :=  SplitBy(sel.Where.Expr, filter)
// 		if err != nil {
// 			return changed, x, err
// 		}
// 		if !changed {
// 			return change, x, err
// 		}
// 		var where *sqlparser.Where
// 		if x != nil {
// 			where = &sqlparser.Where{
// 				Type: sel.Type,
// 				Expr: x,
// 			}
// 		}
// 		return true, &sqlparser.Select{
// 				Cache: sel.Cache,
// 				Comments: sel.Comments,
// 				Distinct: sel.Distinct,
// 				Hints: sel.Hints,
// 				SelectExprs: sel.SelectExprs,
// 				From: sel.From,
// 				Where:  where,
// 				GroupBy: sel.GroupBy,
// 				Having: sel.Having,
// 				OrderBy: sel.OrderBy,
// 				Limit: sel.Limit,
// 				Lock: sel.Lock,
// 			}, nil
// 	case *sqlparser.Union:

// 		type Union struct {
// 			Type        string
// 			Left, Right SelectStatement
// 			OrderBy     OrderBy
// 			Limit       *Limit
// 			Lock        string
// 		}

// 	case *sqlparser.ParenSelect:
// 	default:
// 		return false, nil, fmt.Errorf("invalid expression %+v", expr)
// 	}
// }
