package parser

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func SplitByTableName(expr sqlparser.Expr, tablename string) (sqlparser.Expr, error) {
	_, expr, err := splitByTableName(expr, tablename)
	return expr, err
}

func splitByTableName(expr sqlparser.Expr, tablename string) (bool, sqlparser.Expr, error) {
	switch v := expr.(type) {
	case *sqlparser.AndExpr          :
		leftChanged, left, err :=  splitByTableName(v.Left, tablename)
		if err != nil {
			return false, nil, err
		}
		rightChanged, right, err :=  splitByTableName(v.Right, tablename)
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
			Left: left,  
			Right: right,
		}, nil
	case *sqlparser.OrExpr           :
		leftChanged, left, err :=  splitByTableName(v.Left, tablename)
		if err != nil {
			return false, nil, err
		}
		rightChanged, right, err :=  splitByTableName(v.Right, tablename)
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
			Left: left,  
			Right: right,
		}, nil
	case *sqlparser.NotExpr          :
		changed, x, err := splitByTableName(v.Expr, tablename)
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
	case *sqlparser.ParenExpr        :
		changed, x, err := splitByTableName(v.Expr, tablename)
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
	case *sqlparser.ComparisonExpr   :
		leftChanged, left, err :=  splitByTableName(v.Left, tablename)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, nil, nil
		}

		escape := v.Escape
		escapeChanged := false
		if escape != nil {
			escapeChanged, escape, err =  splitByTableName(escape, tablename)
			if err != nil {
				return false, nil, err
			}
			if escape == nil {
				return true, nil, nil
			}
		}

		rightChanged, right, err :=  splitByTableName(v.Right, tablename)
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
			Left: left,  
			Right: right,
			Escape: escape,
		}, nil
	case *sqlparser.RangeCond        :
		leftChanged, left, err :=  splitByTableName(v.Left, tablename)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, nil, nil
		}

		fromChanged, from, err :=  splitByTableName(v.From, tablename)
		if err != nil {
			return false, nil, err
		}
		if from == nil {
			return true, nil, nil
		}

		toChanged, to, err :=  splitByTableName(v.To, tablename)
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
			Left: left,  
			From: from,  
			To: to,
		}, nil
	case *sqlparser.IsExpr           :
		changed, x, err :=  splitByTableName(v.Expr, tablename)
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
			Expr: x,
		}, nil
	case *sqlparser.ExistsExpr       :
		return false, expr, nil

		// changed, x, err :=  splitSubqueryByTableName(v.Expr, tablename)
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
	case *sqlparser.SQLVal           :
		return false,  expr, nil
	case *sqlparser.NullVal          :
		return false,  expr, nil
	case sqlparser.BoolVal           :
		return false,  expr, nil
	case *sqlparser.ColName          :
		changed, x, err := splitColNameByTableName(v, tablename)
		if x == nil {
			return changed, nil, err
		}
		return changed, x, err
	case sqlparser.ValTuple          :
		var results = make([]sqlparser.Expr, 0, len(v))
		for idx := range []sqlparser.Expr(sqlparser.Exprs(v)) {
			changed, x, err :=  splitByTableName(v[idx], tablename)
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
	case *sqlparser.Subquery         :
		return false, expr, nil
		// return splitSubqueryByTableName(v, tablename)
	case sqlparser.ListArg           :
		return false, expr, nil
	case *sqlparser.BinaryExpr       :
		leftChanged, left, err :=  splitByTableName(v.Left, tablename)
		if err != nil {
			return false, nil, err
		}
		if left == nil {
			return true, nil, nil
		}

		rightChanged, right, err :=  splitByTableName(v.Right, tablename)
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
			Left: left,  
			Right: right,
		}, nil
	case *sqlparser.UnaryExpr        :
		changed, x, err :=  splitByTableName(v.Expr, tablename)
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
			Expr: x, 
		}, nil
	case *sqlparser.IntervalExpr     :
		changed, x, err :=  splitByTableName(v.Expr, tablename)
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
	case *sqlparser.CollateExpr      :
		changed, x, err :=  splitByTableName(v.Expr, tablename)
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
			Expr: x, 
		}, nil
	case *sqlparser.FuncExpr         :
		changed, x, err :=  splitSelectExprsByTableName(v.Exprs, tablename)
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
			Name: v.Name,
			Distinct: v.Distinct,
			Exprs: x,
		}, nil
	case *sqlparser.CaseExpr         :
			return true, nil, nil
	case *sqlparser.ValuesFuncExpr   :
			return true, nil, nil
	case *sqlparser.ConvertExpr      :
		changed, x, err :=  splitByTableName(v.Expr, tablename)
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
	case *sqlparser.SubstrExpr       :
		nameChanged, name, err :=  splitColNameByTableName(v.Name, tablename)
		if err != nil {
			return false, nil, err
		}
		if name == nil {
			return true, nil, nil
		}

		fromChanged, from, err :=  splitByTableName(v.From, tablename)
		if err != nil {
			return false, nil, err
		}
		if from == nil {
			return true, nil, nil
		}

		toChanged, to, err :=  splitByTableName(v.To, tablename)
		if err != nil {
			return false, nil, err
		}
		if to == nil {
			return true, nil, nil
		}

		if !nameChanged && !fromChanged && !toChanged {
			return false, expr, nil
		}
		return true, &sqlparser.SubstrExpr{
			Name: name,
			From: from,  
			To: to,
		}, nil
	case *sqlparser.ConvertUsingExpr :
		changed, x, err :=  splitByTableName(v.Expr, tablename)
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
	case *sqlparser.MatchExpr        :
		columnsChanged, columns, err :=  splitSelectExprsByTableName(v.Columns, tablename)
		if err != nil {
			return false, nil, err
		}
		if columns == nil {
			return true, nil, nil
		}
		if !columnsChanged {
			return false, expr, nil
		}

		changed, x, err :=  splitByTableName(v.Expr, tablename)
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
			Expr: x, 
			Option: v.Option,
		}, nil
	case *sqlparser.GroupConcatExpr  :
		return true, nil, nil
	case *sqlparser.Default          :
		return true, nil, nil
	default                :
		return false, nil, fmt.Errorf("invalid expression %+v", expr)
	}
}

func splitColNameByTableName(expr *sqlparser.ColName, tablename string) (bool, *sqlparser.ColName, error) {
		if tablename == "" && expr.Qualifier.IsEmpty() {
			return false, expr, nil
		}
		if tablename == strings.ToLower(expr.Qualifier.Name.String()) ||
			tablename == strings.ToLower(expr.Qualifier.Qualifier.String()) {
			return false, expr, nil
		}
		return true, nil, nil
}

func splitSelectExprsByTableName(expr sqlparser.SelectExprs, tablename string) (bool, sqlparser.SelectExprs, error) {
	var selectExprs []sqlparser.SelectExpr
	allchanged := false
	for idx := range expr {
		switch v := expr[idx].(type) {
		case *sqlparser.StarExpr:
		return true, nil, nil
		case *sqlparser.AliasedExpr:
		case sqlparser.Nextval:
			changed, x, err := splitByTableName(v.Expr, tablename)
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
			return false, nil, fmt.Errorf("invalid expression %+v", expr)
		}
	}

	if !allchanged {
		return false, expr, nil
	}

	return true, sqlparser.SelectExprs(selectExprs), nil
}

// func splitSubqueryByTableName(expr *sqlparser.Subquery, tablename string) (bool, sqlparser.Expr, error) {
// 	_, x, err splitSelectStatementByTableName(expr.Select, tablename)

// }

// func splitSelectStatementByTableName(expr sqlparser.SelectStatement, tablename string) (bool, sqlparser.SelectStatement, error) {
// 	switch sel := expr.(type) {
// 	case *sqlparser.Select:
// 		changed, x, err :=  splitByTableName(sel.Where.Expr, tablename)
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