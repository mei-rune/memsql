package parser

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

func parse(sqlstr string) (*sqlparser.Select, error) {
	stmt, err := sqlparser.Parse(sqlstr)
	if err != nil {
		return nil, err
	}
	// Otherwise do something with stmt
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("only support select statement")
	}
	return selectStmt, nil
}

type Datasource struct {
	Table string
	As    string
}

type simpleExecuteContext struct {
	s    *storage
	stmt *sqlparser.Select
	ds   Datasource
}

func ExecuteSelect(ec *simpleExecuteContext, stmt *sqlparser.Select) (Query, error) {
	if len(stmt.From) != 1 {
		return nil, fmt.Errorf("currently only one expression in from supported, got %v", len(stmt.From))
	}

	if stmt.Hints != "" {
		return nil, errors.Errorf("currently unsupport hints")
	}
	if stmt.Having != nil {
		return nil, errors.Errorf("currently unsupport having")
	}
	if stmt.GroupBy != nil {
		return nil, errors.Errorf("currently unsupport groupBy")
	}
	if stmt.OrderBy != nil {
		return nil, errors.Errorf("currently unsupport orderBy")
	}
	if stmt.Limit != nil {
		return nil, errors.Errorf("currently unsupport limit")
	}
	if stmt.Lock != "" {
		return nil, errors.Errorf("currently unsupport lock")
	}
	if stmt.Distinct != "" {
		return nil, errors.Errorf("currently unsupport distinct")
	}

	// Where       *Where

	query, err = ExecuteTableExpression(ec, stmt.From[0], stmt.Where)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse from expression")
	}
	return query, nil
}

func ExecuteTableExpression(ec *simpleExecuteContext, expr sqlparser.TableExpr, where *sqlparser.Where) error {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ExecuteAliasedTableExpression(ec, expr, where)
	case *sqlparser.JoinTableExpr:
		return ParseJoinTableExpression(ec, expr, where)
	// case *sqlparser.ParenTableExpr:
	// 	return ParseTableExpression(expr.Exprs[0])
	default:
		return errors.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

/*
func ExecuteJoinTableExpression(ec *simpleExecuteContext, expr *sqlparser.JoinTableExpr, where *sqlparser.Where) error {
// 	type JoinTableExpr struct {
// 	LeftExpr  TableExpr
// 	Join      string
// 	RightExpr TableExpr
// 	Condition JoinCondition
// }

  err := ExecuteAliasedTableExpression(ec, expr.LeftExpr, where)
  if err != nil {
  	return err
  }

  err = ExecuteAliasedTableExpression(ec, expr.RightExpr, where)
  if err != nil {
  	return err
  }

  return nil
}
*/

func ExecuteAliasedTableExpression(ec *simpleExecuteContext, expr *sqlparser.AliasedTableExpr, where *sqlparser.Where) (Query, error) {
	if len(expr.Partitions) > 0 {
		return nil, fmt.Errorf("invalid partitions in the table expression %+v", expr.Expr)
	}
	if expr.Hints != nil {
		return nil, fmt.Errorf("invalid index hits in the table expression %+v", expr.Expr)
	}
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		ec.ds.Table = subExpr.Name.String()
		if expr.As == nil {
			ec.ds.As = ec.ds.Table
		} else {
			ec.ds.As = expr.As.String()
		}

		return ExecuteTable(ec, ec.ds, ec.stmt.Where)
		// if expr.As.IsEmpty() {
		// 	return nil, fmt.Errorf("table \"%v\" must have unique alias", subExpr.Name)
		// }
		// return logical.NewDataSource(subExpr.Name.String(), expr.As.String()), nil
	case *sqlparser.Subquery:
		return nil, fmt.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	default:
		return nil, fmt.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ExecuteTable(ec *simpleExecuteContext, ds Datasource, expr sqlparser.Expr) (Query, error) {
	tableExpr, err := SplitByColumnName(expr, ByTag())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't resolve where '"+sqlparser.Source(expr)+"'")
	}

	filter, err := toFilter(tableExpr)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't convert where '"+sqlparser.Source(expr)+"'")
	}

	ec.From(ds.Table)
}
