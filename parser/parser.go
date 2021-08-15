package parser

/*
import (
	"errors"
	"fmt"

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
	 As string
}

type simpleExecuteContext struct {
  s *storage
  stmt *sqlparser.Select
  ds Datasource
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

	err = ExecuteTableExpression(ec, stmt.From[0])
	if err != nil {
		return nil, errors.Wrap(err, "couldn't parse from expression")
	}
}

func ExecuteTableExpression(ec *simpleExecuteContext, expr sqlparser.TableExpr) (error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ExecuteAliasedTableExpression(ec, expr)
	case *sqlparser.JoinTableExpr:
		return ParseJoinTableExpression(expr)
	// case *sqlparser.ParenTableExpr:
	// 	return ParseTableExpression(expr.Exprs[0])
	default:
		return errors.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
	}
}

func ExecuteJoinTableExpression(ec *simpleExecuteContext, expr *sqlparser.JoinTableExpr) error {
// 	type JoinTableExpr struct {
// 	LeftExpr  TableExpr
// 	Join      string
// 	RightExpr TableExpr
// 	Condition JoinCondition
// }

  err := ExecuteAliasedTableExpression(ec, expr.LeftExpr)
  if err != nil {
  	return err
  }


  err = ExecuteAliasedTableExpression(ec, expr.RightExpr)
  if err != nil {
  	return err
  }

  return nil
}

func ExecuteAliasedTableExpression(ec *simpleExecuteContext, expr *sqlparser.AliasedTableExpr) (Query, error) {
	if len(expr.Partitions) >0 {
		return nil, errors.Errorf("invalid partitions in the table expression %+v", expr.Expr)
	}
	if expr.Hints != nil {
		return nil, errors.Errorf("invalid index hits in the table expression %+v", expr.Expr)
	}
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		ec.ds.Table = subExpr.Name.String()
		if expr.As == nil {
			ec.ds.As = ec.ds.Table
		} else {
			ec.ds.As = expr.As.String()
		}

		return ExecuteWhere(ec, ec.ds, ec.stmt.)
		// if expr.As.IsEmpty() {
		// 	return nil, errors.Errorf("table \"%v\" must have unique alias", subExpr.Name)
		// }
		// return logical.NewDataSource(subExpr.Name.String(), expr.As.String()), nil
	case *sqlparser.Subquery:
		return nil, errors.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	default:
		return nil, errors.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}


func ExecuteWhere(ec *simpleExecuteContext, es Datasource, expr *sqlparser.Expr) error {
}

*/