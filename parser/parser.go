package parser

import (
	"fmt"
	"reflect"

	"github.com/xwb1989/sqlparser"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/filter"
	"github.com/runner-mei/errors"
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
	s    memcore.Storage
	stmt *sqlparser.Select
	ds   Datasource
}

func ExecuteSelect(ec *simpleExecuteContext, stmt *sqlparser.Select) (memcore.Query, error) {
	if len(stmt.From) != 1 {
		return memcore.Query{}, fmt.Errorf("currently only one expression in from supported, got %v", len(stmt.From))
	}

	if stmt.Hints != "" {
		return memcore.Query{}, errors.New("currently unsupport hints")
	}
	if stmt.Having != nil {
		return memcore.Query{}, errors.New("currently unsupport having")
	}
	if stmt.GroupBy != nil {
		return memcore.Query{}, errors.New("currently unsupport groupBy")
	}
	if stmt.OrderBy != nil {
		return memcore.Query{}, errors.New("currently unsupport orderBy")
	}
	if stmt.Limit != nil {
		return memcore.Query{}, errors.New("currently unsupport limit")
	}
	if stmt.Lock != "" {
		return memcore.Query{}, errors.New("currently unsupport lock")
	}
	if stmt.Distinct != "" {
		return memcore.Query{}, errors.New("currently unsupport distinct")
	}

	// Where       *Where

	query, err := ExecuteTableExpression(ec, stmt.From[0], stmt.Where)
	if err != nil {
		return memcore.Query{}, errors.Wrap(err, "couldn't parse from expression")
	}
	return query, nil
}

func ExecuteTableExpression(ec *simpleExecuteContext, expr sqlparser.TableExpr, where *sqlparser.Where) (memcore.Query, error) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return ExecuteAliasedTableExpression(ec, expr, where)
	//case *sqlparser.JoinTableExpr:
	//	return ParseJoinTableExpression(ec, expr, where)
	// case *sqlparser.ParenTableExpr:
	// 	return ParseTableExpression(expr.Exprs[0])
	default:
		return memcore.Query{}, fmt.Errorf("invalid table expression %+v of type %v", expr, reflect.TypeOf(expr))
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

func ExecuteAliasedTableExpression(ec *simpleExecuteContext, expr *sqlparser.AliasedTableExpr, where *sqlparser.Where) (memcore.Query, error) {
	if len(expr.Partitions) > 0 {
		return memcore.Query{}, fmt.Errorf("invalid partitions in the table expression %+v", expr.Expr)
	}
	if expr.Hints != nil {
		return memcore.Query{}, fmt.Errorf("invalid index hits in the table expression %+v", expr.Expr)
	}
	switch subExpr := expr.Expr.(type) {
	case sqlparser.TableName:
		ec.ds.Table = subExpr.Name.String()
		if expr.As == nil {
			ec.ds.As = ec.ds.Table
		} else {
			ec.ds.As = expr.As.String()
		}

		if ec.stmt.Where.Expr == nil {
		return ExecuteTable(ec, ec.ds, nil)
		}
		return ExecuteTable(ec, ec.ds, ec.stmt.Where.Expr)
		// if expr.As.IsEmpty() {
		// 	return nil, fmt.Errorf("table \"%v\" must have unique alias", subExpr.Name)
		// }
		// return logical.NewDataSource(subExpr.Name.String(), expr.As.String()), nil
	case *sqlparser.Subquery:
		return memcore.Query{}, fmt.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	default:
		return memcore.Query{}, fmt.Errorf("invalid aliased table expression %+v of type %v", expr.Expr, reflect.TypeOf(expr.Expr))
	}
}

func ExecuteTable(ec *simpleExecuteContext, ds Datasource, expr sqlparser.Expr) (memcore.Query, error) {
	_, tableExpr, err := SplitByColumnName(expr, ByTag())
	if err != nil {
		return memcore.Query{}, errors.Wrap(err, "couldn't resolve where '"+sqlparser.String(expr)+"'")
	}

	var f = func(filter.Context) (bool, error) {
		return true, nil
	}
	if tableExpr != nil {
		f, err = ToFilter(nil, tableExpr)
		if err != nil {
			return memcore.Query{}, errors.Wrap(err, "couldn't convert where '"+sqlparser.String(expr)+"'")
		}
	}

	return ec.s.From(ds.Table, f)
}