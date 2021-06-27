package memsql

import (
	"errors"

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
