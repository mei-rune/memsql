package memsql


// type Iterator interface {
// 	Next() ([]memcore.KeyValue, error)
// }


// func From(ctx *SessionContext, tableName, tableAs string, tableExpr sqlparser.Expr) (memcore.Query, []memcore.TableName, error)
// 	kvs, err := ToKeyValues(tableExpr, nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = EnsureTables(ctx, tableName, tableAs, kvs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return ctx.Storage.From(ctx, tableName, tableAs, tableExpr)
// }



// func ReadTable(ctx *SessionContext, tableName string, kvs []memcore.KeyValue) (interface{}, error) {

// }


// func EnsureTables(ctx *SessionContext, tableName, tableAs string, kvs []memcore.KeyValue) (error) {
// 	value, err := ctx.Read(tableName, kvs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	switch v := value.(type) {
// 	case map[string]interface{}:
// 		return saveToTable(ctx, table, kvs, v)
// 	case []map[string]interface{}:
// 		for 
// 		return saveToTable(ctx, table, kvs, v)
// 	}
// }
