package memsql

import (
	"reflect"
	"time"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/parser"
	"github.com/xwb1989/sqlparser"
)

type HookStorage struct {
	storage memcore.Storage
}

func (hs *HookStorage) From(ctx *SessionContext, tableName, tableAs string, tableExpr sqlparser.Expr) (memcore.Query, []memcore.TableName, error) {
	kvs, err := parser.ToKeyValues(tableExpr, nil)
	if err != nil {
		return memcore.Query{}, nil, err
	}

	err = hs.EnsureTables(ctx, tableName, tableAs, kvs)
	if err != nil {
		return memcore.Query{}, nil, err
	}

	return fromRun(ctx, hs.storage, tableName, tableAs, tableExpr)
}

func (hs *HookStorage) EnsureTables(ctx *SessionContext, tableName, tableAs string, iterator parser.KeyValueIterator) error {
	for {
		current, err := iterator.Next()
		if err != nil {
			if memcore.IsNoRows(err) {
				return nil
			}
			return err
		}

		t, value, err := hs.Read(ctx, tableName, current)
		if err != nil {
			hs.storage.Set(tableName, current, t, memcore.Table{}, err)
			return err
		}

		switch v := value.(type) {
		case map[string]interface{}:
			return hs.saveRecordToTable(ctx, tableName, t, current, v)
		case []map[string]interface{}:
			return hs.saveRecordsToTable(ctx, tableName, t, current, v)
		default:
			return errors.New("read '" + tableName + "(" + memcore.KeyValues(current).ToKey() + ")' and return unknown type - " + reflect.TypeOf(value).Name())
		}
	}
}

func (hs *HookStorage) saveRecordToTable(ctx *SessionContext, tableName string, t time.Time, tags []memcore.KeyValue, record map[string]interface{}) error {
	return hs.saveRecordsToTable(ctx, tableName, t, tags, []map[string]interface{}{record})
}

func (hs *HookStorage) saveRecordsToTable(ctx *SessionContext, tableName string, t time.Time, tags []memcore.KeyValue, records []map[string]interface{}) error {
	table, err := memcore.ToTable(records)
	if err != nil {
		return err
	}
	return hs.storage.Set(tableName, tags, t, table, nil)
}

func (hs *HookStorage) Read(ctx *SessionContext, tableName string, tags []memcore.KeyValue) (time.Time, interface{}, error) {
	return time.Time{}, nil, errors.New("not implemented")
}
