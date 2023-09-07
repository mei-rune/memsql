package memsql

import (
	"reflect"
	"time"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/parser"
	"github.com/xwb1989/sqlparser"
	// "github.com/cabify/timex"
)

type ReadFunc func(ctx *SessionContext, tableName string, tags []memcore.KeyValue) (time.Time, interface{}, error)


func NewHookStorage(storage memcore.Storage, read ReadFunc) *HookStorage {
	return &HookStorage{
		Storage:storage, 
	 	Read: read,
	 }
}

var _ Storage = &HookStorage{}

type HookStorage struct {
	Storage memcore.Storage

 	Read ReadFunc
}

func (hs *HookStorage) From(ctx *SessionContext, tableName TableAlias, tableExpr sqlparser.Expr, trace func(TableName)) (memcore.Query, error) {
	ctx.OnIniting(func() error {
		kvs, err := parser.ToKeyValues(ctx, tableExpr, tableName, nil)
		if err != nil {
			return   err
		}

		err = hs.EnsureTables(ctx, tableName, kvs)
		if err != nil {
			return  err
		}
		return nil
	})

	return memcore.Query{
		Iterate: func() memcore.Iterator {
			q, err := fromRun(ctx, hs.Storage, tableName, tableExpr, trace)
			if err != nil {
				return func(ctx memcore.Context) (Record, error) {
					return memcore.Record{}, err
				}
			}
			return q.Iterate()
		},
	}, nil
}

func (hs *HookStorage) EnsureTables(ctx *SessionContext, tableName TableAlias, iterator parser.KeyValueIterator) error {
	if iterator == nil {
		return nil
	}
	predateLimit := hs.GetPredateLimit(ctx)
	for {
		tags, err := iterator.Next(nil)
		if err != nil {
			if memcore.IsNoRows(err) {
				return nil
			}
			return err
		}

		if hs.Storage.Exists(tableName.Name, tags, predateLimit) {
			ctx.Debuger.ReadSkip(tableName.Name, tags)
			continue
		}

		t, value, err := hs.Read(ctx, tableName.Name, tags)
		if err != nil {
			ctx.Debuger.ReadError(tableName.Name, tags, err)
			hs.Storage.Set(tableName.Name, tags, t, memcore.Table{}, err)
			return err
		}

		ctx.Debuger.ReadOk(tableName.Name, tags, value)

		switch v := value.(type) {
		case map[string]interface{}:
			err = hs.saveRecordToTable(ctx, tableName.Name, t, tags, v)
		case []map[string]interface{}:
			err = hs.saveRecordsToTable(ctx, tableName.Name, t, tags, v)
		default:
			err = errors.New("read '" + tableName.Name + "(" + memcore.KeyValues(tags).ToKey() + ")' and return unknown type - " + reflect.TypeOf(value).Name())
		}
		if err != nil {
			return err
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
	return hs.Storage.Set(tableName, tags, t, table, nil)
}

func (hs *HookStorage) GetPredateLimit(ctx *SessionContext) time.Time {
	// TODO: xxx
	return time.Now().Add(1 * time.Minute)
}

func ReadValues(values map[string][]map[string]interface{}) ReadFunc {
	return func(ctx *SessionContext, tableName string, tags []memcore.KeyValue) (time.Time, interface{}, error) {
		value, ok := values[tableName + "-" + memcore.KeyValues(tags).ToKey()]
		if !ok {
			return time.Time{}, nil, memcore.ErrNotFound 
		}
		return time.Now(), value, nil
	}
}
