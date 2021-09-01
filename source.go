package memsql

import (
	"github.com/runner-mei/memsql/memcore"
)

func WrapForeign(q memcore.Query) ForeignQuery {
	var fq = ForeignQuery{}
	fq.Query.Iterate = func() memcore.Iterator {
		fq.next = q.Iterate()
		return fq.nextItem
	}
	return fq
}

type ForeignQuery struct {
	memcore.Query

	next      memcore.Iterator
	items     []memcore.Record
	readDone  bool
	readError error
	index     int
}

func (query *ForeignQuery) ReadAll(ctx memcore.Context) ([]memcore.Record, error) {
	if !query.readDone {
		if query.readError != nil {
			return nil, query.readError
		}

		err := query.readAll(ctx)
		if err != nil {
			return nil, err
		}
		query.readDone = true
	}
	return query.items, nil
}

func (query *ForeignQuery) readAll(ctx memcore.Context) error {
	for {
		current, err := query.next(ctx)
		if err != nil {
			if !memcore.IsNoRows(err) {
				query.readError = err
				return err
			}
			break
		}

		query.items = append(query.items, current)
	}
	return nil
}

func (query *ForeignQuery) nextItem(ctx memcore.Context) (item memcore.Record, err error) {
	if !query.readDone {
		if query.readError != nil {
			err = query.readError
			return
		}

		err = query.readAll(ctx)
		if err != nil {
			return
		}

		query.readDone = true
	}

	if query.index >= len(query.items) {
		err = memcore.ErrNoRows
		return
	}

	item = query.items[query.index]
	query.index++
	return
}
