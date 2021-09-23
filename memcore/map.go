package memcore


func (q Query) Map(mapFunc func(Context, Record) (Record, error)) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()

			return func(ctx Context) (item Record, err error) {
				item, err = next(ctx)
				if err != nil {
					return
				}

				return mapFunc(ctx, item)
			}
		},
	}
}
