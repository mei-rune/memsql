package memcore

// DefaultIfEmpty returns the elements of the specified sequence
// if the sequence is empty.
func (q Query) DefaultIfEmpty(defaultValue Record) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			state := 1

			return func(ctx Context) (item Record, err error) {
				switch state {
				case 1:
					item, err = next(ctx)
					if err == nil {
						state = 2
					} else if IsNoRows(err) {
						item = defaultValue
						err = nil
						state = -1
					}
					return
				case 2:
					return next(ctx)
				}
				err = ErrNoRows
				return
			}
		},
	}
}
