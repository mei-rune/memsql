package memcore

// Reverse inverts the order of the elements in a collection.
//
// Unlike OrderBy, this sorting method does not consider the actual values
// themselves in determining the order. Rather, it just returns the elements in
// the reverse order from which they are produced by the underlying source.
func (q Query) Reverse() Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			var readDone = false
			var readError error
			var index int

			var items = make([]Record, 0, 16)

			return func(ctx Context) (item Record, err error) {
				if !readDone {
					if readError != nil {
						err = readError
						return
					}

					for {
						current, err := next(ctx)
						if err != nil {
							if !IsNoRows(err) {
								readError = err
								return Record{}, err
							}
							break
						}

						items = append(items, current)
					}

					index = len(items) - 1
					readDone = true
				}

				if index < 0 {
					err = ErrNoRows
					return
				}

				item = items[index]
				index--
				return
			}
		},
	}
}
