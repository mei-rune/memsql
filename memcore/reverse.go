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

			items := make([]Record, 0, 16)
			for {
				current, err := next()
				if err != nil {
					if !IsNoRows(err) {
						return func() (Record, error) {
							return Record{}, err
						}
					}
					break
				}

				items = append(items, current)
			}

			index := len(items) - 1
			return func() (item Record, err error) {
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
