package memcore

// Append inserts an item to the end of a collection, so it becomes the last
// item.
func (q Query) Append(item Record) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			appended := false

			return func() (Record, error) {
				i, err := next()
				if err == nil {
					return i, nil
				}
				if !IsNoRows(err) {
					return Record{}, err
				}

				if !appended {
					appended = true
					return item, nil
				}

				return Record{}, ErrNoRows
			}
		},
	}
}

// Concat concatenates two collections.
//
// The Concat method differs from the Union method because the Concat method
// returns all the original elements in the input sequences. The Union method
// returns only unique elements.
func (q Query) Concat(q2 Query) Query {
	return q.UnionAll(q2)
}

// Prepend inserts an item to the beginning of a collection, so it becomes the
// first item.
func (q Query) Prepend(item Record) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			prepended := false

			return func() (Record, error) {
				if prepended {
					return next()
				}

				prepended = true
				return item, nil
			}
		},
	}
}
