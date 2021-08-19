package memcore

// Where filters a collection of values based on a predicate. Each
// element's index is used in the logic of the predicate function.
//
// The first argument represents the zero-based index of the element within
// collection. The second argument of predicate represents the element to test.
func (q Query) Where(predicate func(int, Record) (bool, error)) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			index := 0

			return func(ctx Context) (item Record, err error) {
				for {
					item, err = next(ctx)
					if err != nil {
						return
					}

					var ok bool
					ok, err = predicate(index, item)
					if err != nil {
						return
					}
					if ok {
						index++
						return
					}

					index++
				}
			}
		},
	}
}
