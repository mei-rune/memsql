package memsql

// Where filters a collection of values based on a predicate. Each
// element's index is used in the logic of the predicate function.
//
// The first argument represents the zero-based index of the element within
// collection. The second argument of predicate represents the element to test.
func (q Query) Where(predicate func(int, Record) bool) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			index := 0

			return func() (item Record, ok bool) {
				for item, ok = next(); ok; item, ok = next() {
					if predicate(index, item) {
						index++
						return
					}

					index++
				}
				return
			}
		},
	}
}
