package memcore

// Zip applies a specified function to the corresponding elements of two
// collections, producing a collection of the results.
//
// The method steps through the two input collections, applying function
// resultSelector to corresponding elements of the two collections. The method
// returns a collection of the values that are returned by resultSelector. If
// the input collections do not have the same number of elements, the method
// combines elements until it reaches the end of one of the collections. For
// example, if one collection has three elements and the other one has four, the
// result collection has only three elements.
func (q Query) Zip(q2 Query,
	resultSelector func(Record, Record) Record) Query {

	return Query{
		Iterate: func() Iterator {
			next1 := q.Iterate()
			next2 := q2.Iterate()

			return func(ctx Context) (item Record, err error) {
				item1, err1 := next1(ctx)
				if err1 != nil {
					return Record{}, err1
				}
				item2, err2 := next2(ctx)
				if err2 != nil {
					return Record{}, err2
				}

				return resultSelector(item1, item2), nil
			}
		},
	}
}
