package memsql


// SelectMany projects each element of a collection to a Query, iterates
// and flattens the resulting collection into one collection.
//
// The first argument to selector represents the zero-based index of that
// element in the source collection. This can be useful if the elements are in a
// known order and you want to do something with an element at a particular
// index, for example. It can also be useful if you want to retrieve the index
// of one or more elements. The second argument to selector represents the
// element to process.
func (q Query) SelectMany(selector func(int, Record) Query) Query {
	return Query{
		Iterate: func() Iterator {
			outernext := q.Iterate()
			index := 0
			var outer Record
			var outerValid bool
			var innernext Iterator

			return func() (item Record, ok bool) {
				for !ok {
					if !outerValid {
						outer, outerValid = outernext()
						if !outerValid {
							return
						}

						innernext = selector(index, outer).Iterate()
						index++
					}

					item, ok = innernext()
					if !ok {
						outerValid = false
					}
				}

				return
			}
		},
	}
}

// SelectManyByIndexed projects each element of a collection to a Query,
// iterates and flattens the resulting collection into one collection, and
// invokes a result selector function on each element therein. The index of each
// source element is used in the intermediate projected form of that element.
func (q Query) SelectManyBy(selector func(int, Record) Query,
	resultSelector func(Record, Record) Record) Query {

	return Query{
		Iterate: func() Iterator {
			outernext := q.Iterate()
			index := 0
			var outer Record
			var outerValid bool
			var innernext Iterator

			return func() (item Record, ok bool) {
				for !ok {
					if !outerValid {
						outer, outerValid = outernext()
						if !outerValid {
							return
						}

						innernext = selector(index, outer).Iterate()
						index++
					}

					item, ok = innernext()
					if !ok {
						outerValid = false
					}
				}

				item = resultSelector(item, outer)
				return
			}
		},
	}
}