package memsql

// Distinct method returns distinct elements from a collection. The result is an
// unordered collection that contains no duplicate values.
func (q Query) Distinct() Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			set := RecordSet{}

			return func() (item Record, ok bool) {
				for item, ok = next(); ok; item, ok = next() {
					if !set.Has(item) {
						set.Add(item)
						return
					}
				}

				return
			}
		},
	}
}

// Distinct method returns distinct elements from a collection. The result is an
// ordered collection that contains no duplicate values.
//
// NOTE: Distinct method on OrderedQuery type has better performance than
// Distinct method on Query type.
func (oq OrderedQuery) Distinct() OrderedQuery {
	return OrderedQuery{
		orders: oq.orders,
		Query: Query{
			Iterate: func() Iterator {
				next := oq.Iterate()
				var prev Record

				return func() (item Record, ok bool) {
					for item, ok = next(); ok; item, ok = next() {
						if item.EqualTo(prev) {
							prev = item
							return
						}
					}

					return
				}
			},
		},
	}
}

// DistinctBy method returns distinct elements from a collection. This method
// executes selector function for each element to determine a value to compare.
// The result is an unordered collection that contains no duplicate values.
func (q Query) DistinctBy(selector func(Record) Value) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			set := make(map[Value]struct{})

			return func() (item Record, ok bool) {
				for item, ok = next(); ok; item, ok = next() {
					s := selector(item)
					if _, has := set[s]; !has {
						set[s] = struct{}{}
						return
					}
				}

				return
			}
		},
	}
}