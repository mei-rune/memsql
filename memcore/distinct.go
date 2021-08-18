package memcore

// Distinct method returns distinct elements from a collection. The result is an
// unordered collection that contains no duplicate values.
func (q Query) Distinct() Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			set := RecordSet{}

			return func(ctx Context) (item Record, err error) {
				for {
					item, err = next(ctx)
					if err != nil {
						return
					}

					if !set.Has(item) {
						set.Add(item)
						return
					}
				}
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
				var hasPrev bool

				return func(ctx Context) (item Record, err error) {
					if !hasPrev {
						item, err = next(ctx)
						if err != nil {
							return
						}
						prev = item
						hasPrev = true
						return
					}

					var ok bool
					for {
						item, err = next(ctx)
						if err != nil {
							return
						}

						ok, err = item.EqualTo(prev, emptyCompareOption)
						if err != nil {
							return
						}
						if !ok {
							prev = item
							return
						}
					}
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

			return func(ctx Context) (item Record, err error) {
				for {
					item, err = next(ctx)
					if err != nil {
						return
					}
					s := selector(item)
					if _, has := set[s]; !has {
						set[s] = struct{}{}
						return
					}
				}
			}
		},
	}
}
