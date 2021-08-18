package memcore

// GroupJoin correlates the elements of two collections based on key equality,
// and groups the results.
//
// This method produces hierarchical results, which means that elements from
// outer query are paired with collections of matching elements from inner.
// GroupJoin enables you to base your results on a whole set of matches for each
// element of outer query.
//
// The resultSelector function is called only one time for each outer element
// together with a collection of all the inner elements that match the outer
// element. This differs from the Join method, in which the result selector
// function is invoked on pairs that contain one element from outer and one
// element from inner.
//
// GroupJoin preserves the order of the elements of outer, and for each element
// of outer, the order of the matching elements from inner.
func (q Query) GroupJoin(inner Query,
	outerKeySelector func(Record) Value,
	innerKeySelector func(Record) Value,
	resultSelector func(outer Record, inners []Record) Record) Query {

	return Query{
		Iterate: func() Iterator {
			outernext := q.Iterate()
			innernext := inner.Iterate()

			var innerLookup = make(map[Value][]Record)
			var readDone = false
			var readError error


			return func(ctx Context) (item Record, err error) {
				if !readDone {
					if readError != nil {
						err = readError
						return
					}

					for {
						innerItem, err := innernext(ctx)
						if err != nil {
							if !IsNoRows(err) {
								readError = err
								return Record{}, err
							}
							break
						}

						innerKey := innerKeySelector(innerItem)
						innerLookup[innerKey] = append(innerLookup[innerKey], innerItem)
					}
					readDone = true
				}

				if item, err = outernext(ctx); err != nil {
					return
				}

				if group, has := innerLookup[outerKeySelector(item)]; !has {
					item = resultSelector(item, []Record{})
				} else {
					item = resultSelector(item, group)
				}

				return
			}
		},
	}
}
