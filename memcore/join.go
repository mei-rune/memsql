package memcore

// Join correlates the elements of two collection based on matching keys.
//
// A join refers to the operation of correlating the elements of two sources of
// information based on a common key. Join brings the two information sources
// and the keys by which they are matched together in one method call. This
// differs from the use of SelectMany, which requires more than one method call
// to perform the same operation.
//
// Join preserves the order of the elements of outer collection, and for each of
// these elements, the order of the matching elements of inner.
func (q Query) Join(inner Query,
	outerKeySelector func(Record) Value,
	innerKeySelector func(Record) Value,
	resultSelector func(outer Record, inner Record) Record) Query {

	return Query{
		Iterate: func() Iterator {
			outernext := q.Iterate()
			innernext := inner.Iterate()

			var innerLookup = make(map[Value][]Record)
			var readDone = false
			var readError error


			var outerItem Record
			var innerGroup []Record
			innerLen, innerIndex := 0, 0

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

				if innerIndex >= innerLen {
					has := false
					for !has {
						outerItem, err = outernext(ctx)
						if err != nil {
							return
						}

						innerGroup, has = innerLookup[outerKeySelector(outerItem)]
						innerLen = len(innerGroup)
						innerIndex = 0
					}
				}

				item = resultSelector(outerItem, innerGroup[innerIndex])
				innerIndex++
				return item, nil
			}
		},
	}
}
