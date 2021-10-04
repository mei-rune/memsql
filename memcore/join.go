package memcore

import (
	"github.com/runner-mei/memsql/vm"
)

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
func (q Query) Join(isLeft bool, inner Query,
	outerKeySelector func(Record) (Value, error),
	innerKeySelector func(Record) (Value, error),
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

						innerKey, err := innerKeySelector(innerItem)
						if err != nil {
							readError = err
							return Record{}, err
						}
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

						outKey, err := outerKeySelector(outerItem)
						if err != nil {
							readError = err
							return Record{}, err
						}

						innerGroup, has = innerLookup[outKey]
						if !has {
							// FIXME: outKey 和 innerKey 可能会因为类型不匹配
							//        所以这里用 Equal 再试一下
							for innerKey, group := range innerLookup {
								ok, _ := innerKey.EqualTo(outKey, vm.EmptyCompareOption())
								if ok {
									innerGroup = group
									has = true
									break
								}
							}
						}
						innerLen = len(innerGroup)
						innerIndex = 0

						if isLeft && innerLen == 0 {
							item = resultSelector(outerItem, Record{})
							innerIndex++
							return item, nil
						}
					}
				}

				item = resultSelector(outerItem, innerGroup[innerIndex])
				innerIndex++
				return item, nil
			}
		},
	}
}

func (q Query) FullJoin(inner Query, resultSelector func(outer Record, inner Record) Record) Query {
	return Query{
		Iterate: func() Iterator {
			outernext := q.Iterate()
			innernext := inner.Iterate()

			var innerItems = make([]Record, 0, 16)
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

						innerItems = append(innerItems, innerItem)
					}
					readDone = true

					if len(innerItems) == 0 {
						err = ErrNoRows
						return
					}
				}

				if innerIndex >= innerLen {
					has := false
					for !has {
						outerItem, err = outernext(ctx)
						if err != nil {
							return
						}

						innerGroup = innerItems
						innerLen = len(innerGroup)
						innerIndex = 0

						has = innerIndex < innerLen
					}
				}

				item = resultSelector(outerItem, innerGroup[innerIndex])
				innerIndex++
				return item, nil
			}
		},
	}
}
