package memcore

import (
 "sort"
 	"github.com/runner-mei/memsql/vm"
)

type comparer func(Value, Value) int

type order struct {
	selector func(Record) (Value, error)
	compare  comparer
	desc     bool
}

// OrderedQuery is the type returned from OrderByAscending, OrderByDescending ThenByAscending and
// ThenByDescending functions.
type OrderedQuery struct {
	Query
	original Query
	orders   []order
}

// OrderByAscending sorts the elements of a collection in ascending order. Elements are
// sorted according to a key.
func (q Query) OrderByAscending(selector func(Record) (Value, error)) OrderedQuery {
	return OrderedQuery{
		orders:   []order{{selector: selector}},
		original: q,
		Query: Query{
			Iterate: func() Iterator {
				var items []Record
				var readDone = false
				var readError error

				var length = 0
				var index = 0

				return func(ctx Context) (item Record, err error) {
					if !readDone {
						if readError != nil {
							err = readError
							return
						}

						items, err = q.sort(ctx, []order{{selector: selector}})
						if err != nil {
							readError = err
							return Record{}, err
						}

						length = len(items)
						index = 0
						readDone = true
					}

					if index < length {
						item = items[index]
						index++
						return
					}
					err = ErrNoRows
					return
				}
			},
		},
	}
}

// OrderByDescending sorts the elements of a collection in descending order.
// Elements are sorted according to a key.
func (q Query) OrderByDescending(selector func(Record) (Value, error)) OrderedQuery {
	return OrderedQuery{
		orders:   []order{{selector: selector, desc: true}},
		original: q,
		Query: Query{
			Iterate: func() Iterator {
				var items []Record
				var readDone = false
				var readError error

				var length = 0
				var index = 0

				return func(ctx Context) (item Record, err error) {
					if !readDone {
						if readError != nil {
							err = readError
							return
						}

						items, err = q.sort(ctx, []order{{selector: selector, desc: true}})
						if err != nil {
							readError = err
							return Record{}, err
						}

						length = len(items)
						index = 0
						readDone = true
					}


					if index < length {
						item = items[index]
						index++
						return
					}

					err = ErrNoRows
					return
				}
			},
		},
	}
}

// ThenByAscending performs a subsequent ordering of the elements in a collection in
// ascending order. This method enables you to specify multiple sort criteria by
// applying any number of ThenByAscending or ThenByDescending methods.
func (oq OrderedQuery) ThenByAscending(selector func(Record) (Value, error)) OrderedQuery {
	return OrderedQuery{
		orders:   append(oq.orders, order{selector: selector}),
		original: oq.original,
		Query: Query{
			Iterate: func() Iterator {
				var items []Record
				var readDone = false
				var readError error

				var length = 0
				var index = 0

				return func(ctx Context) (item Record, err error) {
					if !readDone {
						if readError != nil {
							err = readError
							return
						}

						items, err = oq.original.sort(ctx, append(oq.orders, order{selector: selector}))
						if err != nil {
							readError = err
							return Record{}, err
						}

						length = len(items)
						index = 0
						readDone = true
					}

					if index < length {
						item = items[index]
						index++
						return
					}

					err = ErrNoRows
					return
				}
			},
		},
	}
}

// ThenByDescending performs a subsequent ordering of the elements in a
// collection in descending order. This method enables you to specify multiple
// sort criteria by applying any number of ThenBy or ThenByDescending methods.
func (oq OrderedQuery) ThenByDescending(selector func(Record) (Value, error)) OrderedQuery {
	return OrderedQuery{
		orders:   append(oq.orders, order{selector: selector, desc: true}),
		original: oq.original,
		Query: Query{
			Iterate: func() Iterator {
				var items []Record
				var readDone = false
				var readError error

				var length = 0
				var index = 0

				return func(ctx Context) (item Record, err error) {
					if !readDone {
						if readError != nil {
							err = readError
							return
						}

						items, err = oq.original.sort(ctx, append(oq.orders, order{selector: selector, desc: true}))
						if err != nil {
							readError = err
							return Record{}, err
						}

						length = len(items)
						index = 0
						readDone = true
					}

					if index < length {
						item = items[index]
						index++
						return
					}

					err = ErrNoRows
					return
				}
			},
		},
	}
}

// Sort returns a new query by sorting elements with provided less function in
// ascending order. The comparer function should return true if the parameter i
// is less than j. While this method is uglier than chaining OrderBy,
// OrderByDescending, ThenBy and ThenByDescending methods, it's performance is
// much better.
func (q Query) Sort(less func(i, j Record) bool) Query {
	return Query{
		Iterate: func() Iterator {
			var items []Record
			var readDone = false
			var readError error

			var length = 0
			var index = 0

			return func(ctx Context) (item Record, err error) {
					if !readDone {
						if readError != nil {
							err = readError
							return
						}

						items, err = q.lessSort(ctx, less)
						if err != nil {
							readError = err
							return Record{}, err
						}

						length = len(items)
						index = 0
						readDone = true
					}

				if index < length {
					item = items[index]
					index++
					return
				}

				err = ErrNoRows
				return
			}
		},
	}
}

type sorter struct {
	items []Record
	less  func(i, j Record) bool
}

func (s sorter) Len() int {
	return len(s.items)
}

func (s sorter) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

func (s sorter) Less(i, j int) bool {
	return s.less(s.items[i], s.items[j])
}

func (q Query) sort(ctx Context, orders []order) (r []Record, err error) {
	next := q.Iterate()
	for {
		item, err := next(ctx)
		if err != nil {
			if IsNoRows(err) {
				break
			}
			return nil, err
		}

		r = append(r, item)
	}

	if len(r) == 0 {
		return
	}

	for i := range orders {
		if orders[i].compare != nil {
			continue
		}
		orders[i].compare = func(a Value, b Value) int {
			ret, err := a.CompareTo(b, vm.EmptyCompareOption())
			if err != nil {
				panic(err)
			}
			return ret
		}
	}

	s := sorter{
		items: r,
		less: func(i, j Record) bool {
			for _, order := range orders {
				x, e := order.selector(i)
				if e != nil {
					err = e
					return false
				}
				y, e := order.selector(j)
				if e != nil {
					err = e
					return true
				}
				switch order.compare(x, y) {
				case 0:
					continue
				case -1:
					return !order.desc
				default:
					return order.desc
				}
			}
			return false
		}}

	sort.Sort(s)
	return
}

func (q Query) lessSort(ctx Context, less func(i, j Record) bool) (r []Record, err error) {
	next := q.Iterate()
	for {
		item, err := next(ctx)
		if err != nil {
			if IsNoRows(err) {
				break
			}
			return nil, err
		}

		r = append(r, item)
	}

	s := sorter{items: r, less: less}

	sort.Sort(s)
	return
}
