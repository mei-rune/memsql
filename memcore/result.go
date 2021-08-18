package memcore

// All determines whether all elements of a collection satisfy a condition.
func (q Query) All(ctx Context, predicate func(Record) bool) (bool, error) {
	next := q.Iterate()

	for {
		item, err := next(ctx)
		if err != nil {
			if IsNoRows(err) {
				break
			}

			return false, err
		}

		if !predicate(item) {
			return false, nil
		}
	}

	return true, nil
}

// Any determines whether any element of a collection exists.
func (q Query) Any(ctx Context) (bool, error) {
	_, err := q.Iterate()(ctx)
	if err != nil {
		if IsNoRows(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// AnyWith determines whether any element of a collection satisfies a condition.
func (q Query) AnyWith(ctx Context, predicate func(Record) bool) (bool, error) {
	next := q.Iterate()
	for {
		item, err := next(ctx)
		if err != nil {
			if IsNoRows(err) {
				break
			}

			return false, err
		}

		if predicate(item) {
			return true, nil
		}
	}

	return false, nil
}

// Count returns the number of elements in a collection.
func (q Query) Count(ctx Context) (r int, err error) {
	next := q.Iterate()
	for {
		_, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}
			return 0, e
		}

		r++
	}

	return
}

// CountWith returns a number that represents how many elements in the specified
// collection satisfy a condition.
func (q Query) CountWith(ctx Context, predicate func(Record) bool) (r int, err error) {
	next := q.Iterate()

	for {
		item, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}
			return 0, e
		}

		if predicate(item) {
			r++
		}
	}

	return
}

// First returns the first element of a collection.
func (q Query) First(ctx Context) (Record, bool, error) {
	item, err := q.Iterate()(ctx)
	if err != nil {
		if IsNoRows(err) {
			return Record{}, false, nil
		}
		return Record{}, false, err
	}
	return item, true, nil
}

// FirstWith returns the first element of a collection that satisfies a
// specified condition.
func (q Query) FirstWith(ctx Context, predicate func(Record) bool) (Record, bool, error) {
	next := q.Iterate()
	for {
		item, err := next(ctx)
		if err != nil {
			if IsNoRows(err) {
				return Record{}, false, nil
			}
			return Record{}, false, err
		}

		if predicate(item) {
			return item, true, nil
		}
	}
}

// ForEach performs the specified action on each element of a collection.
//
// The first argument to action represents the zero-based index of that
// element in the source collection. This can be useful if the elements are in a
// known order and you want to do something with an element at a particular
// index, for example. It can also be useful if you want to retrieve the index
// of one or more elements. The second argument to action represents the
// element to process.
func (q Query) ForEach(ctx Context, action func(int, Record) error) error {
	next := q.Iterate()
	index := 0
	for {
		item, err := next(ctx)
		if err != nil {
			if IsNoRows(err) {
				return nil
			}
			return err
		}

		if err := action(index, item); err != nil {
			return err
		}
		index++
	}
}

// Last returns the last element of a collection.
func (q Query) Last(ctx Context) (r Record, exists bool, err error) {
	next := q.Iterate()

	for {
		item, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}

			err = e
			return
		}

		r = item
		exists = true
		err = nil
	}
	return
}

// LastWith returns the last element of a collection that satisfies a specified
// condition.
func (q Query) LastWith(ctx Context, predicate func(Record) bool) (r Record, exists bool, err error) {
	next := q.Iterate()

	for {
		item, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}

			err = e
			return
		}

		if predicate(item) {
			r = item
			exists = true
			err = nil
		}
	}
	return
}

// Results iterates over a collection and returnes slice of interfaces
func (q Query) Results(ctx Context) (r []Record, err error) {
	next := q.Iterate()

	for {
		item, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}

			err = e
			return
		}

		r = append(r, item)
	}

	return
}

// SequenceEqual determines whether two collections are equal.
func (q Query) SequenceEqual(ctx Context, q2 Query) (bool, error) {
	next1 := q.Iterate()
	next2 := q2.Iterate()

	for {
		item1, e := next1(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}

			return false, e
		}

		item2, e := next2(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}

			return false, e
		}

		ok3, err := item1.EqualTo(item2, emptyCompareOption)
		if err != nil {
			return false, err
		}
		if !ok3 {
			return false, nil
		}
	}

	_, err := next2(ctx)
	if err == nil {
		return false, nil
	}
	if IsNoRows(err) {
		return true, nil
	}
	return false, err
}

// Single returns the only element of a collection, and nil if there is not
// exactly one element in the collection.
func (q Query) Single(ctx Context) (Record, bool, error) {
	next := q.Iterate()
	item, err := next(ctx)
	if err != nil {
		if IsNoRows(err) {
			err = nil
		}
		return Record{}, false, err
	}

	_, err = next(ctx)
	if err == nil {
		return Record{}, false, nil
	}
	if !IsNoRows(err) {
		return Record{}, false, err
	}

	return item, true, nil
}

// SingleWith returns the only element of a collection that satisfies a
// specified condition, and nil if more than one such element exists.
func (q Query) SingleWith(ctx Context, predicate func(Record) bool) (r Record, found bool, err error) {
	next := q.Iterate()

	for {
		item, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}
			err = e
			return
		}

		if predicate(item) {
			if found {
				return Record{}, false, nil
			}

			found = true
			r = item
		}
	}

	return r, found, nil
}

// // ToChannel iterates over a collection and outputs each element to a channel,
// // then closes it.
// func (q Query) ToChannel(result chan<- Record) {
// 	next := q.Iterate()

// 	for item, ok := next(); ok; item, ok = next() {
// 		result <- item
// 	}

// 	close(result)
// }
