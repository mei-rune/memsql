package memcore

// All determines whether all elements of a collection satisfy a condition.
func (q Query) All(predicate func(Record) bool) bool {
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		if !predicate(item) {
			return false
		}
	}

	return true
}

// Any determines whether any element of a collection exists.
func (q Query) Any() bool {
	_, ok := q.Iterate()()
	return ok
}

// AnyWith determines whether any element of a collection satisfies a condition.
func (q Query) AnyWith(predicate func(Record) bool) bool {
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		if predicate(item) {
			return true
		}
	}

	return false
}

// Count returns the number of elements in a collection.
func (q Query) Count() (r int) {
	next := q.Iterate()

	for _, ok := next(); ok; _, ok = next() {
		r++
	}

	return
}

// CountWith returns a number that represents how many elements in the specified
// collection satisfy a condition.
func (q Query) CountWith(predicate func(Record) bool) (r int) {
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		if predicate(item) {
			r++
		}
	}

	return
}

// First returns the first element of a collection.
func (q Query) First() (Record, bool) {
	item, ok := q.Iterate()()
	return item, ok
}

// FirstWith returns the first element of a collection that satisfies a
// specified condition.
func (q Query) FirstWith(predicate func(Record) bool) (Record, bool) {
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		if predicate(item) {
			return item, true
		}
	}

	return Record{}, false
}

// ForEach performs the specified action on each element of a collection.
//
// The first argument to action represents the zero-based index of that
// element in the source collection. This can be useful if the elements are in a
// known order and you want to do something with an element at a particular
// index, for example. It can also be useful if you want to retrieve the index
// of one or more elements. The second argument to action represents the
// element to process.
func (q Query) ForEach(action func(int, Record)) {
	next := q.Iterate()
	index := 0

	for item, ok := next(); ok; item, ok = next() {
		action(index, item)
		index++
	}
}

// Last returns the last element of a collection.
func (q Query) Last() (r Record, ok bool) {
	next := q.Iterate()

	for item, exists := next(); exists; item, exists = next() {
		r = item
		ok = true
	}
	return
}

// LastWith returns the last element of a collection that satisfies a specified
// condition.
func (q Query) LastWith(predicate func(Record) bool) (r Record, ok bool) {
	next := q.Iterate()

	for item, exists := next(); exists; item, exists = next() {
		if predicate(item) {
			r = item
			ok = true
		}
	}

	return
}

// Results iterates over a collection and returnes slice of interfaces
func (q Query) Results() (r []Record) {
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		r = append(r, item)
	}

	return
}

// SequenceEqual determines whether two collections are equal.
func (q Query) SequenceEqual(q2 Query) bool {
	next := q.Iterate()
	next2 := q2.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		item2, ok2 := next2()
		if !ok2 {
			return false
		}
		ok3, _ := item.EqualTo(item2, emptyCompareOption)
		if !ok3 {
			return false
		}
	}

	_, ok2 := next2()
	return !ok2
}

// Single returns the only element of a collection, and nil if there is not
// exactly one element in the collection.
func (q Query) Single() (Record, bool) {
	next := q.Iterate()
	item, ok := next()
	if !ok {
		return Record{}, false
	}

	_, ok = next()
	if ok {
		return Record{}, false
	}

	return item, true
}

// SingleWith returns the only element of a collection that satisfies a
// specified condition, and nil if more than one such element exists.
func (q Query) SingleWith(predicate func(Record) bool) (r Record, found bool) {
	next := q.Iterate()

	for item, exists := next(); exists; item, exists = next() {
		if predicate(item) {
			if found {
				return Record{}, false
			}

			found = true
			r = item
		}
	}

	return r, found
}

// ToChannel iterates over a collection and outputs each element to a channel,
// then closes it.
func (q Query) ToChannel(result chan<- Record) {
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		result <- item
	}

	close(result)
}
