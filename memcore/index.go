package memcore

// IndexOf searches for an element that matches the conditions defined by a specified predicate
// and returns the zero-based index of the first occurrence within the collection. This method
// returns -1 if an item that matches the conditions is not found.
func (q Query) IndexOf(predicate func(Record) bool) int {
	index := 0
	next := q.Iterate()

	for item, ok := next(); ok; item, ok = next() {
		if predicate(item) {
			return index
		}
		index++
	}

	return -1
}
