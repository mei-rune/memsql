package memcore

// IndexOf searches for an element that matches the conditions defined by a specified predicate
// and returns the zero-based index of the first occurrence within the collection. This method
// returns -1 if an item that matches the conditions is not found.
func (q Query) IndexOf(ctx Context, predicate func(Record) bool) (int, error) {
	index := 0
	next := q.Iterate()

	for {
		item, err := next(ctx)
		if err != nil {
			if IsNoRows(err) {
				break
			}
			return -1, err
		}

		if predicate(item) {
			return index, nil
		}
		index++
	}

	return -1, nil
}
