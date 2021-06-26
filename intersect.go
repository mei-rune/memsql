package memsql

// Intersect produces the set intersection of the source collection and the
// provided input collection. The intersection of two sets A and B is defined as
// the set that contains all the elements of A that also appear in B, but no
// other elements.
func (q Query) Intersect(q2 Query) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			next2 := q2.Iterate()

			set := RecordSet{}
			for item, ok := next2(); ok; item, ok = next2() {
				set.Add(item)
			}

			return func() (item Record, ok bool) {
				for item, ok = next(); ok; item, ok = next() {
					if idx := set.Search(item); idx >= 0 {
						set.Delete(idx)
						return
					}
				}

				return
			}
		},
	}
}

// IntersectBy produces the set intersection of the source collection and the
// provided input collection. The intersection of two sets A and B is defined as
// the set that contains all the elements of A that also appear in B, but no
// other elements.
//
// IntersectBy invokes a transform function on each element of both collections.
func (q Query) IntersectBy(q2 Query,
	selector func(Record) Value) Query {

	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			next2 := q2.Iterate()

			set := make(map[Value]struct{})
			for item, ok := next2(); ok; item, ok = next2() {
				s := selector(item)
				set[s] = struct{}{}
			}

			return func() (item Record, ok bool) {
				for item, ok = next(); ok; item, ok = next() {
					s := selector(item)
					if _, has := set[s]; has {
						delete(set, s)
						return
					}
				}

				return
			}
		},
	}
}