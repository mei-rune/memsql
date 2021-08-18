package memcore

// Intersect produces the set intersection of the source collection and the
// provided input collection. The intersection of two sets A and B is defined as
// the set that contains all the elements of A that also appear in B, but no
// other elements.
func (q Query) Intersect(q2 Query) Query {
	return Query{
		Iterate: func() Iterator {
			next1 := q.Iterate()
			next2 := q2.Iterate()

			set := RecordSet{}
			for {
				current, err := next2()
				if err != nil {
					if !IsNoRows(err) {
						return func() (Record, error) {
							return Record{}, err
						}
					}
					break
				}

				set.Add(current)
			}

			return func() (item Record, err error) {
				for {
					item, err = next1()
					if err != nil {
						return
					}

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
			next1 := q.Iterate()
			next2 := q2.Iterate()

			set := make(map[Value]struct{})
			for {
				current, err := next2()
				if err != nil {
					if !IsNoRows(err) {
						return func() (Record, error) {
							return Record{}, err
						}
					}
					break
				}

				s := selector(current)
				set[s] = struct{}{}
			}

			return func() (item Record, err error) {
				for {
					item, err = next1()
					if err != nil {
						return
					}

					s := selector(item)
					if _, has := set[s]; has {
						delete(set, s)
						return
					}
				}
			}
		},
	}
}
