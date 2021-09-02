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

			var set = RecordSet{}
			var readDone = false
			var readError error

			return func(ctx Context) (item Record, err error) {
				if !readDone {
					if readError != nil {
						err = readError
						return
					}
					for {
						current, err := next2(ctx)
						if err != nil {
							if !IsNoRows(err) {
								readError = err
								return Record{}, err
							}
							break
						}

						set.Add(current)
					}
					readDone = true
				}

				for {
					item, err = next1(ctx)
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
			var readDone = false
			var readError error

			return func(ctx Context) (item Record, err error) {
				if !readDone {
					if readError != nil {
						err = readError
						return
					}
					for {
						current, err := next2(ctx)
						if err != nil {
							if !IsNoRows(err) {
								readError = err
								return Record{}, err
							}
							break
						}

						s := selector(current)
						set[s] = struct{}{}
					}
					readDone = true
				}

				for {
					item, err = next1(ctx)
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
