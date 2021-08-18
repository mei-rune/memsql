package memcore

// Except produces the set difference of two sequences. The set difference is
// the members of the first sequence that don't appear in the second sequence.
func (q Query) Except(q2 Query) Query {
	return Query{
		Iterate: func() Iterator {
			next1 := q.Iterate()
			next2 := q2.Iterate()
			set := RecordSet{}

			readDone := false
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

					if !set.Has(item) {
						return
					}
				}
			}
		},
	}
}

// ExceptBy invokes a transform function on each element of a collection and
// produces the set difference of two sequences. The set difference is the
// members of the first sequence that don't appear in the second sequence.
func (q Query) ExceptBy(q2 Query,
	selector func(Record) Value) Query {
	return Query{
		Iterate: func() Iterator {
			next1 := q.Iterate()
			next2 := q2.Iterate()

			set := make(map[Value]struct{})
			readDone := false
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
					if _, has := set[s]; !has {
						return
					}
				}
			}
		},
	}
}
