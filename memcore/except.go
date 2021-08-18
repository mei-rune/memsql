package memcore

// Except produces the set difference of two sequences. The set difference is
// the members of the first sequence that don't appear in the second sequence.
func (q Query) Except(q2 Query) Query {
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
					if _, has := set[s]; !has {
						return
					}
				}
			}
		},
	}
}
