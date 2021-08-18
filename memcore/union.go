package memcore

// Union produces the set union of two collections.
//
// This method excludes duplicates from the return set. This is different
// behavior to the Concat method, which returns all the elements in the input
// collection including duplicates.
func (q Query) Union(q2 Query) Query {
	return Query{
		Iterate: func() Iterator {
			next1 := q.Iterate()
			next2 := q2.Iterate()

			set := RecordSet{}
			use1 := true

			return func(ctx Context) (item Record, err error) {
				if use1 {
					for {
						item, err = next1(ctx)
						if err != nil {
							if IsNoRows(err) {
								break
							}
							return
						}

						if !set.Has(item) {
							set.Add(item)
							return
						}
					}

					use1 = false
				}

				for {
					item, err = next2(ctx)
					if err != nil {
						return
					}

					if !set.Has(item) {
						set.Add(item)
						return
					}
				}

				return
			}
		},
	}
}

// UnionAll concatenates two collections.
//
// The UnionAll method differs from the Union method because the UnionAll method
// returns all the original elements in the input sequences. The Union method
// returns only unique elements.
func (q Query) UnionAll(q2 Query) Query {
	return Query{
		Iterate: func() Iterator {
			next1 := q.Iterate()
			next2 := q2.Iterate()
			use1 := true

			return func(ctx Context) (item Record, err error) {
				if use1 {
					item, err = next1(ctx)
					if err == nil {
						return
					}
					if !IsNoRows(err) {
						return
					}

					use1 = false
				}

				item, err = next2(ctx)
				return
			}
		},
	}
}
