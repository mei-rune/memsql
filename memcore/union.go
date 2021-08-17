package memcore

// Union produces the set union of two collections.
//
// This method excludes duplicates from the return set. This is different
// behavior to the Concat method, which returns all the elements in the input
// collection including duplicates.
func (q Query) Union(q2 Query) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			next2 := q2.Iterate()

			set := RecordSet{}
			use1 := true

			return func() (item Record, ok bool) {
				if use1 {
					for item, ok = next(); ok; item, ok = next() {
						if !set.Has(item) {
							set.Add(item)
							return
						}
					}

					use1 = false
				}

				for item, ok = next2(); ok; item, ok = next2() {
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


func (q Query) UnionAll(q2 Query) Query {
	return Query{
		Iterate: func() Iterator {
			next1 := q.Iterate()
			next2 := q2.Iterate()
			use1 := true

			return func() (item Record, ok bool) {
				if use1 {
					item, ok = next1()
					if ok {
						return
					}
					use1 = false
				}

				item, ok = next2()
				return
			}
		},
	}
}
