package memcore


type ReferenceQuery struct {
	Query
	IsCopy  bool
	done    bool
	records []Record
	err     error
}


func (q Query) ToReference() *ReferenceQuery {
	query := &ReferenceQuery{}

	query.Iterate = func() Iterator {
		if !query.IsCopy {
			return q.Iterate()
		}


		var index = 0
		return func(ctx Context) (item Record, err error) {
			if !query.done {
				query.records, query.err = q.Results(ctx)
				query.done = true
			}

			if query.err != nil {
				err = query.err
				return
			}

			if len(query.records) <= index {
				err = ErrNoRows
				return
			}

			item = query.records[index]
			index++
			return
		}
	}
	return query
}
