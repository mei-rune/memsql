package memcore

// Aggregate applies an accumulator function over a sequence.
//
// Aggregate method makes it simple to perform a calculation over a sequence of
// values. This method works by calling f() one time for each element in source
// except the first one. Each time f() is called, Aggregate passes both the
// element from the sequence and an aggregated value (as the first argument to
// f()). The first element of source is used as the initial aggregate value. The
// result of f() replaces the previous aggregated value.
//
// Aggregate returns the final result of f().
func (q Query) Aggregate(ctx Context, f func(Record, Record) (Record, error)) (result Record, err error) {
	next := q.Iterate()

	result, err = next(ctx)
	if err != nil {
		if IsNoRows(err) {
			err = nil
		}
		return
	}

	for {
		current, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}
			return Record{}, e
		}

		result, err = f(result, current)
		if err != nil {
			return
		}
	}
	return
}

// AggregateWithSeed applies an accumulator function over a sequence. The
// specified seed value is used as the initial accumulator value.
//
// Aggregate method makes it simple to perform a calculation over a sequence of
// values. This method works by calling f() one time for each element in source
// except the first one. Each time f() is called, Aggregate passes both the
// element from the sequence and an aggregated value (as the first argument to
// f()). The value of the seed parameter is used as the initial aggregate value.
// The result of f() replaces the previous aggregated value.
//
// Aggregate returns the final result of f().
func (q Query) AggregateWithSeed(ctx Context, seed Record,
	f func(Record, Record) (Record, error)) (result Record, err error) {
	next := q.Iterate()
	result = seed

	for {
		current, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}
			return Record{}, e
		}

		result, err = f(result, current)
		if err != nil {
			return
		}
	}
	return
}

// AggregateWithSeedBy applies an accumulator function over a sequence. The
// specified seed value is used as the initial accumulator value, and the
// specified function is used to select the result value.
//
// Aggregate method makes it simple to perform a calculation over a sequence of
// values. This method works by calling f() one time for each element in source.
// Each time func is called, Aggregate passes both the element from the sequence
// and an aggregated value (as the first argument to func). The value of the
// seed parameter is used as the initial aggregate value. The result of func
// replaces the previous aggregated value.
//
// The final result of func is passed to resultSelector to obtain the final
// result of Aggregate.
func (q Query) AggregateWithSeedBy(ctx Context, seed Record,
	f func(Record, Record) (Record, error),
	resultSelector func(Record) (Record, error)) (result Record, err error) {

	next := q.Iterate()
	result = seed

	for {
		current, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}
			return Record{}, e
		}

		result, err = f(result, current)
		if err != nil {
			return
		}
	}

	return resultSelector(result)
}