package memcore

import "github.com/runner-mei/memsql/vm"

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
func (q Query) Aggregate(ctx Context, f func(Context, Record, Record) (Record, error)) (result Record, err error) {
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

		result, err = f(ctx, result, current)
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
	f func(Context, Record, Record) (Record, error)) (result Record, err error) {
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

		result, err = f(ctx, result, current)
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
	f func(Context, Record, Record) (Record, error),
	resultSelector func(Context, Record) (Record, error)) (result Record, err error) {

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

		result, err = f(ctx, result, current)
		if err != nil {
			return
		}
	}

	return resultSelector(ctx, result)
}

type AggregatorFactory interface {
	Create() Aggregator
}

type AggregatorFactoryFunc func() Aggregator

func (f AggregatorFactoryFunc) Create() Aggregator {
	return f()
}

type Aggregator interface {
	Agg(Context, Record) error

	Result(Context) (Value, error)
}

type aggregatorWraper struct {
	Aggregator vm.Aggregator
	ReadValue  func(Context, Record) (Value, error)
}

func (w aggregatorWraper) Agg(ctx Context, r Record) error {
	value, err := w.ReadValue(ctx, r)
	if err != nil {
		return err
	}
	return w.Aggregator.Agg(value)
}

func (w aggregatorWraper) Result(ctx Context) (Value, error) {
	return w.Aggregator.Result()
}

func AggregatorFunc(create func() vm.Aggregator,
	readValue func(Context, Record) (Value, error)) AggregatorFactoryFunc {
	return AggregatorFactoryFunc(func() Aggregator {
		return aggregatorWraper{
			Aggregator: create(),
			ReadValue:  readValue,
		}
	})
}

func (q Query) AggregateWithFunc(ctx Context, names []string, aggregators []Aggregator) (result Record, err error) {
	next := q.Iterate()

	for {
		current, e := next(ctx)
		if e != nil {
			if IsNoRows(e) {
				break
			}
			return Record{}, e
		}

		for idx := range aggregators {
			err = aggregators[idx].Agg(ctx, current)
			if err != nil {
				return
			}
		}
	}

	for idx := range aggregators {
		var value Value
		value, err = aggregators[idx].Result(ctx)
		if err != nil {
			return
		}
		result.Columns = append(result.Columns, mkColumn(names[idx]))
		result.Values = append(result.Values, value)
	}
	return result, nil
}

func (q Query) AggregateWith(names []string, aggregatorFactories []AggregatorFactory) Query {
	return Query{
		Iterate: func() Iterator {
			next := q.Iterate()
			done := false
			var aggregators = make([]Aggregator, len(aggregatorFactories))
			for idx := range aggregators {
				aggregators[idx] = aggregatorFactories[idx].Create()
			}

			return func(ctx Context) (Record, error) {
				if !done {
					for {
						item, err := next(ctx)
						if err != nil {
							if !IsNoRows(err) {
								return Record{}, err
							}
							break
						}
						for idx := range aggregators {
							err := aggregators[idx].Agg(ctx, item)
							if err != nil {
								return Record{}, err
							}
						}
					}
					done = true

					var result Record
					for idx := range aggregators {
						value, err := aggregators[idx].Result(ctx)
						if err != nil {
							return result, err
						}
						result.Columns = append(result.Columns, mkColumn(names[idx]))
						result.Values = append(result.Values, value)
					}
					return result, nil
				}

				return Record{}, ErrNoRows
			}
		},
	}
}
