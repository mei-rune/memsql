package memsql


// Iterator is an alias for function to iterate over data.
type Iterator func() (item Record, ok bool)

// Query is the type returned from query functions. It can be iterated manually
// as shown in the example.
type Query struct {
	Iterate func() Iterator
}

// Iterable is an interface that has to be implemented by a custom collection in
// order to work with linq.
type Iterable interface {
	Iterate() Iterator
}

// From initializes a linq query with passed slice, array or map as the source.
// String, channel or struct implementing Iterable interface can be used as an
// input. In this case From delegates it to FromString, FromChannel and
// FromIterable internally.
func From(source Table) Query {
	return Query{
		Iterate: func() Iterator {
			index := 0

			return func() (item Record, ok bool) {
				ok = index < source.Length()
				if ok {
					item = source.At(index)
					index++
				}
				return
			}
		},
	}
}


// FromChannel initializes a linq query with passed channel, linq iterates over
// channel until it is closed.
func FromChannel(source <-chan Record) Query {
	return Query{
		Iterate: func() Iterator {
			return func() (item Record, ok bool) {
				item, ok = <-source
				return
			}
		},
	}
}

// From initializes a linq query with passed slice, array or map as the source.
// String, channel or struct implementing Iterable interface can be used as an
// input. In this case From delegates it to FromString, FromChannel and
// FromIterable internally.
func FromRecords(source []Record) Query {
	return Query{
		Iterate: func() Iterator {
			index := 0

			return func() (item Record, ok bool) {
				ok = index < len(source)
				if ok {
					item = source[index]
					index++
				}
				return
			}
		},
	}
}


// FromIterable initializes a linq query with custom collection passed. This
// collection has to implement Iterable interface, linq iterates over items,
// that has to implement Comparable interface or be basic types.
func FromIterable(source Iterable) Query {
	return Query{
		Iterate: source.Iterate,
	}
}
