package memsql

// // Group is a type that is used to store the result of GroupBy method.
// type Group struct {
// 	Column Column
// 	Key   Value
// 	Group []interface{}
// }

// // GroupBy method groups the elements of a collection according to a specified
// // key selector function and projects the elements for each group by using a
// // specified function.
// func (q Query) GroupBy(keySelector func(Record) (Column, Value),
// 	elementSelector func(Record) Record) Query {
// 	return Query{
// 		func() Iterator {
// 			next := q.Iterate()
// 			set := make(map[Value][]Record)

// 			for item, ok := next(); ok; item, ok = next() {
// 				key := keySelector(item)
// 				set[key] = append(set[key], elementSelector(item))
// 			}

// 			len := len(set)
// 			idx := 0
// 			groups := make([]Group, len)
// 			for k, v := range set {
// 				groups[idx] = Group{k, v}
// 				idx++
// 			}

// 			index := 0

// 			return func() (item Record, ok bool) {
// 				ok = index < len
// 				if ok {
// 					item = groups[index]
// 					index++
// 				}

// 				return
// 			}
// 		},
// 	}
// }
