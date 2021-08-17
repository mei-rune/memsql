package memcore

import (
	"reflect"
	"testing"
)

func TestAll(t *testing.T) {
	input := []int64{2, 4, 6, 8}

	r1 := fromInts(input...).All(func(i Record) bool {
		return i.Values[0].Int64%2 == 0
	})
	r2 := fromInts(input...).All(func(i Record) bool {
		return i.Values[0].Int64%2 != 0
	})

	if !r1 {
		t.Errorf("From(%v).All()=%v", input, r1)
	}

	if r2 {
		t.Errorf("From(%v).All()=%v", input, r2)
	}
}

func TestAny(t *testing.T) {
	tests := []struct {
		input []int64
		want  bool
	}{
		{[]int64{1, 2, 2, 3, 1}, true},
		{[]int64{}, false},
	}

	for _, test := range tests {
		if r := fromInts(test.input...).Any(); r != test.want {
			t.Errorf("From(%v).Any()=%v expected %v", test.input, r, test.want)
		}
	}
}

func TestAnyWith(t *testing.T) {
	tests := []struct {
		input []int64
		want  bool
	}{
		{[]int64{1, 2, 2, 3, 1}, false},
		{[]int64{}, false},
	}

	for _, test := range tests {
		if r := fromInts(test.input...).AnyWith(func(i Record) bool {
			return i.Values[0].Int64 == 4
		}); r != test.want {
			t.Errorf("From(%v).Any()=%v expected %v", test.input, r, test.want)
		}
	}
}

// func TestAverage(t *testing.T) {
// 	tests := []struct {
// 		input interface{}
// 		want  float64
// 	}{
// 		{[]int{1, 2, 2, 3, 1}, 1.8},
// 		{[5]uint{1, 2, 5, 7, 10}, 5.},
// 		{[]float32{1., 1.}, 1.},
// 	}

// 	for _, test := range tests {
// 		if r := From(test.input).Average(); r != test.want {
// 			t.Errorf("From(%v).Average()=%v expected %v", test.input, r, test.want)
// 		}
// 	}
// }

// func TestAverageForNaN(t *testing.T) {
// 	if r := From([]int{}).Average(); !math.IsNaN(r) {
// 		t.Errorf("From([]int{}).Average()=%v expected %v", r, math.NaN())
// 	}
// }

// func TestContains(t *testing.T) {
// 	tests := []struct {
// 		input interface{}
// 		value interface{}
// 		want  bool
// 	}{
// 		{[]int{1, 2, 2, 3, 1}, 10, false},
// 		{[5]uint{1, 2, 5, 7, 10}, uint(5), true},
// 		{[]float32{}, 1., false},
// 	}

// 	for _, test := range tests {
// 		if r := From(test.input).Contains(test.value); r != test.want {
// 			t.Errorf("From(%v).Contains(%v)=%v expected %v", test.input, test.value, r, test.want)
// 		}
// 	}
// }

func TestCount(t *testing.T) {
	tests := []struct {
		input []int64
		want  int
	}{
		{[]int64{1, 2, 2, 3, 1}, 5},
		{[]int64{1, 2, 5, 7, 10, 12, 15}, 7},
		{[]int64{}, 0},
	}

	for _, test := range tests {
		if r := fromInts(test.input...).Count(); r != test.want {
			t.Errorf("From(%v).Count()=%v expected %v", test.input, r, test.want)
		}
	}
}

func TestCountWith(t *testing.T) {
	tests := []struct {
		input []int64
		want  int
	}{
		{[]int64{1, 2, 2, 3, 1}, 4},
		{[]int64{}, 0},
	}

	for _, test := range tests {
		if r := fromInts(test.input...).CountWith(func(i Record) bool {
			return i.Values[0].Int64 <= 2
		}); r != test.want {
			t.Errorf("From(%v).CountWith()=%v expected %v", test.input, r, test.want)
		}
	}
}

func TestFirst(t *testing.T) {
	tests := []struct {
		input       []int64
		resultValid bool
		want        int64
	}{
		{[]int64{1, 2, 2, 3, 1}, true, 1},
		{[]int64{}, false, 0},
	}

	for _, test := range tests {
		r, ok := fromInts(test.input...).First()
		if !ok {
			if test.resultValid {
				t.Errorf("From(%v).First()=%v expected %v", test.input, r, test.want)
			}
			continue
		}
		if r.Values[0].Int64 != test.want {
			t.Errorf("From(%v).First()=%v expected %v", test.input, r, test.want)
		}
	}
}

func TestFirstWith(t *testing.T) {
	tests := []struct {
		input       []int64
		resultValid bool
		want        int64
	}{
		{[]int64{1, 2, 2, 3, 1}, true, 3},
		{[]int64{}, false, 0},
	}

	for _, test := range tests {
		r, ok := fromInts(test.input...).FirstWith(func(i Record) bool {
			return i.Values[0].Int64 > 2
		})
		if !ok {
			if test.resultValid {
				t.Errorf("From(%v).First()=%v expected %v", test.input, r, test.want)
			}
			continue
		}
		if r.Values[0].Int64 != test.want {
			t.Errorf("From(%v).FirstWith()=%v expected %v", test.input, r, test.want)
		}
	}
}

func TestForEachIndexed(t *testing.T) {
	tests := []struct {
		input []int64
		want  []int64
	}{
		{[]int64{1, 2, 2, 35, 111}, []int64{1, 3, 4, 38, 115}},
		{[]int64{}, []int64{}},
	}

	for _, test := range tests {
		output := []int64{}
		fromInts(test.input...).ForEach(func(index int, item Record) {
			output = append(output, item.Values[0].Int64+int64(index))
		})

		if !reflect.DeepEqual(output, test.want) {
			t.Fatalf("From(%#v).ForEachIndexed()=%#v expected=%#v", test.input, output, test.want)
		}
	}
}

func TestLast(t *testing.T) {
	tests := []struct {
		input       []int64
		resultValid bool
		want        int64
	}{
		{[]int64{1, 2, 2, 3, 1}, true, 1},
		{[]int64{}, false, 0},
	}

	for _, test := range tests {
		r, ok := fromInts(test.input...).Last()
		if !ok {
			if test.resultValid {
				t.Errorf("From(%v).First()=%v expected %v", test.input, r, test.want)
			}
			continue
		}
		if r.Values[0].Int64 != test.want {
			t.Errorf("From(%v).Last()=%v expected %v", test.input, r, test.want)
		}
	}
}

func TestLastWith(t *testing.T) {
	tests := []struct {
		input       []int64
		resultValid bool
		want        int64
	}{
		{[]int64{1, 2, 2, 3, 1, 4, 2, 5, 1, 1}, true, 5},
		{[]int64{}, false, 0},
	}

	for _, test := range tests {
		r, ok := fromInts(test.input...).LastWith(func(i Record) bool {
			return i.Values[0].Int64 > 2
		})
		if !ok {
			if test.resultValid {
				t.Errorf("From(%v).First()=%v expected %v", test.input, r, test.want)
			}
			continue
		}
		if r.Values[0].Int64 != test.want {
			t.Errorf("From(%v).Last()=%v expected %v", test.input, r, test.want)
		}
	}
}

// func TestMax(t *testing.T) {
// 	tests := []struct {
// 		input interface{}
// 		want  interface{}
// 	}{
// 		{[]int{1, 2, 2, 3, 1}, 3},
// 		{[]int{1}, 1},
// 		{[]int{}, nil},
// 	}

// 	for _, test := range tests {
// 		if r := From(test.input).Max(); r != test.want {
// 			t.Errorf("From(%v).Max()=%v expected %v", test.input, r, test.want)
// 		}
// 	}
// }

// func TestMin(t *testing.T) {
// 	tests := []struct {
// 		input interface{}
// 		want  interface{}
// 	}{
// 		{[]int{1, 2, 2, 3, 0}, 0},
// 		{[]int{1}, 1},
// 		{[]int{}, nil},
// 	}

// 	for _, test := range tests {
// 		if r := From(test.input).Min(); r != test.want {
// 			t.Errorf("From(%v).Min()=%v expected %v", test.input, r, test.want)
// 		}
// 	}
// }

func TestResults(t *testing.T) {
	input := []int64{1, 2, 3}
	want := makeRecords(1, 2, 3)

	if r := fromInts(input...).Results(); !reflect.DeepEqual(r, want) {
		t.Errorf("From(%v).Raw()=%v expected %v", input, r, want)
	}
}

func TestSequenceEqual(t *testing.T) {
	tests := []struct {
		input  []int64
		input2 []int64
		want   bool
	}{
		{[]int64{1, 2, 2, 3, 1}, []int64{4, 6}, false},
		{[]int64{1, -1, 100}, []int64{1, -1, 100}, true},
		{[]int64{}, []int64{}, true},
	}

	for _, test := range tests {
		if r := fromInts(test.input...).SequenceEqual(fromInts(test.input2...)); r != test.want {
			t.Errorf("From(%v).SequenceEqual(%v)=%v expected %v", test.input, test.input2, r, test.want)
		}
	}
}

func TestSingle(t *testing.T) {
	tests := []struct {
		input       []int64
		resultValid bool
		want        int64
	}{
		{[]int64{1, 2, 2, 3, 1}, false, 0},
		{[]int64{1}, true, 1},
		{[]int64{}, false, 0},
	}

	for _, test := range tests {
		r, ok := fromInts(test.input...).Single()
		if !ok {
			if test.resultValid {
				t.Errorf("From(%v).First()=%v expected %v", test.input, r, test.want)
			}
			continue
		}

		if r.Values[0].Int64 != test.want {
			t.Errorf("From(%v).Single()=%v expected %v", test.input, r, test.want)
		}
	}
}

func TestSingleWith(t *testing.T) {
	tests := []struct {
		input       []int64
		resultValid bool
		want        int64
	}{
		{[]int64{1, 2, 2, 3, 1}, true, 3},
		{[]int64{1, 1, 1}, false, 0},
		{[]int64{5, 1, 1, 10, 2, 2}, false, 0},
		{[]int64{}, false, 0},
	}

	for _, test := range tests {
		r, ok := fromInts(test.input...).SingleWith(func(i Record) bool {
			return i.Values[0].Int64 > 2
		})
		if !ok {
			if test.resultValid {
				t.Errorf("From(%v).First()=%v expected %v", test.input, r, test.want)
			}
			continue
		}
		if r.Values[0].Int64 != test.want {
			t.Errorf("From(%v).SingleWith()=%v expected %v", test.input, r, test.want)
		}
	}
}

// func TestSumInts(t *testing.T) {
// 	tests := []struct {
// 		input interface{}
// 		want  int64
// 	}{
// 		{[]int{1, 2, 2, 3, 1}, 9},
// 		{[]int{1}, 1},
// 		{[]int{}, 0},
// 	}

// 	for _, test := range tests {
// 		if r := From(test.input).SumInts(); r != test.want {
// 			t.Errorf("From(%v).SumInts()=%v expected %v", test.input, r, test.want)
// 		}
// 	}
// }

// func TestSumUInts(t *testing.T) {
// 	tests := []struct {
// 		input interface{}
// 		want  uint64
// 	}{
// 		{[]uint{1, 2, 2, 3, 1}, 9},
// 		{[]uint{1}, 1},
// 		{[]uint{}, 0},
// 	}

// 	for _, test := range tests {
// 		if r := From(test.input).SumUInts(); r != test.want {
// 			t.Errorf("From(%v).SumInts()=%v expected %v", test.input, r, test.want)
// 		}
// 	}
// }

// func TestSumFloats(t *testing.T) {
// 	tests := []struct {
// 		input interface{}
// 		want  float64
// 	}{
// 		{[]float32{1., 2., 2., 3., 1.}, 9.},
// 		{[]float64{1.}, 1.},
// 		{[]float32{}, 0.},
// 	}

// 	for _, test := range tests {
// 		if r := From(test.input).SumFloats(); r != test.want {
// 			t.Errorf("From(%v).SumFloats()=%v expected %v", test.input, r, test.want)
// 		}
// 	}
// }

func TestToChannel(t *testing.T) {
	c := make(chan Record)
	input := []int64{1, 2, 3, 4, 5}

	go func() {
		fromInts(input...).ToChannel(c)
	}()

	result := []int64{}
	for value := range c {
		result = append(result, value.Values[0].Int64)
	}

	if !reflect.DeepEqual(result, input) {
		t.Errorf("From(%v).ToChannel()=%v expected %v", input, result, input)
	}
}

// func TestToMap(t *testing.T) {
// 	input := make(map[int]bool)
// 	input[1] = true
// 	input[2] = false
// 	input[3] = true

// 	result := make(map[int]bool)
// 	From(input).ToMap(&result)

// 	if !reflect.DeepEqual(result, input) {
// 		t.Errorf("From(%v).ToMap()=%v expected %v", input, result, input)
// 	}
// }

// func TestToMapBy(t *testing.T) {
// 	input := make(map[int]bool)
// 	input[1] = true
// 	input[2] = false
// 	input[3] = true

// 	result := make(map[int]bool)
// 	From(input).ToMapBy(&result,
// 		func(i interface{}) interface{} {
// 			return i.(KeyValue).Key
// 		},
// 		func(i interface{}) interface{} {
// 			return i.(KeyValue).Value
// 		})

// 	if !reflect.DeepEqual(result, input) {
// 		t.Errorf("From(%v).ToMapBy()=%v expected %v", input, result, input)
// 	}
// }

// func TestToMapByT_PanicWhenKeySelectorFnIsInvalid(t *testing.T) {
// 	mustPanicWithError(t, "ToMapByT: parameter [keySelectorFn] has a invalid function signature. Expected: 'func(T)T', actual: 'func(int,int)int'", func() {
// 		result := make(map[int]bool)
// 		From([]int{1, 1, 1, 2, 1, 2, 3, 4, 2}).ToMapByT(
// 			&result,
// 			func(item, j int) int { return item + 2 },
// 			func(item int) int { return item + 2 },
// 		)
// 	})
// }

// func TestToMapByT_PanicWhenValueSelectorFnIsInvalid(t *testing.T) {
// 	mustPanicWithError(t, "ToMapByT: parameter [valueSelectorFn] has a invalid function signature. Expected: 'func(T)T', actual: 'func(int,int)int'", func() {
// 		result := make(map[int]bool)
// 		From([]int{1, 1, 1, 2, 1, 2, 3, 4, 2}).ToMapByT(
// 			&result,
// 			func(item int) int { return item + 2 },
// 			func(item, j int) int { return item + 2 },
// 		)
// 	})
// }

// func TestToSlice(t *testing.T) {
// 	tests := []struct {
// 		input             []int
// 		output            []int
// 		want              []int
// 		wantedOutputCap   int
// 		outputIsANewSlice bool
// 	}{
// 		// output is nil slice
// 		{
// 			[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
// 			nil,
// 			[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
// 			16,
// 			true},
// 		// output is empty slice (cap=0)
// 		{
// 			[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
// 			[]int{},
// 			[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
// 			16,
// 			true},
// 		// ToSlice() overwrites existing elements and reslices.
// 		{[]int{1, 2, 3},
// 			[]int{99, 98, 97, 96, 95},
// 			[]int{1, 2, 3},
// 			5,
// 			false},
// 		// cap(out)>len(result): we get the same slice, resliced. cap unchanged.
// 		{[]int{1, 2, 3, 4, 5},
// 			make([]int, 0, 11),
// 			[]int{1, 2, 3, 4, 5},
// 			11,
// 			false},
// 		// cap(out)==len(result): we get the same slice, cap unchanged.
// 		{[]int{1, 2, 3, 4, 5},
// 			make([]int, 0, 5),
// 			[]int{1, 2, 3, 4, 5},
// 			5,
// 			false},
// 		// cap(out)<len(result): we get a new slice with len(out)=len(result) and cap doubled: cap(out')==2*cap(out)
// 		{[]int{1, 2, 3, 4, 5},
// 			make([]int, 0, 4),
// 			[]int{1, 2, 3, 4, 5},
// 			8,
// 			true},
// 		// cap(out)<<len(result): trigger capacity to double more than once (26 -> 52 -> 104)
// 		{make([]int, 100),
// 			make([]int, 0, 26),
// 			make([]int, 100),
// 			104,
// 			true},
// 		// len(out) > len(result): we get the same slice with len(out)=len(result) and cap unchanged: cap(out')==cap(out)
// 		{[]int{1, 2, 3, 4, 5},
// 			make([]int, 0, 50),
// 			[]int{1, 2, 3, 4, 5},
// 			50,
// 			false},
// 	}

// 	for c, test := range tests {
// 		initialOutputValue := test.output
// 		From(test.input).ToSlice(&test.output)
// 		modifiedOutputValue := test.output

// 		// test slice values
// 		if !reflect.DeepEqual(test.output, test.want) {
// 			t.Fatalf("case #%d: From(%#v).ToSlice()=%#v expected=%#v", c, test.input, test.output, test.want)
// 		}

// 		// test capacity of output slice
// 		if cap(test.output) != test.wantedOutputCap {
// 			t.Fatalf("case #%d: cap(output)=%d expected=%d", c, cap(test.output), test.wantedOutputCap)
// 		}

// 		// test if a new slice is allocated
// 		inPtr := (*reflect.SliceHeader)(unsafe.Pointer(&initialOutputValue)).Data
// 		outPtr := (*reflect.SliceHeader)(unsafe.Pointer(&modifiedOutputValue)).Data
// 		isNewSlice := inPtr != outPtr
// 		if isNewSlice != test.outputIsANewSlice {
// 			t.Fatalf("case #%d: isNewSlice=%v (in=0x%X out=0x%X) expected=%v", c, isNewSlice, inPtr, outPtr, test.outputIsANewSlice)
// 		}
// 	}
// }
