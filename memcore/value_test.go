package memcore

import "testing"

func TestGetComparer(t *testing.T) {
	tests := []struct {
		x    Value
		y    Value
		opt  CompareOption
		want int
		fail bool
	}{
		{x: MustToValue(100), y: MustToValue(500), want: -1},
		{x: MustToValue(-100), y: MustToValue(-500), want: 1},
		{x: MustToValue(256), y: MustToValue(256), want: 0},
		{x: MustToValue(int8(100)), y: MustToValue(int8(-100)), want: 1},
		{x: MustToValue(int8(-100)), y: MustToValue(int8(100)), want: -1},
		{x: MustToValue(int8(100)), y: MustToValue(int8(100)), want: 0},
		{x: MustToValue(int16(100)), y: MustToValue(int16(-100)), want: 1},
		{x: MustToValue(int16(-100)), y: MustToValue(int16(100)), want: -1},
		{x: MustToValue(int16(100)), y: MustToValue(int16(100)), want: 0},
		{x: MustToValue(int32(100)), y: MustToValue(int32(-100)), want: 1},
		{x: MustToValue(int32(-100)), y: MustToValue(int32(100)), want: -1},
		{x: MustToValue(int32(100)), y: MustToValue(int32(100)), want: 0},
		{x: MustToValue(int64(100)), y: MustToValue(int64(-100)), want: 1},
		{x: MustToValue(int64(-100)), y: MustToValue(int64(100)), want: -1},
		{x: MustToValue(int64(100)), y: MustToValue(int64(100)), want: 0},
		{x: MustToValue(uint(100)), y: MustToValue(uint(0)), want: 1},
		{x: MustToValue(uint(0)), y: MustToValue(uint(100)), want: -1},
		{x: MustToValue(uint(100)), y: MustToValue(uint(100)), want: 0},
		{x: MustToValue(uint8(100)), y: MustToValue(uint8(0)), want: 1},
		{x: MustToValue(uint8(0)), y: MustToValue(uint8(100)), want: -1},
		{x: MustToValue(uint8(100)), y: MustToValue(uint8(100)), want: 0},
		{x: MustToValue(uint16(100)), y: MustToValue(uint16(0)), want: 1},
		{x: MustToValue(uint16(0)), y: MustToValue(uint16(100)), want: -1},
		{x: MustToValue(uint16(100)), y: MustToValue(uint16(100)), want: 0},
		{x: MustToValue(uint32(100)), y: MustToValue(uint32(0)), want: 1},
		{x: MustToValue(uint32(0)), y: MustToValue(uint32(100)), want: -1},
		{x: MustToValue(uint32(100)), y: MustToValue(uint32(100)), want: 0},
		{x: MustToValue(uint64(100)), y: MustToValue(uint64(0)), want: 1},
		{x: MustToValue(uint64(0)), y: MustToValue(uint64(100)), want: -1},
		{x: MustToValue(uint64(100)), y: MustToValue(uint64(100)), want: 0},
		{x: MustToValue(float32(5.)), y: MustToValue(float32(1.)), want: 1},
		{x: MustToValue(float32(1.)), y: MustToValue(float32(5.)), want: -1},
		// {x: MustToValue(float32(0)), y: MustToValue(float32(0)), want: 0},
		{x: MustToValue(float64(5.)), y: MustToValue(float64(1.)), want: 1},
		{x: MustToValue(float64(1.)), y: MustToValue(float64(5.)), want: -1},
		// {x: MustToValue(float64(0)), y: MustToValue(float64(0)), want: 0},
		{x: MustToValue(true), y: MustToValue(true), want: 0},
		{x: MustToValue(false), y: MustToValue(false), want: 0},
		{x: MustToValue(true), y: MustToValue(false), want: 1},
		{x: MustToValue(false), y: MustToValue(true), want: -1},
		{x: MustToValue("foo"), y: MustToValue("foo"), want: 0},
		{x: MustToValue("foo"), y: MustToValue("bar"), want: 1},
		{x: MustToValue("bar"), y: MustToValue("foo"), want: -1},
		{x: MustToValue("FOO"), y: MustToValue("bar"), want: -1},

		{x: MustToValue(100), y: MustToValue(uint64(500)), want: -1},
		{x: MustToValue(600), y: MustToValue(uint64(500)), want: 1},
		{x: MustToValue(256), y: MustToValue(uint64(256)), want: 0},
		{x: MustToValue(uint64(100)), y: MustToValue(500), want: -1},
		{x: MustToValue(uint64(100)), y: MustToValue(-500), want: 1},
		{x: MustToValue(uint64(256)), y: MustToValue(256), want: 0},

		{x: MustToValue(100), y: MustToValue("500"), want: -1, fail: true},

		{x: MustToValue(100), y: MustToValue("500"), want: -1, opt: CompareOption{Weak: true}},
		{x: MustToValue(-100), y: MustToValue("-500"), want: 1, opt: CompareOption{Weak: true}},
		{x: MustToValue(256), y: MustToValue("256"), want: 0, opt: CompareOption{Weak: true}},
		{x: MustToValue("100"), y: MustToValue(500), want: -1, opt: CompareOption{Weak: true}},
		{x: MustToValue("-100"), y: MustToValue(-500), want: 1, opt: CompareOption{Weak: true}},
		{x: MustToValue("256"), y: MustToValue(256), want: 0, opt: CompareOption{Weak: true}},
		{x: MustToValue("256.0"), y: MustToValue(256), want: 0, opt: CompareOption{Weak: true}},

		{x: MustToValue(100.0), y: MustToValue("500"), want: -1, opt: CompareOption{Weak: true}},
		{x: MustToValue(-100.0), y: MustToValue("-500"), want: 1, opt: CompareOption{Weak: true}},
		// {x: MustToValue(256.0), y: MustToValue("256"), want: 0, opt: CompareOption{Weak:true}},
		{x: MustToValue("100.1"), y: MustToValue(500), want: -1, opt: CompareOption{Weak: true}},
		{x: MustToValue("-100.1"), y: MustToValue(-500), want: 1, opt: CompareOption{Weak: true}},
		//{x: MustToValue("256.1"), y: MustToValue(256), want: 0, opt: CompareOption{Weak:true}},

		{x: MustToValue(100.0), y: MustToValue("500.1"), want: -1, opt: CompareOption{Weak: true}},
		{x: MustToValue(-100.0), y: MustToValue("-500.1"), want: 1, opt: CompareOption{Weak: true}},
		// {x: MustToValue(256.0), y: MustToValue("256.1"), want: 0, opt: CompareOption{Weak:true}},
		{x: MustToValue("100.1"), y: MustToValue(500.0), want: -1, opt: CompareOption{Weak: true}},
		{x: MustToValue("-100.1"), y: MustToValue(-500.0), want: 1, opt: CompareOption{Weak: true}},
		// {x: MustToValue("256.1"), y: MustToValue(256.0), want: 0, opt: CompareOption{Weak:true}},

		{x: MustToValue("100"), y: MustToValue("500"), want: -1, opt: CompareOption{Weak: true}},
		//{x: MustToValue("90"), y: MustToValue("500"), want: -1, opt: CompareOption{Weak:true}},
		//{x: MustToValue("500"), y: MustToValue("90"), want: 1, opt: CompareOption{Weak:true}},
		//{x: MustToValue("-100"), y: MustToValue("-500"), want: 1, opt: CompareOption{Weak:true}},
		{x: MustToValue("256"), y: MustToValue("256"), want: 0, opt: CompareOption{Weak: true}},
		//{x: MustToValue("256.0"), y: MustToValue("256"), want: 0, opt: CompareOption{Weak:true}},

	}

	for _, test := range tests {
		r, err := test.x.CompareTo(test.y, test.opt)
		if err != nil {
			if !test.fail {
				t.Errorf("(%v)(%v,%v)=%v expected %v, fail", test.x, test.x, test.y, r, test.want)
			}
			continue
		}
		if r != test.want {
			t.Errorf("(%v)(%v,%v)=%v expected %v", test.x, test.x, test.y, r, test.want)
		}
	}
}
