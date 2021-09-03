package vm


func ToSwitchWithValue(readValue func(Context) (Value, error), 
	condList []func(Context) (Value, error), 
	valueList []func(Context) (Value, error), 
	elseValue func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		value, err := readValue(ctx)
		if err != nil {
			return Null(), err
		}
		for idx := range condList {
			condValue, err := condList[idx](ctx)
			if err != nil {
				return Null(), err
			}
			ok, err := condValue.EqualTo(value, EmptyCompareOption())
			if err != nil {
				return Null(), err
			}
			if ok {
				return valueList[idx](ctx)
			}
		}
		return elseValue(ctx)
	}
}


func ToSwitch(condList []func(Context) (bool, error), 
	valueList []func(Context) (Value, error), 
	elseValue func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		for idx := range condList {
			ok, err := condList[idx](ctx)
			if err != nil {
				return Null(), err
			}
			if ok {
				return valueList[idx](ctx)
			}
		}
		return elseValue(ctx)
	}
}