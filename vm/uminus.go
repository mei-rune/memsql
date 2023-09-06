package vm

func UminusFunc(read func(Context) (Value, error)) func(Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		value, err := read(ctx)
		if err != nil {
			return Null(), err
		}

		return Uminus(value)
	}
}
