package vm

var  Funcs = map[string]func(ctx Context, values []Value) (Value, error) {}

func CallFunc(call func(Context, []Value) (Value, error), readValues func(Context) ([]Value, error)) func(ctx Context) (Value, error) {
	return func(ctx Context) (Value, error) {
		values, err := readValues(ctx)
		if err != nil {
			return Null(), err
		}
		return call(ctx, values)
	}
}