package vm

func UminusFunc(read func(Context) (Value, error)) func(Context) (Value, error) {
  return func(ctx Context) (Value, error) {
    value, err := read(ctx)
    if err != nil {
      return Null(), err
    }

    switch value.Type {
    // case ValueNull:
    //   return Null(), NewArithmeticError("-", value.Type.String(), "")
    // case ValueBool:
    //   return Null(), NewArithmeticError("-", value.Type.String(), "")
    // case ValueString:
    //   return Null(), NewArithmeticError("-", value.Type.String(), "")
    case ValueInt64:
      return IntToValue(-value.Int64), nil
    case ValueUint64:
      return IntToValue(-int64(value.Uint64)), nil
    case ValueFloat64:
      return FloatToValue(-value.Float64), nil
    case ValueInterval:
      return IntToValue(-value.Int64), nil
    default:
      return Null(), NewArithmeticError("-", value.Type.String(), "")
    }
  }
}
