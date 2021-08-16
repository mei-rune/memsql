package filter

type Context interface {
  GetValue(tableName, name string) (memsql.Value, error) 
}

func And(left, right func(Context) (bool, error))  func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
    ok, err := left(ctx)
    if err != nil {
      return false, err
    }
    if !ok {
      return false, nil
    }

    return right(ctx)
  }
}


func Or(left, right func(Context) (bool, error))  func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
    ok, err := left(ctx)
    if err != nil {
      return false, err
    }
    if ok {
      return true, nil
    }
    return right(ctx)
  }
}

func Not(f func(Context) (bool, error))  func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      ok, err := f(ctx)
      if err != nil {
        return false, err
      }
      return !ok, nil
    }
}

func Equal(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      return leftValue.Equal(rightValue)
    }
}