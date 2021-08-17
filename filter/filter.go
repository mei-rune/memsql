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

func LessThan(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      return leftValue.CompareTo(rightValue) < 0
    }
}

func GreaterThan(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      return leftValue.CompareTo(rightValue) > 0
    }
}

func LessEqual(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      return leftValue.CompareTo(rightValue) <= 0
    }
}

func GreaterEqual(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      return leftValue.CompareTo(rightValue) >= 0
    }
}

func NotEqual(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      return leftValue.CompareTo(rightValue) != 0
    }
}

func NotEqual(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      return leftValue.CompareTo(rightValue) != 0
    }
}

func In(left func(Context) (memsql.Value, error), right func(Context) ([]memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValues, err := right(ctx)
      if err != nil {
        return false, err
      }
      for _, value := range rightValues {
        if value.Equal(leftValue) {
          return true, nil
        }
      }
      return false, nil
    }
}

func NotIn(left func(Context) (memsql.Value, error), right func(Context) ([]memsql.Value, error)) func(Context) (bool, error) {
  return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValues, err := right(ctx)
      if err != nil {
        return false, err
      }
      for _, value := range rightValues {
        if value.Equal(leftValue) {
          return false, nil
        }
      }
      return true, nil
    }
}

func Like(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return   return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      leftStr, err := leftValue.ToString()
      if err != nil {
        return false, errors.Wrap(err, "left operant isnot string")
      }
      rightStr, err := rightValue.ToString()
      if err != nil {
        return false, errors.Wrap(err, "right operant isnot string")
      }
      if strings.HasPrefix(rightStr, "%") {
        if strings.HasSuffix(rightStr, "%") {
          s := strings.TrimPrefix(rightStr, "%")
          s = strings.TrimSuffix(s, "%")
          return strings.Contains(leftStr, s) 
        }
        return strings.HasSuffix(leftStr, strings.TrimPrefix("%"))
      }
      if strings.HasSuffix(rightStr, "%") {
        return strings.HasPrefix(leftStr, strings.TrimSuffix(rightStr, "%"))
      }
      return leftStr == rightStr, nil
  }
}



func Regexp(left, right func(Context) (memsql.Value, error)) func(Context) (bool, error) {
  return   return func(ctx Context) (bool, error) {
      leftValue, err := left(ctx)
      if err != nil {
        return false, err
      }
      rightValue, err := right(ctx)
      if err != nil {
        return false, err
      }
      leftStr, err := leftValue.ToString()
      if err != nil {
        return false, errors.Wrap(err, "left operant isnot string")
      }
      rightStr, err := rightValue.ToString()
      if err != nil {
        return false, errors.Wrap(err, "right operant isnot string")
      }

      
  }
}