package vm

import (
	"strconv"
	"strings"
	"time"
)

func (r *Value) EqualTo(to Value, opt CompareOption) (bool, error) {
	switch to.Type {
	case ValueNull:
		return r.IsNil(), nil
	case ValueBool:
		return r.EqualToBool(to.Bool, opt)
	case ValueString:
		return r.EqualToString(to.Str, opt)
	case ValueInt64:
		return r.EqualToInt64(to.Int64, opt)
	case ValueUint64:
		return r.EqualToUint64(to.Uint64, opt)
	case ValueFloat64:
		return r.EqualToFloat64(to.Float64, opt)
	case ValueDatetime:
		return r.EqualToDatetime(to.Int64, opt)
	case ValueInterval:
		return r.EqualToInterval(time.Duration(to.Int64), opt)
	default:
		return false, NewTypeMismatch(r.Type.String(), "unknown")
	}
}

func (r *Value) EqualToBool(to bool, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, nil
	case ValueBool:
		return r.Bool == to, nil
	case ValueString:
		if opt.Weak {
			switch r.Str {
			case "1", "t", "T", "true", "TRUE", "True":
				return to == true, nil
			case "0", "f", "F", "false", "FALSE", "False":
				return to == false, nil
			}
		}
	case ValueInt64:
		if opt.Weak {
			return (r.Int64 != 0) == to, nil
		}
	case ValueUint64:
		if opt.Weak {
			return (r.Uint64 != 0) == to, nil
		}
	}

	return false, NewTypeMismatch(r.Type.String(), "boolean")
}

func (r *Value) EqualToString(to string, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueBool:
		if opt.Weak {
			switch r.Str {
			case "1", "t", "T", "true", "TRUE", "True":
				return r.Bool == true, nil
			case "0", "f", "F", "false", "FALSE", "False":
				return r.Bool == false, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueString:
		if opt.IgnoreCase {
			return strings.EqualFold(r.Str, to), nil
		}
		return r.Str == to, nil
	case ValueInt64:
		if opt.Weak {
			i64, err := strconv.ParseInt(to, 10, 64)
			if err == nil {
				return r.Int64 == i64, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueUint64:
		if opt.Weak {
			u64, err := strconv.ParseUint(to, 10, 64)
			if err == nil {
				return r.Uint64 == u64, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "string")
	case ValueFloat64:
		return false, NewTypeMismatch(r.Type.String(), "string")
	default:
		return false, NewTypeMismatch(r.Type.String(), "string")
	}
}

func (r *Value) EqualToInt64(to int64, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, NewTypeMismatch(r.Type.String(), "int")
	case ValueBool:
		if opt.Weak {
			return (to != 0) == r.Bool, nil
		}
		return false, NewTypeMismatch(r.Type.String(), "int")
	case ValueString:
		if opt.Weak {
			i64, err := strconv.ParseInt(r.Str, 10, 64)
			if err == nil {
				return i64 == to, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "int")
	case ValueInt64:
		return r.Int64 == to, nil
	case ValueUint64:
		if to < 0 {
			return false, nil
		}
		return r.Uint64 == uint64(to), nil
	case ValueFloat64:
		return false, NewTypeMismatch(r.Type.String(), "int")
	default:
		return false, NewTypeMismatch(r.Type.String(), "int")
	}
}

func (r *Value) EqualToUint64(to uint64, opt CompareOption) (bool, error) {
	switch r.Type {
	case ValueNull:
		return false, NewTypeMismatch(r.Type.String(), "uint")
	case ValueBool:
		if opt.Weak {
			return (to != 0) == r.Bool, nil
		}
		return false, NewTypeMismatch(r.Type.String(), "uint")
	case ValueString:
		if opt.Weak {
			u64, err := strconv.ParseUint(r.Str, 10, 64)
			if err == nil {
				return u64 == to, nil
			}
		}
		return false, NewTypeMismatch(r.Type.String(), "uint")
	case ValueInt64:
		if r.Int64 < 0 {
			return false, nil
		}
		return uint64(r.Int64) == to, nil
	case ValueUint64:
		return r.Uint64 == to, nil
	case ValueFloat64:
		return false, NewTypeMismatch(r.Type.String(), "uint")
	default:
		return false, NewTypeMismatch(r.Type.String(), "uint")
	}
}

func (r *Value) EqualToFloat64(to float64, opt CompareOption) (bool, error) {
	return false, NewTypeMismatch(r.Type.String(), "float")
}

func (r *Value) EqualToDatetime(to int64, opt CompareOption) (bool, error) {
	if r.Type == ValueDatetime {
		return r.Int64 == to, nil
	}
	return false, NewTypeMismatch(r.Type.String(), "datetime")
}

func (r *Value) EqualToInterval(to time.Duration, opt CompareOption) (bool, error) {
	if r.Type == ValueInterval {
		return time.Duration(r.Int64) == to, nil
	}
	return false, NewTypeMismatch(r.Type.String(), "interval")
}
