package vm


type Aggregator interface {
	Agg(Value) error

	Result() (Value, error)
}

var AggFuncs = map[string]func() Aggregator{
	"count": func() Aggregator {
		return &countAgg{}
	},
	"sum": func() Aggregator {
		return &sumAgg{
			sum: IntToValue(0),
		}
	},
	"avg": func() Aggregator {
		return &avgAgg{
			sum: IntToValue(0),
		}
	},
}

type countAgg struct {
	count int64
}

func (c *countAgg) Agg(value Value) error {
	if value.IsNull() {
		return nil
	}
	c.count ++ 
	return nil
}

func (c *countAgg) Result() (Value, error) {
	return IntToValue(c.count), nil
}

type sumAgg struct {
	sum Value
}

func (c *sumAgg) Agg(value Value) (err error) {
	if value.IsNull() {
		return nil
	}
	c.sum, err = Plus(c.sum, value)
	return err
}

func (c *sumAgg) Result() (Value, error) {
	return c.sum, nil
}

type avgAgg struct {
	name string
	sum Value
	count int64
}

func (c *avgAgg) Agg(value Value) (err error) {
	if value.IsNull() {
		return nil
	}
	c.sum, err = Plus(c.sum, value)
	if err != nil {
		return err
	}
	c.count ++
	return nil
}

func (c *avgAgg) Result() (Value, error) {
	return divInt(c.sum, c.count)
}