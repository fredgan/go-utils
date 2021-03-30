package nosql

import (
	"errors"
	"fmt"
	"strconv"
)

var ErrNegativeInt = errors.New("redis cluster: unexpected value for Uint64")

func (r *Redis) Int(reply interface{}, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case int:
		return reply, nil
	case int8:
		return int(reply), nil
	case int16:
		return int(reply), nil
	case int32:
		return int(reply), nil
	case int64:
		x := int(reply)
		if int64(x) != reply {
			return 0, strconv.ErrRange
		}
		return x, nil
	case uint:
		n := int(reply)
		if n < 0 {
			return 0, strconv.ErrRange
		}
		return n, nil
	case uint8:
		return int(reply), nil
	case uint16:
		return int(reply), nil
	case uint32:
		n := int(reply)
		if n < 0 {
			return 0, strconv.ErrRange
		}
		return n, nil
	case uint64:
		n := int(reply)
		if n < 0 {
			return 0, strconv.ErrRange
		}
		return n, nil
	case []byte:
		data := string(reply)
		if len(data) == 0 {
			return 0, ErrNil
		}

		n, err := strconv.ParseInt(data, 10, 0)
		return int(n), err
	case string:
		if len(reply) == 0 {
			return 0, ErrNil
		}

		n, err := strconv.ParseInt(reply, 10, 0)
		return int(n), err
	case nil:
		return 0, ErrNil
	case error:
		return 0, reply
	}
	return 0, fmt.Errorf("redis cluster: unexpected type for Int, got type %T", reply)
}

func (r *Redis) Int64(reply interface{}, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case int:
		return int64(reply), nil
	case int8:
		return int64(reply), nil
	case int16:
		return int64(reply), nil
	case int32:
		return int64(reply), nil
	case int64:
		return reply, nil
	case uint:
		n := int64(reply)
		if n < 0 {
			return 0, strconv.ErrRange
		}
		return n, nil
	case uint8:
		return int64(reply), nil
	case uint16:
		return int64(reply), nil
	case uint32:
		return int64(reply), nil
	case uint64:
		n := int64(reply)
		if n < 0 {
			return 0, strconv.ErrRange
		}
		return n, nil
	case []byte:
		data := string(reply)
		if len(data) == 0 {
			return 0, ErrNil
		}

		n, err := strconv.ParseInt(data, 10, 64)
		return n, err
	case string:
		if len(reply) == 0 {
			return 0, ErrNil
		}

		n, err := strconv.ParseInt(reply, 10, 64)
		return n, err
	case nil:
		return 0, ErrNil
	case error:
		return 0, reply
	}
	return 0, fmt.Errorf("redis cluster: unexpected type for Int64, got type %T", reply)
}

func (r *Redis) Uint64(reply interface{}, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case uint:
		return uint64(reply), nil
	case uint8:
		return uint64(reply), nil
	case uint16:
		return uint64(reply), nil
	case uint32:
		return uint64(reply), nil
	case uint64:
		return reply, nil
	case int:
		if reply < 0 {
			return 0, ErrNegativeInt
		}
		return uint64(reply), nil
	case int8:
		if reply < 0 {
			return 0, ErrNegativeInt
		}
		return uint64(reply), nil
	case int16:
		if reply < 0 {
			return 0, ErrNegativeInt
		}
		return uint64(reply), nil
	case int32:
		if reply < 0 {
			return 0, ErrNegativeInt
		}
		return uint64(reply), nil
	case int64:
		if reply < 0 {
			return 0, ErrNegativeInt
		}
		return uint64(reply), nil
	case []byte:
		data := string(reply)
		if len(data) == 0 {
			return 0, ErrNil
		}

		n, err := strconv.ParseUint(data, 10, 64)
		return n, err
	case string:
		if len(reply) == 0 {
			return 0, ErrNil
		}

		n, err := strconv.ParseUint(reply, 10, 64)
		return n, err
	case nil:
		return 0, ErrNil
	case error:
		return 0, reply
	}
	return 0, fmt.Errorf("redis cluster: unexpected type for Uint64, got type %T", reply)
}

func (r *Redis) Float64(reply interface{}, err error) (float64, error) {
	if err != nil {
		return 0, err
	}

	var value float64
	err = nil
	switch v := reply.(type) {
	case float32:
		value = float64(v)
	case float64:
		value = v
	case int:
		value = float64(v)
	case int8:
		value = float64(v)
	case int16:
		value = float64(v)
	case int32:
		value = float64(v)
	case int64:
		value = float64(v)
	case uint:
		value = float64(v)
	case uint8:
		value = float64(v)
	case uint16:
		value = float64(v)
	case uint32:
		value = float64(v)
	case uint64:
		value = float64(v)
	case []byte:
		data := string(v)
		if len(data) == 0 {
			return 0, ErrNil
		}
		value, err = strconv.ParseFloat(string(v), 64)
	case string:
		if len(v) == 0 {
			return 0, ErrNil
		}
		value, err = strconv.ParseFloat(v, 64)
	case nil:
		err = ErrNil
	case error:
		err = v
	default:
		err = fmt.Errorf("redis cluster: unexpected type for Float64, got type %T", v)
	}

	return value, err
}

func (r *Redis) Bool(reply interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	switch reply := reply.(type) {
	case bool:
		return reply, nil
	case int64:
		return reply != 0, nil
	case []byte:
		data := string(reply)
		if len(data) == 0 {
			return false, ErrNil
		}

		return strconv.ParseBool(data)
	case string:
		if len(reply) == 0 {
			return false, ErrNil
		}

		return strconv.ParseBool(reply)
	case nil:
		return false, ErrNil
	case error:
		return false, reply
	}
	return false, fmt.Errorf("redis cluster: unexpected type for Bool, got type %T", reply)
}

func (r *Redis) Bytes(reply interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []byte:
		if len(reply) == 0 {
			return nil, ErrNil
		}
		return reply, nil
	case string:
		data := []byte(reply)
		if len(data) == 0 {
			return nil, ErrNil
		}
		return data, nil
	case nil:
		return nil, ErrNil
	case error:
		return nil, reply
	}
	return nil, fmt.Errorf("redis cluster: unexpected type for Bytes, got type %T", reply)
}

func (r *Redis) String(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}

	value := ""
	err = nil
	switch v := reply.(type) {
	case string:
		if len(v) == 0 {
			return "", ErrNil
		}

		value = v
	case []byte:
		if len(v) == 0 {
			return "", ErrNil
		}

		value = string(v)
	case int:
		value = strconv.FormatInt(int64(v), 10)
	case int8:
		value = strconv.FormatInt(int64(v), 10)
	case int16:
		value = strconv.FormatInt(int64(v), 10)
	case int32:
		value = strconv.FormatInt(int64(v), 10)
	case int64:
		value = strconv.FormatInt(v, 10)
	case uint:
		value = strconv.FormatUint(uint64(v), 10)
	case uint8:
		value = strconv.FormatUint(uint64(v), 10)
	case uint16:
		value = strconv.FormatUint(uint64(v), 10)
	case uint32:
		value = strconv.FormatUint(uint64(v), 10)
	case uint64:
		value = strconv.FormatUint(v, 10)
	case float32:
		value = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		value = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		value = strconv.FormatBool(v)
	case nil:
		err = ErrNil
	case error:
		err = v
	default:
		err = fmt.Errorf("redis cluster: unexpected type for String, got type %T", v)
	}

	return value, err
}

func (r *Redis) Strings(reply interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		result := make([]string, len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			switch subReply := reply[i].(type) {
			case string:
				result[i] = subReply
			case []byte:
				result[i] = string(subReply)
			default:
				return nil, fmt.Errorf("redis cluster: unexpected element type for String, got type %T", reply[i])
			}
		}
		return result, nil
	case []string:
		return reply, nil
	case nil:
		return nil, ErrNil
	case error:
		return nil, reply
	}
	return nil, fmt.Errorf("redis cluster: unexpected type for Strings, got type %T", reply)
}

func (r *Redis) Values(reply interface{}, err error) ([]interface{}, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		return reply, nil
	case nil:
		return nil, ErrNil
	case error:
		return nil, reply
	}
	return nil, fmt.Errorf("redis cluster: unexpected type for Values, got type %T", reply)
}
