package msi

import (
	"fmt"
	"time"
)

//interface converter : provides a list of helper functions; none of these shall be the core of msi

func ToString(i interface{}) (string, error) {
	ret, ok := i.(string)
	if !ok {
		return "", fmt.Errorf(`not a string, [%v]`, i)
	}
	return ret, nil
}
func ToInt(i interface{}) (int, error) {
	if ret, ok := i.(int); ok {
		return ret, nil
	}

	if ret, ok := i.(int64); ok {
		return int(ret), nil
	}
	if f, err := ToFloat(i); err == nil {
		return int(f), nil
	}
	return 0, fmt.Errorf(`not a int or int64 or float64`)
}

func ToFloat(i interface{}) (float64, error) {
	ret, ok := i.(float64)
	if !ok {
		return float64(0), fmt.Errorf(`not a float64 [%v]`, i)
	}
	return ret, nil
}

func ToTime(i interface{}) (*time.Time, error) {
	//	if ret, ok := i.(time.Time); ok {
	//		return &ret, nil
	//	}
	//	if ret, ok := i.(*time.Time); ok {
	//		return ret, nil
	//	}

	if i == nil {
		return nil, fmt.Errorf(`time is nil`)
	}
	switch v := i.(type) {
	case time.Time:
		return &v, nil
	case *time.Time:
		return v, nil
	case string:
		return ToTime(ParseByte(`time.Time`, []byte(v)))
		//	case []byte:
		//		return ToTime(ParseByte(`time.Time`, v))
	default:
		return nil, fmt.Errorf("I don't know about type %T!\n", v)
	}
	return nil, fmt.Errorf(`not a time.Time or *time.Time or nil`)
}

//ToDate 2017-01-01 format
func ToDateString(i interface{}) (string, error) {
	d, err := ToTime(i)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day()), nil

}

func GetKeyStr(m map[string]interface{}, key string) (string, error) {
	_s, ok := m[key]
	if !ok {
		return "", fmt.Errorf(`not found key [%s]`, key)
	}
	return ToString(_s)
}

func GetKeyInt(m map[string]interface{}, key string) (int, error) {
	_s, ok := m[key]
	if !ok {
		return 0, fmt.Errorf(`not found key [%s]`, key)
	}
	n, err := ToInt(_s)
	if err != nil {
		return 0, fmt.Errorf(`key[%s] to int err: %s `, key, err.Error())
	}
	return n, nil
}

func GetKeyFloat64(m map[string]interface{}, key string) (float64, error) {
	_s, ok := m[key]
	if !ok {
		return 0, fmt.Errorf(`not found key [%s]`, key)
	}
	return ToFloat(_s)
}

func GetKeyTime(m map[string]interface{}, key string) (*time.Time, error) {
	_s, ok := m[key]
	if !ok {
		return nil, fmt.Errorf(`not found key [%s]`, key)
	}
	return ToTime(_s)
}
