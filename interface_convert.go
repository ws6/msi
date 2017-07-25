package msi

import (
	"fmt"
	"time"
)

//interface converter : provides a list of helper functions; none of these shall be the core of msi

func ToString(i interface{}) (string, error) {
	ret, ok := i.(string)
	if !ok {
		return "", fmt.Errorf(`not a string`)
	}
	return ret, nil
}
func ToInt(i interface{}) (int, error) {
	ret, ok := i.(int)
	if !ok {
		return 0, fmt.Errorf(`not a int`)
	}
	return ret, nil
}

func ToFloat(i interface{}) (float64, error) {
	ret, ok := i.(float64)
	if !ok {
		return float64(0), fmt.Errorf(`not a float64`)
	}
	return ret, nil
}

func ToTime(i interface{}) (*time.Time, error) {
	if ret, ok := i.(time.Time); ok {
		return &ret, nil
	}
	if ret, ok := i.(*time.Time); ok {
		return ret, nil
	}

	return nil, fmt.Errorf(`not a time.Time or *time.Time`)
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
	return ToInt(_s)
}

func GetKeyFloat64(m map[string]interface{}, key string) (float64, error) {
	_s, ok := m[key]
	if !ok {
		return 0, fmt.Errorf(`not found key [%s]`, key)
	}
	return ToFloat(_s)
}

func GetTime(m map[string]interface{}, key string) (*time.Time, error) {
	_s, ok := m[key]
	if !ok {
		return nil, fmt.Errorf(`not found key [%s]`, key)
	}
	return ToTime(_s)
}
