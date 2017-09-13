package msi

import "reflect"

func IsArray(v interface{}) bool {
	if v == nil {
		return false
	}
	rt := reflect.TypeOf(v)
	switch rt.Kind() {
	case reflect.Slice:
		return true
	case reflect.Array:
		return true
	default:
		return false
	}

	return false
}
