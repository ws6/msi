package msi

import (
	"regexp"
)

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func StringInRegexSlice(a string, list []string) bool {
	for _, pattern := range list {

		matched, _ := regexp.MatchString(pattern, a)
		if matched {
			return true
		}

	}
	return false
}
