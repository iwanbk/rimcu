package resp3

import "fmt"

type StringsResult struct {
	typ cacheTyp
	val interface{}
}

func newStringsResult(cv cacheVal) *StringsResult {
	return &StringsResult{
		typ: cv.typ,
		val: cv.val,
	}
}

func (sr *StringsResult) Bool() (bool, error) {
	if sr.typ != cacheTypBool {
		return false, fmt.Errorf("not boolean")
	}
	return sr.val.(bool), nil
}

func (sr *StringsResult) String() (string, error) {
	if sr.typ != cacheTypString {
		return "", fmt.Errorf("not a string")
	}
	return sr.val.(string), nil
}

type cacheVal struct {
	typ cacheTyp
	val interface{}
}

type cacheTyp int8

const (
	cacheTypString = 1
	cacheTypBool   = 2 // TODO: it is not really supported yet
)
