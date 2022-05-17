package resp3

import "fmt"

type StringsResult struct {
	typ            cacheTyp
	val            interface{}
	fromLocalCache bool
}

func newStringsResult(cv cacheVal, fromLocalCache bool) *StringsResult {
	return &StringsResult{
		typ:            cv.typ,
		val:            cv.val,
		fromLocalCache: fromLocalCache,
	}
}

func (sr *StringsResult) Bool() (bool, error) {
	if sr.typ != cacheTypBool {
		return false, fmt.Errorf("not boolean")
	}
	return sr.val.(bool), nil
}

func (sr *StringsResult) FromLocalCache() bool {
	return sr.fromLocalCache
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
