package result

type StringsResult interface {
	Bool() (bool, error)
	String() (string, error)
}
