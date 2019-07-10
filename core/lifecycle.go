package core

type LifeCycler interface {
	Start(...interface{}) error
	Stop(...interface{}) error
}
