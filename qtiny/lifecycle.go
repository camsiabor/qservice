package qtiny

type LifeCycler interface {
	Start(map[string]interface{}) error
	Stop(map[string]interface{}) error
}
