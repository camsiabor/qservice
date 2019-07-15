package qtiny

type LifeCycler interface {
	Start(config map[string]interface{}) error
	Stop(config map[string]interface{}) error
}
