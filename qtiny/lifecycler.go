package qtiny

type LifeCycler interface {
	Start(config map[string]interface{}, future Future) error
	Stop(config map[string]interface{}, future Future) error
}
