package core

type MessageOptions map[string]interface{}
type MessageHeaders map[string]interface{}

type Message struct {
	Id      int64
	Address string
	Data    interface{}
	Headers MessageHeaders
	Options MessageOptions
}
