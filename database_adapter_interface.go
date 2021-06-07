package database

type DatabaseAdapterInterface interface {
	Connect() error
	IsConnected() bool
	EnsureConnection()
	Disconnect()
	GetConnection() interface{}
}
