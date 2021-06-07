package influx

type InfluxAdapterInterface interface {
	Connect() error
	IsConnected() bool
	EnsureConnection()
	Disconnect()
	GetConnection() interface{}
	Query(queryString string) (interface{}, error)
	Write(points []*DataPoint) (bool, error)
	InitDB() bool
}
