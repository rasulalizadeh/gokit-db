package tests

import (
	"testing"

	"github.com/rasulalizadeh/gokit-db/influx"

	"github.com/stretchr/testify/assert"
)

func TestInflux2Connect(t *testing.T) {
	fakeAdapter := influx.NewInflux2AdapterRoot("test", "test", "http://localhost:3254")
	err := fakeAdapter.Connect()
	assert.NotNil(t, err)
	adapter := influx.NewInflux2AdapterRootDefaultHost("test", "test")
	err = adapter.Connect()
	assert.Nil(t, err)
	assert.True(t, adapter.IsConnected())
	adapter.Disconnect()
	assert.False(t, adapter.IsConnected())
}

func TestInflux2AddPoint(t *testing.T) {
	adapter := influx.NewInflux2AdapterRootDefaultHost("test", "test")
	var dataPoint []*influx.DataPoint
	testPoint := influx.NewDataPointNow("test run",
		map[string]string{"name": "addPoint"}, map[string]interface{}{"runtime": 1.15})
	dataPoint = append(dataPoint, testPoint)
	result, err := adapter.Write(dataPoint)
	//adapter.Disconnect()
	assert.True(t, result)
	assert.Nil(t, err)
}

func TestQuery(t *testing.T) {
	adapter := influx.NewInflux2AdapterRootDefaultHost("test", "test")
	adapter.Query(`from(bucket:"companies")|> range(start: -1h) |> filter(fn: (r) => r._measurement == "stat")`)
}
