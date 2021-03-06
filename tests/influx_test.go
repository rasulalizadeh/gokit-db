package tests

import (
	"testing"

	"github.com/rasulalizadeh/gokit-db/influx"

	"github.com/stretchr/testify/assert"
)

func TestInfluxConnect(t *testing.T) {
	fakeAdapter := influx.NewInfluxAdapter("test", "http://localhost:2035/")
	err := fakeAdapter.Connect()
	assert.NotNil(t, err)
	adapter := influx.NewInfluxAdapterDefaultHost("test")
	err = adapter.Connect()
	assert.Nil(t, err)
}
