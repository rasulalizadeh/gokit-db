package tests

import (
	"rasulalizadeh/gokit/database/influx"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInfluxConnect(t *testing.T) {
	fakeAdapter := influx.NewInfluxAdapter("test", "http://localhost:2035/")
	err := fakeAdapter.Connect()
	assert.NotNil(t, err)
	adapter := influx.NewInfluxAdapterDefaultHost("test")
	err = adapter.Connect()
	assert.Nil(t, err)
}

