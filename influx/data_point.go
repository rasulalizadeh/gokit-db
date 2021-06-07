package influx

import (
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	influx "github.com/influxdata/influxdb1-client/v2"
	"time"
)

type DataPoint struct {
	measurement string
	tags        map[string]string
	values      map[string]interface{}
	time        time.Time
}

func NewDataPoint(measurement string, tags map[string]string, values map[string]interface{}, time time.Time) *DataPoint {
	return &DataPoint{
		measurement: measurement,
		tags:        tags,
		values:      values,
		time:        time,
	}
}

func NewDataPointNow(measurement string, tags map[string]string, values map[string]interface{}) *DataPoint {
	return NewDataPoint(measurement, tags, values, time.Now())
}

func (i *DataPoint) ToV1DataPoint() *influx.Point {
	dataPoint, err := influx.NewPoint(i.measurement, i.tags, i.values, i.time)
	if err == nil {
		return dataPoint
	} else {
		return nil
	}
}

func (i *DataPoint) ToV2DataPoint() *write.Point {
	return influxdb2.NewPoint(i.measurement, i.tags, i.values, i.time)
}