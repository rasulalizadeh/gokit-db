package influx

import (
	"fmt"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	influx "github.com/influxdata/influxdb1-client/v2"
	"log"
	"strings"
)

type InfluxAdapter struct {
	connection influx.Client
	host       string
	bucketName string
}

func NewInfluxAdapterDefaultHost(bucketName string) InfluxAdapter {
	return NewInfluxAdapter(bucketName, "http://localhost:8086")
}

func NewInfluxAdapter(bucketName string, host string) InfluxAdapter {
	adapter := InfluxAdapter{
		connection: nil,
		bucketName: bucketName,
		host:       host,
	}
	return adapter
}

func (i *InfluxAdapter) Connect() error {
	conn, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr: i.host,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	} else {
		_, _, err = conn.Ping(200)
		if err == nil {
			i.connection = conn
		}
	}
	return err
}

func (i *InfluxAdapter) EnsureConnection() {
	if !i.IsConnected() {
		i.Connect()
	}
	if !i.IsConnected() {
		panic("Influx database v1 connection problem")
	}
}

func (i *InfluxAdapter) IsConnected() bool {
	return i.connection != nil
}

func (i *InfluxAdapter) Disconnect() {
	defer i.connection.Close()
}

func (i *InfluxAdapter) GetConnection() interface{} {
	return i.connection
}

func (i *InfluxAdapter) Query(queryString string) (interface{}, error) {
	i.EnsureConnection()
	q := influx.NewQuery(queryString, i.bucketName, "")
	response, err := i.connection.Query(q)
	if err != nil || response.Error() != nil {
		if response != nil && response.Error() != nil {
			if strings.Contains(response.Error().Error(), "database not found") {
				if i.InitDB() {
					return i.Query(queryString)
				}
			} else {
				log.Fatalln(response.Error())
			}
		} else {
			log.Fatalln(err)
		}
		return response, err
	}
	return response, err
}

func (i *InfluxAdapter) Write(points []*DataPoint) (bool, error) {
	i.EnsureConnection()
	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Precision: "",
		Database:  i.bucketName,
	})
	for _, point := range points {
		bp.AddPoint(point.ToV1DataPoint())
	}
	err := i.connection.Write(bp)
	if err != nil {
		if strings.Contains(err.Error(), "database not found") {
			if i.InitDB() {
				return i.Write(points)
			}
		} else {
			log.Fatalln(err.Error())
		}
	}
	return true, err
}

func (i *InfluxAdapter) InitDB() bool {
	i.EnsureConnection()
	q := influx.NewQuery("CREATE DATABASE "+i.bucketName+"", "", "")
	response, err := i.connection.Query(q)
	if err != nil || response.Error() != nil {
		return false
	}
	return true
}

//func InfluxInsertCompanyPoints(company *stock.Company) bool {
//	//status, _, _ := query("SELECT count(value) FROM cpu_load")
//	if len(company.Values) > 0 {
//		status, _ := influxWrite(company.GetValuePoints())
//		return status
//	}
//	return true
//}
//
//func InfluxLoadCompanyValues(company *stock.Company) error {
//	queryString := "SELECT " +
//		"mean(\"close\") AS \"mean_close\", " +
//		"mean(\"first\") AS \"mean_first\", " +
//		"mean(\"high\") AS \"mean_high\", " +
//		"mean(\"last\") AS \"mean_last\", " +
//		"mean(\"low\") AS \"mean_low\", " +
//		"mean(\"open\") AS \"mean_open\" " +
//		"FROM \"stock\".\"autogen\".\"value\" " +
//		"WHERE \"name\"='"+company.Name +"' GROUP BY time(1d) FILL(previous)"
//	result, response, err := influxQuery(queryString)
//	if result && response != nil {
//		company.ClearValues()
//		for _, row := range response.Results[0].Series[0].Values {
//			date, _ := time.Parse("2006-01-02T15:04:05Z", row[0].(string))
//			end, _ := row[1].(json.Number).Int64()
//			first, _ := row[2].(json.Number).Int64()
//			high, _ := row[3].(json.Number).Int64()
//			last, _ := row[4].(json.Number).Int64()
//			low, _ := row[5].(json.Number).Int64()
//			open, _ := row[6].(json.Number).Int64()
//			company.AddValue(stock.CompanyValue{
//				Time:      date,
//				Close:     uint32(end),
//				First:     uint32(first),
//				High:      uint32(high),
//				Last:      uint32(last),
//				Low:       uint32(low),
//				Open:      uint32(open),
//				saved	: true,
//			})
//		}
//	}
//	fmt.Println("Loaded", company.GetName(),"company values, amount: ", len(company.GetValues()))
//	return err
//}
