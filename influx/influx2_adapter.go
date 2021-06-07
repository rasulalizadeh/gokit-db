package influx

import (
	"context"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"log"
	"strings"
)

const token = "IrrT8bGezrpOIrjKcGeTFWeYKWiEDCp393P1WKQ1LVTv-i8Ukb5f9ppnS41-diBM6KNEM7Ef3lEzhbXNtQLOnw=="

type Influx2Adapter struct {
	connection influxdb2.Client
	host       string
	bucketName string
	orgName    string
	token	   string
}

func NewInflux2AdapterRootDefaultHost(organization string, bucketName string) *Influx2Adapter {
	return NewInflux2Adapter(organization, bucketName, "http://localhost:8086", token)
}
func NewInflux2AdapterRoot(organization string, bucketName string, host string) *Influx2Adapter {
	return NewInflux2Adapter(organization, bucketName, host, token)
}

func NewInflux2AdapterDefaultHost(organization string, bucketName string, token string) *Influx2Adapter {
	return NewInflux2Adapter(organization, bucketName, "http://localhost:8086", token)
}

func NewInflux2Adapter(organization string, bucketName string, host string, token string) *Influx2Adapter {
	return &Influx2Adapter{
		connection: nil,
		bucketName: bucketName,
		orgName:    organization,
		host:       host,
		token: 		token,
	}
}

func (i *Influx2Adapter) Connect() error {
	if i.IsConnected() {
		return nil
	}
	conn := influxdb2.NewClientWithOptions(i.host, i.token, influxdb2.DefaultOptions().SetBatchSize(200))
	status, err := conn.Ready(context.Background())
	if status {
		i.connection = conn
	}
	return err
}

func (i *Influx2Adapter) EnsureConnection() {
	if !i.IsConnected() {
		err := i.Connect()
		if err != nil {
			panic("Influx database v2 connection problem")
		}
	}
}

func (i *Influx2Adapter) IsConnected() bool {
	return i.connection != nil
}

func (i *Influx2Adapter) Disconnect() {
	if i.IsConnected() {
		i.connection.Close()
		i.connection = nil
	}
}

func (i *Influx2Adapter) GetConnection() interface{} {
	return i.connection
}

func (i *Influx2Adapter) Query(queryString string) (interface{}, error) {
	i.EnsureConnection()
	queryAPI := i.connection.QueryAPI(i.orgName)
	response, err := queryAPI.Query(context.Background(), queryString)
	if err != nil || response.Err() != nil {
		if response != nil && response.Err() != nil {
			if strings.Contains(response.Err().Error(), "database not found") {
				if i.InitDB() {
					return i.Query(queryString)
				}
			} else {
				log.Fatalln(response.Err())
			}
		} else {
			log.Fatalln(err)
		}
		return response, err
	}
	return response, err
}

func (i *Influx2Adapter) Write(points []*DataPoint) (bool, error) {
	i.EnsureConnection()
	writeAPI := i.connection.WriteAPI(i.orgName, i.bucketName)
	errorsCh := writeAPI.Errors()
	failed := false
	var lastError error
	go func() {
		for err := range errorsCh {
			failed = true
			lastError = err
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					if i.InitDB() {
						i.Write(points)
					}
				}
			}
		}
	}()
	for _, point := range points {
		writeAPI.WritePoint(point.ToV2DataPoint())
	}
	writeAPI.Flush()
	return !failed, lastError
}

func (i *Influx2Adapter) InitDB() bool {
	i.EnsureConnection()
	if !i.HasOrganization(i.orgName) {
		_, err := i.CreateOrganization(i.orgName)
		if err != nil {
			return false
		}
	}
	if !i.HasBucket(i.bucketName) {
		_, err := i.CreateBucket(i.orgName, i.bucketName)
		if err != nil {
			return false
		}
	}
	return true
}

func (i *Influx2Adapter) HasOrganization(name string) bool {
	i.EnsureConnection()
	orgAPI := i.connection.OrganizationsAPI()
	_, err := orgAPI.FindOrganizationByName(context.Background(), name)
	if err != nil {
		return false
	}
	return true
}

func (i *Influx2Adapter) GetOrganization(name string) (interface{}, error) {
	i.EnsureConnection()
	orgAPI := i.connection.OrganizationsAPI()
	org, err := orgAPI.FindOrganizationByName(context.Background(), name)
	if err != nil {
		return nil, err
	}
	return org, err
}

func (i *Influx2Adapter) CreateOrganization(name string) (interface{}, error) {
	i.EnsureConnection()
	org, err := i.GetOrganization(name)
	if err != nil {
		orgAPI := i.connection.OrganizationsAPI()
		org, err = orgAPI.CreateOrganization(context.Background(), &domain.Organization{
			Name:        name,
		})
		if err != nil {
			return nil, err
		}
	}
	return org, nil
}

func (i *Influx2Adapter) HasBucket(bucketName string) bool {
	i.EnsureConnection()
	bucketAPI := i.connection.BucketsAPI()
	_, err := bucketAPI.FindBucketByName(context.Background(), bucketName)
	if err != nil {
		return false
	}
	return true
}

func (i *Influx2Adapter) GetBucket(bucketName string) (interface{}, error) {
	i.EnsureConnection()
	bucketAPI := i.connection.BucketsAPI()
	bucket, err := bucketAPI.FindBucketByName(context.Background(), bucketName)
	if err != nil {
		return nil, err
	}
	return bucket, err
}

func (i *Influx2Adapter) CreateBucket(organizationName string, bucketName string) (interface{}, error) {
	i.EnsureConnection()
	bucket, err := i.GetBucket(bucketName)
	if err != nil {
		bucketAPI := i.connection.BucketsAPI()
		org, err := i.GetOrganization(organizationName)
		if err != nil {
			return false, err
		}
		if org, ok := org.(*domain.Organization); ok {
			bucket, err = bucketAPI.CreateBucketWithName(context.Background(), org, bucketName)
		}
		if err != nil {
			return nil, err
		}
	}
	return bucket, nil
}