package tests

import (
	"rasulalizadeh/gokit-db/mongo"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMongoConnect(t *testing.T) {
	fakeAdapter := mongo.NewMongoAdapter("localhost:12300", "test")
	err := fakeAdapter.Connect()
	assert.NotNil(t, err)
	adapter := mongo.NewMongoAdapterDefaultHost("test")
	err = adapter.Connect()
	assert.Nil(t, err)
	assert.True(t, adapter.IsConnected())
	adapter.Disconnect()
	assert.False(t, adapter.IsConnected())
}

func TestMongoCRUD(t *testing.T) {
	adapter := mongo.NewMongoAdapterDefaultHost("test")
	err := adapter.Connect()
	assert.Nil(t, err)
	err = adapter.Insert("test", bson.M{"name": "test call", "time": time.Now()})
	assert.Nil(t, err)
	err = adapter.Insert("test", bson.M{"name": "rmCall", "time": time.Now()})
	assert.Nil(t, err)
	err = adapter.Remove("test", bson.M{"name": "rmCall"})
	assert.Nil(t, err)
	result := adapter.FindOne("test", bson.M{"name": "total"})
	if result.Err() == nil {
		var row bson.M
		err = result.Decode(&row)
		err := adapter.Upsert("test", bson.M{"name": "total"}, bson.M{"count": row["count"].(int32) + 1})
		assert.Nil(t, err)
	} else {
		err := adapter.Upsert("test", bson.M{"name": "total"}, bson.M{"count": 1})
		assert.Nil(t, err)
	}

}
