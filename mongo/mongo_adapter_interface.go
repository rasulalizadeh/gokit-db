package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoAdapterInterface interface {
	Connect() error
	IsConnected() bool
	EnsureConnection()
	Disconnect()
	GetConnection() interface{}
	Insert(collectionName string, fields bson.M) error
	Upsert(collectionName string, filters bson.M, fields bson.M) error
	Update(collectionName string, filters bson.M, fields bson.M) error
	Remove(collectionName string, filters bson.M) error
	FindOne(collectionName string, filters bson.M) *mongo.SingleResult
	Find(collectionName string, filters bson.M, page int, limit int) (*mongo.Cursor, error)
	FindAll(collectionName string, filters bson.M) (*mongo.Cursor, error)
}
