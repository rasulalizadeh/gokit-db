package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type MongoAdapter struct {
	connection *mongo.Client
	dbName string
	host string
}

func NewMongoAdapterDefaultHost(dbName string) *MongoAdapter {
	return NewMongoAdapter("localhost:27017", dbName)
}

func NewMongoAdapter(host string, dbName string) *MongoAdapter {
	return &MongoAdapter{host: host, dbName: dbName}
}

func (m *MongoAdapter) Connect() error {
	if m.IsConnected() {
		return nil
	}
	conn, err := mongo.NewClient(options.Client().ApplyURI("mongodb://" + m.host))
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err = conn.Connect(ctx)
	if err != nil {
		return err
	}
	err = conn.Ping(ctx, readpref.Primary())
	if err == nil {
		m.connection = conn
	}
	return err
}

func (m *MongoAdapter) IsConnected() bool {
	return m.connection != nil
}

func (m *MongoAdapter) EnsureConnection() {
	if !m.IsConnected() {
		err := m.Connect()
		if err != nil {
			panic("Mongo database connection problem")
		}
	}
}

func (m *MongoAdapter) Disconnect() {
	if m.IsConnected() {
		err := m.connection.Disconnect(context.Background())
		if err == nil {
			m.connection = nil
		}
	}
}

func (m *MongoAdapter) GetConnection() interface{} {
	return m.connection
}

func (m *MongoAdapter) Insert(collectionName string, fields bson.M) error {
	m.EnsureConnection()
	collection := m.connection.Database(m.dbName).Collection(collectionName)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := collection.InsertOne(ctx, fields)
	return err
}

func (m *MongoAdapter) Upsert(collectionName string, filters bson.M, fields bson.M) error {
	m.EnsureConnection()
	collection := m.connection.Database(m.dbName).Collection(collectionName)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := collection.UpdateOne(ctx,
		filters,
		bson.M{"$set": fields},
		options.Update().SetUpsert(true))
	return err
}

func (m *MongoAdapter) Update(collectionName string, filters bson.M, fields bson.M) error {
	m.EnsureConnection()
	collection := m.connection.Database(m.dbName).Collection(collectionName)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := collection.UpdateOne(ctx,
		filters,
		bson.M{"$set": fields})
	return err
}

func (m *MongoAdapter) Remove(collectionName string, filters bson.M) error {
	m.EnsureConnection()
	collection := m.connection.Database(m.dbName).Collection(collectionName)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := collection.DeleteOne(ctx, filters)
	return err
}

func (m *MongoAdapter) Find(collectionName string, filters bson.M, page int, limit int) (*mongo.Cursor, error) {
	m.EnsureConnection()
	collection := m.connection.Database(m.dbName).Collection(collectionName)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	findOpts := options.FindOptions{}
	if limit > 0 {
		limit64 := int64(limit)
		findOpts.Limit = &limit64
		if page >= 0 {
			skip64 := limit64 * int64(page - 1)
			findOpts.Skip = &skip64
		}
	}
	cur, err := collection.Find(ctx, filters, &findOpts)
	return cur, err
}

func (m *MongoAdapter) FindOne(collectionName string, filters bson.M) *mongo.SingleResult {
	m.EnsureConnection()
	collection := m.connection.Database(m.dbName).Collection(collectionName)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	result := collection.FindOne(ctx, filters)
	return result
}

func (m *MongoAdapter) FindAll(collectionName string, filters bson.M) (*mongo.Cursor, error) {
	m.EnsureConnection()
	collection := m.connection.Database(m.dbName).Collection(collectionName)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	cur, err := collection.Find(ctx, filters)
	return cur, err
}

//func MongoSave(code string, name string, uid string) bool{
//	if mongoClient == nil {
//		mongoConnect()
//	}
//	collection := mongoClient.Database(mongoDBName).Collection("companies")
//	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
//	_, err := collection.UpdateOne(ctx,
//		bson.M{"name": code},
//		bson.M{"$set": bson.M{"name": code, "description": name, "code": uid}},
//		options.Update().SetUpsert(true))
//	if err != nil {
//		log.Fatalln(err)
//		return false
//	}
//	return true
//}
//
//func MongoSaveCompany(company *stock.Company) bool {
//	if mongoClient == nil {
//		mongoConnect()
//	}
//	collection := mongoClient.Database(mongoDBName).Collection("companies")
//	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
//	_, err := collection.UpdateOne(ctx,
//		company.GetBSONFilter(),
//		bson.M{"$set": company.GetBSONData()},
//		options.Update().SetUpsert(true))
//	if err != nil {
//		log.Fatalln(err)
//		return false
//	}
//	return true
//}

//func MongoFindCompaniesPaginate(filter bson.M, page int, limit int) []stock.Company {
//	if mongoClient == nil {
//		mongoConnect()
//	}
//	if filter == nil {
//		filter = bson.M{}
//	}
//	filter["status"] = 1
//	collection := mongoClient.Database(mongoDBName).Collection("companies")
//	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
//	findOpts := options.FindOptions{}
//	if limit > 0 {
//		limit64 := int64(limit)
//		findOpts.Limit = &limit64
//		if page >= 0 {
//			skip64 := limit64 * int64(page - 1)
//			findOpts.Skip = &skip64
//		}
//	}
//	cur, err := collection.Find(ctx, filter, &findOpts)
//	if err != nil { log.Fatal(err) }
//	defer cur.Close(ctx)
//	var companies []stock.Company
//	for cur.Next(ctx) {
//		var result bson.M
//		err := cur.Decode(&result)
//		if err != nil { log.Fatal(err) }
//		companies = append(companies, stock.NewCompanyFromJSON(result))
//	}
//	if err := cur.Err(); err != nil {
//		log.Fatal(err)
//	}
//	return companies
//}
//
//func MongoAllCompanies() []stock.Company {
//	return MongoFindCompaniesPaginate(nil, -1, -1)
//}
//
//func MongoAllCompaniesPaginate(page int, count int) []stock.Company {
//	return MongoFindCompaniesPaginate(nil, page, count)
//}
//
//func MongoFindCompanies(filter bson.M) []stock.Company {
//	return MongoFindCompaniesPaginate(filter, -1, -1)
//}
