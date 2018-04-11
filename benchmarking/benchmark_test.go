package benchmarking

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"

	"go.opencensus.io/trace"
)

var addr string
var client *mongo.Client
var coll *mongo.Collection

func TestMain(m *testing.M) {
	var err error
	client, err = mongo.NewClient("mongodb://localhost:27017")
	if err != nil {
		log.Fatalf("client failed: %v", err)
	}
	coll = client.Database("foo").Collection("bar")
	os.Exit(m.Run())
}

func BenchmarkDeleteOne(b *testing.B) {
	traceBenchmarker(b, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Insert one element before
			_, err := coll.InsertOne(context.Background(), map[string]string{"db": "mongo"})
			if err != nil {
				b.Fatalf("Insert one failed: %v", err)
			}
			b.StartTimer()

			filters := []*bson.Document{
				bson.NewDocument(bson.EC.String("db", "mongo")),
				bson.NewDocument(bson.EC.String("db", "unknown")),
			}
			for _, filter := range filters {
				_, err := coll.DeleteOne(context.Background(), filter)
				if err != nil {
					b.Fatalf("DeleteOne failed: %v", err)
				}
			}
		}
	})
}

func BenchmarkDeleteMany(b *testing.B) {
	traceBenchmarker(b, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			// Insert some elements element before
			_, err := coll.InsertMany(context.Background(), []interface{}{
				map[string]interface{}{"v": "a", "vowel": true, "i": "a"},
				map[string]interface{}{"v": "b", "vowel": false, "i": "a"},
				map[string]interface{}{"v": "e", "vowel": true, "i": "a"},
				map[string]interface{}{"v": "i", "vowel": true, "i": "a"},
				map[string]interface{}{"v": "z", "vowel": false, "i": "a"},
				map[string]interface{}{"v": "c", "vowel": false, "i": "a"},
			})
			if err != nil {
				b.Fatalf("Insert many failed: %v", err)
			}
			b.StartTimer()

			filters := []*bson.Document{
				bson.NewDocument(bson.EC.String("vowel", "true")),
				bson.NewDocument(bson.EC.String("foo", "unknown")),
			}
			for _, filter := range filters {
				ctx := context.Background()
				_, err := coll.DeleteMany(ctx, filter)
				if err != nil {
					b.Fatalf("DeleteMany failed: %v", err)
				}
			}
		}
		_, _ = coll.DeleteMany(context.Background(), bson.NewDocument(bson.EC.String("i", "a")))
	})
}

func BenchmarkFindOne(b *testing.B) {
	traceBenchmarker(b, func(b *testing.B) {
		b.ReportAllocs()
		// Insert one element before
		_, err := coll.InsertOne(context.Background(), map[string]string{"db": "mongo"})
		if err != nil {
			b.Fatalf("Insert one failed: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			filters := []*bson.Document{
				bson.NewDocument(bson.EC.String("db", "mongo")),
				bson.NewDocument(bson.EC.String("db", "unknown")),
			}
			for _, filter := range filters {
				res := bson.NewDocument()
				_ = coll.FindOne(context.Background(), filter).Decode(res)
			}
		}

		// Clean up
		_, _ = coll.DeleteMany(context.Background(), bson.NewDocument(bson.EC.String("db", "mongo")))
	})
}

func BenchmarkFindMany(b *testing.B) {
	traceBenchmarker(b, func(b *testing.B) {
		b.ReportAllocs()
		// Insert some elements element before
		_, err := coll.InsertMany(context.Background(), []interface{}{
			map[string]interface{}{"v": "a", "vowel": true, "i": "a"},
			map[string]interface{}{"v": "b", "vowel": false, "i": "a"},
			map[string]interface{}{"v": "e", "vowel": true, "i": "a"},
			map[string]interface{}{"v": "i", "vowel": true, "i": "a"},
			map[string]interface{}{"v": "z", "vowel": false, "i": "a"},
			map[string]interface{}{"v": "c", "vowel": false, "i": "a"},
		})
		if err != nil {
			b.Fatalf("Insert many failed: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			filters := []*bson.Document{
				bson.NewDocument(bson.EC.String("vowel", "true")),
				bson.NewDocument(bson.EC.String("foo", "unknown")),
			}
			for _, filter := range filters {
				ctx := context.Background()
				cursor, err := coll.Find(ctx, filter)
				if err != nil {
					b.Fatalf("Find error: %v", err)
				}
				for cursor.Next(ctx) {
					res := bson.NewDocument()
					if err := cursor.Decode(res); err != nil {
						b.Fatalf("Cursor.Decode error: %v", err)
					}
				}
				if err := cursor.Err(); err != nil {
					b.Fatalf("Cursor.Err(): %v", err)
				}
			}
		}

		// Clean up
		_, _ = coll.DeleteMany(context.Background(), bson.NewDocument(bson.EC.String("i", "a")))
	})
}

func BenchmarkInsertOne(b *testing.B) {
	traceBenchmarker(b, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		nowUnix := time.Now().Unix()
		for i := int64(0); i < int64(b.N); i++ {
			_, err := coll.InsertOne(context.Background(), map[string]interface{}{"db": "mongo", "index": nowUnix + i})
			if err != nil {
				b.Fatalf("InsertOne failed: %v", err)
			}
		}

		// Clean up
		_, _ = coll.DeleteMany(context.Background(), bson.NewDocument(bson.EC.String("db", "mongo")))
	})
}

func BenchmarkInsertMany(b *testing.B) {
	traceBenchmarker(b, func(b *testing.B) {
		b.ReportAllocs()
		n := 1000
		contents := make([]interface{}, n)
		for i := 0; i < n; i++ {
			contents[i] = map[string]interface{}{"db": "mongo", "index": i}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := coll.InsertMany(context.Background(), contents)
			if err != nil {
				b.Fatalf("InsertMany failed: %v", err)
			}
		}

		// Clean up
		_, _ = coll.DeleteMany(context.Background(), bson.NewDocument(bson.EC.String("db", "mongo")))
	})
}

func traceBenchmarker(b *testing.B, fn func(*testing.B)) {
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	b.Run("AlwaysSample", fn)
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.NeverSample()})
	b.Run("NeverSample", fn)
}
