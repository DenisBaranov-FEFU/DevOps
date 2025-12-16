package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"news_app/pkg/storage"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Storage struct {
	client     *mongo.Client
	database   string
	collection string
}

func New(connStr string) (*Storage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &Storage{
		client:     client,
		database:   "news_app",
		collection: "posts",
	}, nil
}

func (s *Storage) Posts(ctx context.Context) ([]storage.Post, error) {
	collection := s.client.Database(s.database).Collection(s.collection)
	
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var posts []storage.Post
	for cursor.Next(ctx) {
		var post storage.Post
		if err := cursor.Decode(&post); err != nil {
			return nil, err
		}
		posts = append(posts, post)
	}

	return posts, nil
}

func (s *Storage) GetPost(ctx context.Context, id int) (*storage.Post, error) {
	collection := s.client.Database(s.database).Collection(s.collection)
	
	var post storage.Post
	err := collection.FindOne(ctx, bson.M{"id": id}).Decode(&post)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errors.New("post not found")
		}
		return nil, err
	}
	
	return &post, nil
}

func (s *Storage) AddPost(ctx context.Context, post storage.Post) (int, error) {
	collection := s.client.Database(s.database).Collection(s.collection)
	
	if post.ID == 0 {
		var lastPost storage.Post
		opts := options.FindOne().SetSort(bson.M{"id": -1})
		err := collection.FindOne(ctx, bson.M{}, opts).Decode(&lastPost)
		if err != nil && err != mongo.ErrNoDocuments {
			return 0, err
		}
		if err == mongo.ErrNoDocuments {
			post.ID = 1
		} else {
			post.ID = lastPost.ID + 1
		}
	}
	
	if post.CreatedAt == 0 {
		post.CreatedAt = time.Now().Unix()
	}
	if post.PublishedAt == 0 {
		post.PublishedAt = time.Now().Unix()
	}

	_, err := collection.InsertOne(ctx, post)
	return post.ID, err
}

func (s *Storage) UpdatePost(ctx context.Context, post storage.Post) error {
	collection := s.client.Database(s.database).Collection(s.collection)
	
	result, err := collection.UpdateOne(
		ctx,
		bson.M{"id": post.ID},
		bson.M{"$set": post},
	)
	if err != nil {
		return err
	}
	
	if result.MatchedCount == 0 {
		return errors.New("post not found")
	}
	
	return nil
}

func (s *Storage) DeletePost(ctx context.Context, id int) error {
	collection := s.client.Database(s.database).Collection(s.collection)
	
	result, err := collection.DeleteOne(ctx, bson.M{"id": id})
	if err != nil {
		return err
	}
	
	if result.DeletedCount == 0 {
		return errors.New("post not found")
	}
	
	return nil
}

func (s *Storage) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.client.Disconnect(ctx)
}

var _ storage.Interface = (*Storage)(nil)