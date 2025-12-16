package storage

import "context"

type Post struct {
	ID          int    `json:"id" bson:"id" db:"id"`
	Title       string `json:"title" bson:"title" db:"title"`
	Content     string `json:"content" bson:"content" db:"content"`
	AuthorID    int    `json:"author_id" bson:"author_id" db:"author_id"`
	AuthorName  string `json:"author_name" bson:"author_name" db:"author_name"`
	CreatedAt   int64  `json:"created_at" bson:"created_at" db:"created_at"`
	PublishedAt int64  `json:"published_at" bson:"published_at" db:"published_at"`
}

type Interface interface {
	Posts(ctx context.Context) ([]Post, error)
	GetPost(ctx context.Context, id int) (*Post, error)
	AddPost(ctx context.Context, post Post) (int, error)
	UpdatePost(ctx context.Context, post Post) error
	DeletePost(ctx context.Context, id int) error
	Close() error
}