package postgres

import (
	"context"
	"errors"
	"fmt"

	"news_app/pkg/storage"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Storage struct {
	db *pgxpool.Pool
}

func New(connStr string) (*Storage, error) {
	db, err := pgxpool.Connect(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}

	if err := db.Ping(context.Background()); err != nil {
		return nil, err
	}

	return &Storage{db: db}, nil
}

func (s *Storage) Posts(ctx context.Context) ([]storage.Post, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, title, content, author_id, author_name, created_at, published_at 
		FROM posts 
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var posts []storage.Post
	for rows.Next() {
		var p storage.Post
		err := rows.Scan(
			&p.ID, &p.Title, &p.Content, &p.AuthorID, 
			&p.AuthorName, &p.CreatedAt, &p.PublishedAt,
		)
		if err != nil {
			return nil, err
		}
		posts = append(posts, p)
	}

	return posts, nil
}

func (s *Storage) GetPost(ctx context.Context, id int) (*storage.Post, error) {
	var p storage.Post
	err := s.db.QueryRow(ctx, `
		SELECT id, title, content, author_id, author_name, created_at, published_at 
		FROM posts WHERE id = $1
	`, id).Scan(
		&p.ID, &p.Title, &p.Content, &p.AuthorID, 
		&p.AuthorName, &p.CreatedAt, &p.PublishedAt,
	)
	if err != nil {
		return nil, errors.New("post not found")
	}
	return &p, nil
}

func (s *Storage) AddPost(ctx context.Context, post storage.Post) (int, error) {
	var id int
	err := s.db.QueryRow(ctx, `
		INSERT INTO posts (title, content, author_id, author_name, created_at, published_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, 
		post.Title, post.Content, post.AuthorID, post.AuthorName, 
		post.CreatedAt, post.PublishedAt,
	).Scan(&id)
	
	return id, err
}

func (s *Storage) UpdatePost(ctx context.Context, post storage.Post) error {
	result, err := s.db.Exec(ctx, `
		UPDATE posts 
		SET title = $1, content = $2, author_id = $3, author_name = $4, 
		    created_at = $5, published_at = $6
		WHERE id = $7
	`,
		post.Title, post.Content, post.AuthorID, post.AuthorName,
		post.CreatedAt, post.PublishedAt, post.ID,
	)
	
	if err != nil {
		return err
	}
	
	if result.RowsAffected() == 0 {
		return errors.New("post not found")
	}
	
	return nil
}

func (s *Storage) DeletePost(ctx context.Context, id int) error {
	result, err := s.db.Exec(ctx, "DELETE FROM posts WHERE id = $1", id)
	if err != nil {
		return err
	}
	
	if result.RowsAffected() == 0 {
		return errors.New("post not found")
	}
	
	return nil
}

func (s *Storage) Close() error {
	s.db.Close()
	return nil
}

var _ storage.Interface = (*Storage)(nil)