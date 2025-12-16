package memdb

import (
	"context"
	"errors"
	"sync"
	"time"

	"news_app/pkg/storage"
)

type Storage struct {
	mu    sync.RWMutex
	posts map[int]storage.Post
	idSeq int
}

func New() *Storage {
	return &Storage{
		posts: make(map[int]storage.Post),
		idSeq: 1,
	}
}

func (s *Storage) Posts(ctx context.Context) ([]storage.Post, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	posts := make([]storage.Post, 0, len(s.posts))
	for _, post := range s.posts {
		posts = append(posts, post)
	}
	return posts, nil
}

func (s *Storage) GetPost(ctx context.Context, id int) (*storage.Post, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	post, exists := s.posts[id]
	if !exists {
		return nil, errors.New("post not found")
	}
	return &post, nil
}

func (s *Storage) AddPost(ctx context.Context, post storage.Post) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	post.ID = s.idSeq
	s.idSeq++

	if post.CreatedAt == 0 {
		post.CreatedAt = time.Now().Unix()
	}
	if post.PublishedAt == 0 {
		post.PublishedAt = time.Now().Unix()
	}

	s.posts[post.ID] = post
	return post.ID, nil
}

func (s *Storage) UpdatePost(ctx context.Context, post storage.Post) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.posts[post.ID]; !exists {
		return errors.New("post not found")
	}

	s.posts[post.ID] = post
	return nil
}

func (s *Storage) DeletePost(ctx context.Context, id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.posts[id]; !exists {
		return errors.New("post not found")
	}

	delete(s.posts, id)
	return nil
}

func (s *Storage) Close() error {
	return nil
}

var _ storage.Interface = (*Storage)(nil)