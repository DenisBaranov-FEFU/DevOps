package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"news_app/pkg/api"
	"news_app/pkg/storage"
	"news_app/pkg/storage/memdb"
	"news_app/pkg/storage/postgres"
	"news_app/pkg/storage/mongo"
)

func main() {
	addr := flag.String("addr", ":8080", "Server address")
	dbType := flag.String("db", "memory", "Database type: memory, postgres, mongodb")
	connStr := flag.String("conn", "", "Database connection string")
	flag.Parse()

	var store storage.Interface
	var err error

	switch *dbType {
	case "memory":
		store = memdb.New()
		log.Println("Using in-memory storage")
	case "postgres":
		if *connStr == "" {
			*connStr = "postgres://user:password@localhost:5432/news_app?sslmode=disable"
		}
		store, err = postgres.New(*connStr)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Using PostgreSQL storage")
	case "mongodb":
		if *connStr == "" {
			*connStr = "mongodb://localhost:27017"
		}
		store, err = mongo.New(*connStr)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Using MongoDB storage")
	default:
		log.Fatalf("Unknown database type: %s", *dbType)
	}
	defer store.Close()

	api := api.New(store)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      api.Router(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Server starting on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server stopped")
}