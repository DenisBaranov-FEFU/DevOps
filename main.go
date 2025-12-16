package main

import (
	"log"
	"news_app/cmd/server"
)

func main() {
	log.Println("Starting News App...")
	server.Run()
}