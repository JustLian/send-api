package main

import (
	"log"
	"os"

	"rian.moe/sender/internal/server"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if err := server.Run(addr); err != nil {
		log.Fatalf("server: %v", err)
	}
}
