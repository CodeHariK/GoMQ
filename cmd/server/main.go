package main

import (
	"log"

	"github.com/codeharik/GoMQ/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
