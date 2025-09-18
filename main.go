package main

import (
	"fmt"
	"net/http"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello World from Go on Dockploy!")
}

func main() {
	http.HandleFunc("/", handler)
	fmt.Println("Server running at :3000")
	http.ListenAndServe(":3000", nil)
}
