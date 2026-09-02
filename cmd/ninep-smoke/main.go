package main

import (
	"fmt"
	"net"
	"os"

	legionns "github.com/rcarmo/legion/internal/namespace"
)

func main() {
	address := os.Getenv("LEGION_9P_TEST_ADDR")
	if address == "" {
		fmt.Fprintln(os.Stderr, "LEGION_9P_TEST_ADDR is required")
		os.Exit(2)
	}
	connection, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	client, err := legionns.NewClient(connection, os.Getenv("LEGION_9P_TEST_CAPABILITY"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer client.Close()
	body, err := client.Read("/cluster/health")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
