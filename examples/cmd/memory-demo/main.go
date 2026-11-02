package main

import (
	"context"
	"fmt"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	"github.com/nextpkg/courier/transport/memory"
	"log"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bus, err := memory.New(memory.Options{})
	if err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bus.Close(stop); err != nil {
			log.Printf("close bus: %v", err)
		}
	}()
	s, err := demo.ListenOrders(ctx, bus, &handlers.Orders{})
	if err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Close(stop); err != nil {
			log.Printf("close s: %v", err)
		}
	}()
	client, c, err := demo.DialOrders(ctx, bus)
	if err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(stop); err != nil {
			log.Printf("close c: %v", err)
		}
	}()
	result, err := client.Get(ctx, "order-42", 0)
	if err != nil {
		return err
	}
	fmt.Printf("result: id=%s source=%s values=%v\n", result.ID, result.Source, result.Values)
	return nil
}
