// kafka-demo provisions an explicit local demo topology, or runs a server/client.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/brokeradmin"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	transport "github.com/nextpkg/courier/transport/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	mode := flag.String("mode", "client", "provision, server or client")
	service := flag.String("service", "all", "server: orders, inventory or all")
	brokers := flag.String("brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	prefix := flag.String("prefix", "courier.demo", "shared deployment namespace")
	flag.Parse()
	if *mode != "provision" && *mode != "server" && *mode != "client" {
		return fmt.Errorf("mode must be provision, server or client")
	}
	if *service != "all" && *service != "orders" && *service != "inventory" {
		return fmt.Errorf("service must be orders, inventory or all")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	opts := transport.Options{Brokers: strings.Split(*brokers, ","), Prefix: *prefix}
	if *mode == "provision" {
		// Deployment action only: runtime constructors and calls never create topics.
		// Replication factor 1 is for this local example, not a production policy.
		admin, err := kgo.NewClient(kgo.SeedBrokers(opts.Brokers...))
		if err != nil {
			return err
		}
		defer admin.Close()
		start, done := context.WithTimeout(ctx, 15*time.Second)
		defer done()
		topics := map[string]int32{*prefix + ".request.Orders": 3, *prefix + ".request.Inventory": 3, *prefix + ".replies": 1}
		if err := brokeradmin.Create(start, admin, topics); err != nil {
			return err
		}
		fmt.Println("demo topics provisioned:", topics)
		return nil
	}
	tr, err := transport.New(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tr.Close(stop); err != nil {
			log.Printf("close tr: %v", err)
		}
	}()
	if *mode == "server" {
		s, err := courier.NewServer(tr, courier.ServerOptions{})
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
		if *service == "all" || *service == "orders" {
			if err = demo.RegisterOrders(s, &handlers.Orders{}); err != nil {
				return err
			}
		}
		if *service == "all" || *service == "inventory" {
			if err = demo.RegisterInventory(s, &handlers.Inventory{}); err != nil {
				return err
			}
		}
		if err = s.Start(ctx); err != nil {
			return err
		}
		fmt.Println("server ready:", *service)
		<-ctx.Done()
		stop, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		return s.Close(stop)
	}
	callCtx, done := context.WithTimeout(ctx, 10*time.Second)
	defer done()
	c, err := courier.NewClient(callCtx, tr, courier.ClientOptions{})
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
	// This business code is identical to the memory and NATS examples.
	future, err := demo.NewOrdersAsyncClient(c).GetAsync(callCtx, "order-42", 40)
	if err != nil {
		return err
	}
	count, err := demo.NewInventoryAsyncClient(c).Count(callCtx, "SKU-123")
	if err != nil {
		return err
	}
	fmt.Println("inventory count:", count)
	item, err := future.Await(callCtx)
	if err != nil {
		return err
	}
	fmt.Printf("order: id=%s source=%s\n", item.ID, item.Source)
	return nil
}
