package main

import (
	"context"
	"github.com/nextpkg/courier"
	"github.com/nextpkg/courier/examples/gen/demo"
	"github.com/nextpkg/courier/examples/handlers"
	transport "github.com/nextpkg/courier/transport/nats"
	"flag"
	"fmt"
	sdk "github.com/nats-io/nats.go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	mode := flag.String("mode", "client", "server or client")
	service := flag.String("service", "all", "server: orders, inventory or all")
	url := flag.String("url", sdk.DefaultURL, "NATS URL")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	conn, err := sdk.Connect(*url, sdk.Name("courier-example"), sdk.ReconnectBufSize(-1), sdk.FlusherTimeout(time.Second))
	if err != nil {
		return err
	}
	defer conn.Close()
	tr, err := transport.New(conn, transport.Options{})
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
	switch *mode {
	case "server":
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
		if *service == "orders" || *service == "all" {
			if err = demo.RegisterOrders(s, &handlers.Orders{}); err != nil {
				return err
			}
		}
		if *service == "inventory" || *service == "all" {
			if err = demo.RegisterInventory(s, &handlers.Inventory{}); err != nil {
				return err
			}
		}
		if err = s.Start(ctx); err != nil {
			return err
		}
		fmt.Println("server ready:", *service)
		<-ctx.Done()
		stop, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		return s.Close(stop)
	case "client":
		callCtx, done := context.WithTimeout(ctx, 5*time.Second)
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
	default:
		return fmt.Errorf("mode must be server or client")
	}
}
