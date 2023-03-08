package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	dockercli "github.com/docker/docker/client"
	"github.com/spacelift-io/homework-object-storage/internal/gateway"
	"github.com/spacelift-io/homework-object-storage/internal/storage"
)

func exit(err error) {
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	cli, err := dockercli.NewClientWithOpts(dockercli.FromEnv)
	if err != nil {
		panic(err)
	}
	bucketName := os.Getenv("BUCKET_NAME")
	if bucketName == "" {
		bucketName = "default"
	}
	storage := storage.NewBalancedStorage(cli, bucketName)

	mainSrv := gateway.NewServer(storage)

	go func() {
		if err := mainSrv.Start(":3000"); err != nil {
			log.Printf("Server error: %s\n", err)
			return
		}
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sigc
	cancel()
	log.Printf("Received signal: '%s', shutting down server\n", sig.String())

	closeCtx, cancelClose := context.WithTimeout(ctx, time.Second*3)
	defer cancelClose()
	if err := mainSrv.Shutdown(closeCtx); err != nil {
		log.Printf("Cannot shutdown server, err: %s\n", err)
		os.Exit(1)
	}
	log.Println("Server shutdown completed successfully")
}
