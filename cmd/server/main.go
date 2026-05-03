package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws-quota-simulator/internal/config"
	"github.com/aws-quota-simulator/internal/proxy"
	"github.com/aws-quota-simulator/internal/quota"
)

func main() {
	configPath := "configs/quotas.yaml"
	if p := os.Getenv("CONFIG_PATH"); p != "" {
		configPath = p
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Wait for upstream to be ready
	if delay := os.Getenv("STARTUP_DELAY"); delay != "" {
		if d, err := time.ParseDuration(delay + "s"); err == nil {
			log.Printf("Waiting %v for upstream to be ready...", d)
			time.Sleep(d)
		}
	}

	log.Printf("Starting AWS Quota Simulator on %s", cfg.Proxy.Address())
	log.Printf("Proxying to upstream: %s", cfg.Proxy.UpstreamURL)

	qm := quota.NewManager(cfg)

	handler, err := proxy.NewProxyHandler(cfg.Proxy.UpstreamURL, qm)
	if err != nil {
		log.Fatalf("Failed to create proxy handler: %v", err)
	}

	server := &http.Server{
		Addr:         cfg.Proxy.Address(),
		Handler:      handler,
		ReadTimeout:  cfg.Proxy.ReadTimeout,
		WriteTimeout: cfg.Proxy.WriteTimeout,
	}

	go func() {
		log.Printf("Server started at http://%s", cfg.Proxy.Address())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	if err := server.Close(); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

func init() {
	fmt.Print("   ___ _    ___   ___ ___ ___ \n  / __| |  / _ \\ / __| __| __|\n | (_ | |_| (_) | (__| _|| _| \n  \\___|___|\\___/ \\___|___|___|\n\n  AWS Quota Simulator\n  Proxy for simulating AWS quota limits\n\n")
}