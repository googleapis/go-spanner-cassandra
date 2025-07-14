/*
Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
This file provides a simple launcher for the Cassandra-to-Spanner proxy.
The launcher starts the proxy, allowing CQL clients (like cqlsh) to connect
to it as if it were a Cassandra database. Once started, the proxy listens for connections (default
localhost:9042) and remains active until a SIGINT or SIGTERM signal is received,
at which point it shuts down gracefully.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	spanner "github.com/googleapis/go-spanner-cassandra/cassandra/gocql"
	"github.com/googleapis/go-spanner-cassandra/logger"
	"go.uber.org/zap"
)

func main() {
	databaseURI := flag.String(
		"db",
		"",
		"The Spanner database URI (required)",
	)

	tcpEndpoint := flag.String(
		"tcp",
		":9042",
		"The Spanner Adapter proxy listner address. Default to :9042 to bind all network interfaces due to docker forwarding",
	)

	numGrpcChannels := flag.Int(
		"grpc-channels",
		4,
		"The number of channels when dial grpc connection. Default to 4.",
	)

	maxConcurrencyPerConn := flag.Int(
		"max-concurrency-per-conn",
		32768,
		"The number of channels when dial grpc connection. Default to 4.",
	)

	logLevel := flag.String(
		"log",
		"info",
		"Log level. Default to info.",
	)

	flag.Parse()

	if *databaseURI == "" {
		fmt.Println("Error: --db is required")
		flag.Usage()
		os.Exit(1)
	}

	opts := &spanner.Options{
		DatabaseUri:           *databaseURI,
		TCPEndpoint:           *tcpEndpoint,
		NumGrpcChannels:       *numGrpcChannels,
		LogLevel:              *logLevel,
		MaxConcurrencyPerConn: *maxConcurrencyPerConn,
	}

	cluster := spanner.NewCluster(opts)
	if cluster == nil {
		logger.Error("Failed to initialize Spanner Cassandra Adapter")
	}
	defer spanner.CloseCluster(cluster)

	logger.Info(
		"Spanner Cassandra Adapter created successfully",
		zap.String("connected database", *databaseURI),
	)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan

	logger.Info("Shutting down Spanner Cassandra Adapter...")
}
