//go:build integration
// +build integration

/*
Copyright 2025 Google LLC

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

package spanner

import (
	"context"
	_ "embed"
	"flag"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	session *gocql.Session
)

//go:embed testdata/cassandra_schema.cql
var cassandraSchema string

// Sample command to run it test:
// go test -tags=integration  ./... -target=spanner
func TestMain(m *testing.M) {
	var target string
	flag.StringVar(
		&target,
		"target",
		"spanner",
		"Specify the test target: 'spanner' or 'cassandra'",
	)
	var spannerEndpoint string
	flag.StringVar(
		&spannerEndpoint,
		"spanner-endpoint",
		"spanner.googleapis.com:443",
		"Specify the spanner endpoint, default to spanner.googleapis.com:443",
	)
	flag.Parse()

	if target == "" {
		target = "spanner"
	}

	switch target {
	case "spanner":
		setupAndRunSpanner(m, spannerEndpoint)
	case "cassandra":
		setupAndRunCassandra(m)
	default:
		log.Fatalf("Invalid target - %s", target)
	}
}

func setupAndRunSpanner(m *testing.M, spannerEndpoint string) {
	// Create a context with a 5-minute timeout
	_, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	databaseUri := os.Getenv("INTEGRATION_TEST_DATABASE_URI")
	if databaseUri == "" {
		log.Fatalf("INTEGRATION_TEST_DATABASE_URI is not set")
	}
	opts := &Options{
		DatabaseUri:     databaseUri,
		SpannerEndpoint: spannerEndpoint,
	}
	cluster := NewCluster(opts)
	if cluster == nil {
		log.Fatalf("Failed to create cluster")
	}
	defer CloseCluster(cluster)
	defer cancel()

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Could not connect to Endpoint: %v", err)
	}
	// TODO: create database and table if not present

	// Run the tests
	code := m.Run()

	// Cleanup and exit
	session.Close()
	os.Exit(code)
}

func setupAndRunCassandra(m *testing.M) {
	// Create a context with a 5-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Define the container request
	req := testcontainers.ContainerRequest{
		Image:        "cassandra:latest",
		ExposedPorts: []string{"9042/tcp"}, // Expose the default Cassandra CQL port
		Env: map[string]string{
			"MAX_HEAP_SIZE": "512M", // Optional tuning for Cassandra
			"HEAP_NEWSIZE":  "100M",
		},
		WaitingFor: wait.ForLog("Starting listening for CQL clients on /0.0.0.0:9042").
			WithStartupTimeout(120 * time.Second),

		// Wait until Cassandra is ready
	}

	// Start the Cassandra container
	cassandraContainer, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		},
	)
	if err != nil {
		log.Fatalf("Could not start container: %v", err)
	}
	defer func() {
		// Ensure the container is terminated after tests
		if err := cassandraContainer.Terminate(ctx); err != nil {
			log.Fatalf("Error while terminating container - %v", err)
		}
	}()

	// Get the container host and mapped port for Cassandra
	host, err := cassandraContainer.Host(ctx)
	if err != nil {
		log.Fatalf("Could not get container host: %v", err)
	}

	mappedPort, err := cassandraContainer.MappedPort(ctx, "9042")
	if err != nil {
		log.Fatalf("Could not get mapped port: %v", err)
	}

	// Configure and create a new Cassandra cluster session
	cluster := gocql.NewCluster(host)
	cluster.Port = mappedPort.Int()           // Use the mapped port from the container
	cluster.ProtoVersion = 4                  // Use protocol version 4
	cluster.ConnectTimeout = 30 * time.Second // Set connection timeout
	// cluster.Keyspace = "system"               // Use the default 'system'
	// keyspace initially

	// Create a session with Cassandra
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("Could not connect to Cassandra: %v", err)
	}
	defer session.Close()

	//create Kespace
	ddlStatementKeyspace := "CREATE KEYSPACE IF NOT EXISTS it_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };"
	if err := session.Query(ddlStatementKeyspace).Exec(); err != nil {
		log.Fatalf("Could not create keyspace: %v", err)
	}

	ddlStatement := strings.ReplaceAll(cassandraSchema, "\n", " ")
	if err := session.Query(ddlStatement).Exec(); err != nil {
		log.Fatalf("Could not create table: %v", err)
	}

	// Run the tests
	code := m.Run()

	// Cleanup and exit
	session.Close()
	os.Exit(code)
}
