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
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/gocql/gocql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

var (
	cluster     *gocql.ClusterConfig
	keyspace    = "it_test"
	tableName   = "AllCqlConstantTypes"
	databaseUri = ""
	env         = "spanner"
)

var adminClient *database.DatabaseAdminClient

// Sample command to run it test:
// go test -v  ./... -tags=integration
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
		env = "spanner"
	} else {
		env = target
	}
	var err error
	keyspace, err = generateKeySpaceName()
	if err != nil {
		log.Fatalf(err.Error())
	}

	switch target {
	case "spanner":
		code := setupAndRunSpanner(m, spannerEndpoint)
		os.Exit(code)
	case "cassandra":
		code := setupAndRunCassandra(m)
		os.Exit(code)
	default:
		log.Fatalf("Invalid target - %s", target)
	}
}

func generateKeySpaceName() (string, error) {
	const (
		keyspacePrefix = "gocql_test_"
		// Go's reference time: Mon Jan 2 15:04:05 MST 2006
		// Format YYYYMMDDHHMMSS
		timestampFormat = "20060102150405"
	)

	currentTime := time.Now()
	timestampStr := currentTime.Format(timestampFormat)
	keyspace := fmt.Sprintf("%s%s", keyspacePrefix, timestampStr)
	return keyspace, nil
}

func setupAndRunSpanner(m *testing.M, spannerEndpoint string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	instanceURI := os.Getenv("INTEGRATION_TEST_INSTANCE")
	if instanceURI == "" {
		log.Fatalf(
			"environment variable INTEGRATION_TEST_INSTANCE is not set or is empty",
		)
	}
	databaseUri = fmt.Sprintf("%s/databases/%s", instanceURI, keyspace)
	var err error
	adminClient, err = database.NewDatabaseAdminClient(ctx, []option.ClientOption{
		option.WithEndpoint(spannerEndpoint),
	}...)
	if err != nil {
		log.Fatalf(err.Error())
	}
	// Create a new random testing database
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          instanceURI,
		CreateStatement: "CREATE DATABASE `" + keyspace + "`",
	})
	if err != nil {
		log.Fatalf(err.Error())
	}
	if _, err := op.Wait(ctx); err != nil {
		log.Fatalf(err.Error())
	}
	// Drop testing database
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(
			context.Background(),
			2*time.Minute,
		)
		defer dropCancel()
		if err := adminClient.DropDatabase(dropCtx,
			&adminpb.DropDatabaseRequest{Database: databaseUri}); err != nil {
			log.Fatalf("failed to drop testing database %v: %v", databaseUri, err)
		}
		defer adminClient.Close()
	}()

	opts := &Options{
		DatabaseUri:     databaseUri,
		SpannerEndpoint: spannerEndpoint,
	}

	cluster = NewCluster(opts)
	if cluster == nil {
		log.Fatalf("Failed to create cluster")
	}
	cluster.Keyspace = keyspace
	defer CloseCluster(cluster)

	// Run the tests
	return m.Run()
}

func setupAndRunCassandra(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
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
	cluster = gocql.NewCluster(host)
	cluster.Port = mappedPort.Int()           // Use the mapped port from the container
	cluster.ProtoVersion = 4                  // Use protocol version 4
	cluster.ConnectTimeout = 30 * time.Second // Set connection timeout

	// Create a session with Cassandra
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Could not connect to Cassandra: %v", err)
	}
	defer session.Close()
	//create Kespace
	ddlStatementKeyspace := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }",
		keyspace,
	)
	if err := session.Query(ddlStatementKeyspace).Exec(); err != nil {
		log.Fatalf("Could not create keyspace: %v", err)
	}
	cluster.Keyspace = keyspace
	// Run the tests
	return m.Run()
}

func createCqlTable(s *gocql.Session, table string) error {
	if err := s.Query(table).RetryPolicy(&gocql.SimpleRetryPolicy{}).Exec(); err != nil {
		log.Printf("error creating table table=%q err=%v\n", table, err)
		return err
	}
	return nil
}

func createSpannerTable(table string) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Minute,
	)
	defer cancel()
	op, err := adminClient.UpdateDatabaseDdl(
		ctx,
		&adminpb.UpdateDatabaseDdlRequest{
			Database: databaseUri,
			Statements: []string{
				table,
			},
		},
	)
	if err != nil {
		return err
	}
	if err := op.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func TestBatch(t *testing.T) {
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to create cql session: %v", err)
	}
	defer session.Close()

	if env == "spanner" {
		if err := createSpannerTable(`CREATE TABLE batch_table (id INT64 NOT NULL OPTIONS (cassandra_type = 'int'))PRIMARY KEY (id)`); err != nil {
			t.Fatal("create spanner table:", err)
		}
	} else {
		if err := createCqlTable(session, `CREATE TABLE batch_table (id int primary key)`); err != nil {
			t.Fatal("create cassandra table:", err)
		}
	}

	batch := session.NewBatch(gocql.LoggedBatch)
	for i := 0; i < 10; i++ {
		batch.Query(`INSERT INTO batch_table (id) VALUES (?)`, i)
	}

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatal("execute batch:", err)
	}

	count := 0
	if err := session.Query(`SELECT COUNT(*) FROM batch_table`).Scan(&count); err != nil {
		t.Fatal("select count:", err)
	} else if count != 10 {
		t.Fatalf("count: expected %d, got %d\n", 100, count)
	}
}
