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
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	session   *gocql.Session
	keyspace  = "it_test"
	tableName = "AllCqlConstantTypes"
)

//go:embed testdata/cassandra_schema.cql
var cassandraSchema string

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

	var err error
	session, err = cluster.CreateSession()
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

type CqlConstantTestData struct {
	PkInt      int32          `cql:"pk_int"`
	ColDouble  float64        `cql:"col_double"`
	ColBoolean bool           `cql:"col_boolean"`
	ColVarchar string         `cql:"col_varchar"`
	ColFloat   float32        `cql:"col_float"`
	ColUuid    gocql.UUID     `cql:"col_uuid"`
	ColBlob    []byte         `cql:"col_blob"`
	ColMap     map[string]int `cql:"col_map"`
	ColSet     []int          `cql:"col_set"`
	ColList    []bool         `cql:"col_list"`
}

func randomInt() int32 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int31()
}

func TestIntegration_AllConstantDataTypes(t *testing.T) {
	// --- Original Data ---
	testData := CqlConstantTestData{
		PkInt:      randomInt(),
		ColDouble:  12345.67890123,
		ColBoolean: false,
		ColVarchar: "Initial Varchar Привет",
		ColFloat:   111.222,
		ColUuid:    gocql.UUID(uuid.New()),
		ColBlob:    []byte{0x01, 0x02, 0xFA, 0xFB},
		ColMap:     map[string]int{"initial": 1, "start": 2},
		ColSet:     []int{100, 200, 300},
		ColList:    []bool{true, true, false},
	}
	// --- Data for Update ---
	updatedColVarchar := "Updated Varchar Value"
	updatedColBoolean := true // Flip the boolean
	updatedColMap := map[string]int{
		"start": 222,
		"added": 333,
	}
	updatedColDouble := 9999.8888

	// --- 1. Insert Operation ---
	t.Run("InsertRow", func(t *testing.T) {
		log.Printf("Attempting to insert row with key: %d", testData.PkInt)
		insertQuery := fmt.Sprintf(`INSERT INTO %s.%s (
            pk_int, col_double, col_boolean, col_varchar, col_float,
            col_uuid, col_blob, col_map, col_set, col_list
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, keyspace, tableName)

		err := session.Query(insertQuery,
			testData.PkInt,
			testData.ColDouble,
			testData.ColBoolean,
			testData.ColVarchar,
			testData.ColFloat,
			testData.ColUuid,
			testData.ColBlob,
			testData.ColMap,
			testData.ColSet,
			testData.ColList,
		).Exec()

		assert.NoError(
			t,
			err,
			fmt.Sprintf("Insert failed for key %d", testData.PkInt),
		)
		if err == nil {
			log.Printf("Successfully inserted row with key: %d", testData.PkInt)
		} else {
			t.FailNow()
		}
	})

	// --- 2. Update Operation ---
	t.Run("UpdateRow", func(t *testing.T) {
		log.Printf("Attempting to update row with key: %d", testData.PkInt)
		updateQuery := fmt.Sprintf(`UPDATE %s.%s SET
            col_varchar = ?,
            col_boolean = ?,
            col_map = ?,
            col_double = ?
            WHERE pk_int = ?`, keyspace, tableName)

		err := session.Query(updateQuery,
			updatedColVarchar,
			updatedColBoolean,
			updatedColMap,
			updatedColDouble,
			testData.PkInt, // WHERE clause
		).Exec()

		assert.NoError(
			t,
			err,
			fmt.Sprintf("Update failed for key %d", testData.PkInt),
		)
		if err == nil {
			log.Printf("Successfully updated row with key: %d", testData.PkInt)
		} else {
			t.FailNow()
		}
	})

	// --- 3. Select and Verify Operation ---
	t.Run("SelectAndVerifyRow", func(t *testing.T) {
		log.Printf(
			"Attempting to select and verify row with key: %d",
			testData.PkInt,
		)
		selectQuery := fmt.Sprintf(`SELECT
            pk_int, col_double, col_boolean, col_varchar, col_float,
            col_uuid, col_blob, col_map, col_set, col_list
            FROM %s.%s WHERE pk_int = ?`, keyspace, tableName)

		var selectedData CqlConstantTestData
		err := session.Query(selectQuery, testData.PkInt).Scan(
			&selectedData.PkInt,
			&selectedData.ColDouble,
			&selectedData.ColBoolean,
			&selectedData.ColVarchar,
			&selectedData.ColFloat,
			&selectedData.ColUuid,
			&selectedData.ColBlob,
			&selectedData.ColMap,
			&selectedData.ColSet,
			&selectedData.ColList,
		)

		assert.NoError(
			t,
			err,
			fmt.Sprintf("Select failed for key %d", testData.PkInt),
		)
		if err != nil {
			t.FailNow()
		}
		log.Printf(
			"Select successful for key %d, performing verification...",
			testData.PkInt,
		)

		// --- Verification ---
		// Create expected data based on insert + update
		expectedData := testData
		expectedData.ColVarchar = updatedColVarchar
		expectedData.ColBoolean = updatedColBoolean
		expectedData.ColMap = updatedColMap
		expectedData.ColDouble = updatedColDouble

		cmpOptions := []cmp.Option{
			// Allow some tolerance for comparing float
			cmp.Comparer(func(x, y float32) bool {
				delta := float32(0.0001)
				return math.Abs(float64(x-y)) <= float64(delta)
			}),
		}
		diff := cmp.Diff(expectedData, selectedData, cmpOptions...)
		assert.Empty(t, diff, "Data mismatch (-expected +got):\n%s", diff)
		if diff == "" {
			log.Printf(
				"Verification successful for key %d using go-cmp",
				testData.PkInt,
			)
		}
	})

	// --- 4. Delete Operation ---
	t.Run("DeleteRow", func(t *testing.T) {
		log.Printf("Attempting to delete row with key: %d", testData.PkInt)
		deleteQuery := fmt.Sprintf(
			"DELETE FROM %s.%s WHERE pk_int = ?",
			keyspace,
			tableName,
		)

		err := session.Query(deleteQuery, testData.PkInt).Exec()
		assert.NoError(
			t,
			err,
			fmt.Sprintf("Delete failed for key %d", testData.PkInt),
		)
		if err == nil {
			log.Printf("Successfully deleted row with key: %d", testData.PkInt)
		}
	})
}
