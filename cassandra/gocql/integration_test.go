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
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"gopkg.in/inf.v0"
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

func assertEqual(
	t *testing.T,
	description string,
	expected, actual interface{},
) {
	t.Helper()
	if expected != actual {
		t.Fatalf(
			"expected %s to be (%+v) but was (%+v) instead",
			description,
			expected,
			actual,
		)
	}
}

func assertDeepEqual(
	t *testing.T,
	description string,
	expected, actual interface{},
) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf(
			"expected %s to be (%+v) but was (%+v) instead",
			description,
			expected,
			actual,
		)
	}
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
		LogLevel:        "warn",
	}

	cluster = NewCluster(opts)
	if cluster == nil {
		log.Fatalf("Failed to create cluster")
	}
	cluster.NumConns = 50
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
	// Run the tests
	return m.Run()
}

func createCqlTable(t *testing.T, s *gocql.Session, table string) {
	if err := s.Query(table).RetryPolicy(&gocql.SimpleRetryPolicy{}).Exec(); err != nil {
		t.Fatalf("error creating cassandra table=%q err=%v\n", table, err)
	}
	return
}

func createSpannerTable(t *testing.T, table string) {
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
		t.Fatalf("error creating spanner table=%q err=%v\n", table, err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("error creating spanner table=%q err=%v\n", table, err)
	}
	return
}

func createSession(
	t *testing.T,
	opts ...func(config *gocql.ClusterConfig),
) *gocql.Session {
	// make a copy to avoid changing global *gocql.ClusterConfig
	localCluster := *cluster
	localCluster.Keyspace = keyspace
	for _, opt := range opts {
		opt(&localCluster)
	}
	session, err := localCluster.CreateSession()
	if err != nil {
		t.Fatalf("Failed to create cql session: %v", err)
	}
	return session
}

func TestSmallInt(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE smallint_table 
		  (id INT64 NOT NULL OPTIONS (cassandra_type = 'smallint'))
			PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE 
			smallint_table (id smallint primary key)`)
	}

	m := make(map[string]interface{})
	m["id"] = int16(2)
	sliceMap := []map[string]interface{}{m}
	if err := session.Query(`INSERT INTO smallint_table (id) VALUES (?)`,
		m["id"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if returned, retErr := session.Query(`SELECT * FROM smallint_table`).Iter().SliceMap(); retErr != nil {
		t.Fatal("select:", retErr)
	} else {
		if sliceMap[0]["id"] != returned[0]["id"] {
			t.Fatal("returned id did not match")
		}
	}
}

func TestScanWithNilArguments(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE scan_with_nil_arguments (
			foo STRING(MAX) OPTIONS (cassandra_type = 'varchar'),
			bar INT64 OPTIONS (cassandra_type = 'int'),) 
			PRIMARY KEY (foo, bar)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE scan_with_nil_arguments (
			foo   varchar,
			bar   int,
			PRIMARY KEY (foo, bar)
		)`)
	}

	for i := 1; i <= 20; i++ {
		if err := session.Query("INSERT INTO scan_with_nil_arguments (foo, bar) VALUES (?, ?)",
			"squares", i*i).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}

	iter := session.Query("SELECT * FROM scan_with_nil_arguments WHERE foo = ?", "squares").
		Iter()
	var n int
	count := 0
	for iter.Scan(nil, &n) {
		count += n
	}
	if err := iter.Close(); err != nil {
		t.Fatal("close:", err)
	}
	if count != 2870 {
		t.Fatalf("expected %d, got %d", 2870, count)
	}
}

func TestRebindQueryInfo(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE rebind_query (
			id INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
			value STRING(MAX) NOT NULL OPTIONS (cassandra_type = 'text'),
			) 
			PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE rebind_query 
			(id int, value text, 
			PRIMARY KEY (id))`)
	}

	if err := session.Query("INSERT INTO rebind_query (id, value) VALUES (?, ?)", 23, "quux").Exec(); err != nil {
		t.Fatalf("insert into rebind_query failed, err '%v'", err)
	}

	if err := session.Query("INSERT INTO rebind_query (id, value) VALUES (?, ?)", 24, "w00t").Exec(); err != nil {
		t.Fatalf("insert into rebind_query failed, err '%v'", err)
	}

	q := session.Query("SELECT value FROM rebind_query WHERE ID = ?")
	q.Bind(23)

	iter := q.Iter()
	var value string
	for iter.Scan(&value) {
	}

	if value != "quux" {
		t.Fatalf("expected %v but got %v", "quux", value)
	}

	q.Bind(24)
	iter = q.Iter()

	for iter.Scan(&value) {
	}

	if value != "w00t" {
		t.Fatalf("expected %v but got %v", "w00t", value)
	}
}

func TestVarint(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE varint_test (
			id STRING(MAX) NOT NULL OPTIONS (cassandra_type = 'varchar'),
			test NUMERIC OPTIONS (cassandra_type = 'varint'),
			test2 NUMERIC OPTIONS (cassandra_type = 'varint'),
			) 
			PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE 
			varint_test (id varchar, test varint, test2 varint, 
			primary key (id))`)
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", 0).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	var result int
	if err := session.Query("SELECT test FROM varint_test").Scan(&result); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result != 0 {
		t.Errorf("Expected 0, was %d", result)
	}

	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", nil).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	if err := session.Query("SELECT test FROM varint_test").Scan(&result); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if result != 0 {
		t.Errorf("Expected 0, was %d", result)
	}

	var nullableResult *int

	if err := session.Query("SELECT test FROM varint_test").Scan(&nullableResult); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if nullableResult != nil {
		t.Errorf("Expected nil, was %d", nullableResult)
	}

	biggie := new(big.Int)
	biggie.SetString("36893488147419103232", 10) // > 2**64
	if err := session.Query(`INSERT INTO varint_test (id, test) VALUES (?, ?)`, "id", biggie).Exec(); err != nil {
		t.Fatalf("insert varint: %v", err)
	}

	resultBig := new(big.Int)
	if err := session.Query("SELECT test FROM varint_test").Scan(resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig.String() != biggie.String() {
		t.Errorf("Expected %s, was %s", biggie.String(), resultBig.String())
	}

	// value not set in cassandra, leave bind variable empty
	resultBig = new(big.Int)
	if err := session.Query("SELECT test2 FROM varint_test").Scan(resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig.Int64() != 0 {
		t.Errorf("Expected %s, was %s", biggie.String(), resultBig.String())
	}

	// can use double pointer to explicitly detect value is not set in cassandra
	if err := session.Query("SELECT test2 FROM varint_test").Scan(&resultBig); err != nil {
		t.Fatalf("select from varint_test failed: %v", err)
	}

	if resultBig != nil {
		t.Errorf("Expected %v, was %v", nil, *resultBig)
	}
}

func TestNilInQuery(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE testNilInsert (
			id INT64 OPTIONS (cassandra_type = 'int'),
			count_col INT64 OPTIONS (cassandra_type = 'int'),
			) 
			PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE testNilInsert 
			(id int, count_col int, 
			PRIMARY KEY (id))`)
	}

	if err := session.Query("INSERT INTO testNilInsert (id,count_col) VALUES (?,?)", 1, nil).Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	var id int

	if err := session.Query("SELECT id FROM testNilInsert").Scan(&id); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	} else if id != 1 {
		t.Fatalf("expected id to be 1, got %v", id)
	}
}

func TestEmptyTimestamp(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE test_empty_timestamp (
			id INT64 OPTIONS (cassandra_type = 'int'),
			time TIMESTAMP OPTIONS (cassandra_type = 'timestamp'),
			num INT64 OPTIONS (cassandra_type = 'int'),
			) 
			PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE test_empty_timestamp 
			(id int, time timestamp, num int, 
			PRIMARY KEY (id))`)
	}

	if err := session.Query("INSERT INTO test_empty_timestamp (id, num) VALUES (?,?)", 1, 561).Exec(); err != nil {
		t.Fatalf("failed to insert with err: %v", err)
	}

	var timeVal time.Time

	if err := session.Query("SELECT time FROM test_empty_timestamp where id = ?", 1).Scan(&timeVal); err != nil {
		t.Fatalf("failed to select with err: %v", err)
	}

	if !timeVal.IsZero() {
		t.Errorf("time.Time bind variable should still be empty (was %s)", timeVal)
	}
}

func TestLargeSizeQuery(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE large_size_query (
			id INT64 OPTIONS (cassandra_type = 'int'),
			text_col STRING(MAX) OPTIONS (cassandra_type = 'varchar'),
			) 
			PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE IF NOT EXISTS large_size_query(
			id int, text_col text, 
			PRIMARY KEY (id))`)
	}

	longString := strings.Repeat("a", 500_000)

	err := session.Query("INSERT INTO large_size_query (id, text_col) VALUES (?, ?)", "1", longString).
		Exec()
	if err != nil {
		t.Fatal(err)
	}

	var result string
	err = session.Query("SELECT text_col FROM large_size_query").Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, longString, result)
}

// TestUseStatementError checks to make sure the correct error is returned when
// the user tries to execute a use statement.
func TestUseStatementError(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if err := session.Query("USE gocql_test").Exec(); err != nil {
		if !strings.HasPrefix(err.Error(), "use statements aren't supported.") {
			t.Fatalf("expected 'use statements aren't supported', got " + err.Error())
		}
	} else {
		t.Fatal("expected err, got nil.")
	}
}

type funcQueryObserver func(context.Context, gocql.ObservedQuery)

func (f funcQueryObserver) ObserveQuery(
	ctx context.Context,
	o gocql.ObservedQuery,
) {
	f(ctx, o)
}

func TestObserve(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE observe (
			 id INT64 NOT NULL OPTIONS (cassandra_type = 'int'),) 
			 PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE observe (id int primary key)`)
	}

	var (
		observedErr      error
		observedKeyspace string
		observedStmt     string
	)

	resetObserved := func() {
		observedErr = errors.New(
			"placeholder only",
		) // used to distinguish err=nil cases
		observedKeyspace = ""
		observedStmt = ""
	}

	observer := funcQueryObserver(
		func(ctx context.Context, o gocql.ObservedQuery) {
			observedKeyspace = o.Keyspace
			observedStmt = o.Statement
			observedErr = o.Err
		},
	)

	// select before inserted, will error but the reporting is err=nil as the
	// query is valid
	resetObserved()
	var value int
	if err := session.Query(`SELECT id FROM observe WHERE id = ?`, 43).Observer(observer).Scan(&value); err == nil {
		t.Fatal("select: expected error")
	} else if observedErr != nil {
		t.Fatalf("select: observed error expected nil, got %q", observedErr)
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM observe WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}

	resetObserved()
	if err := session.Query(`INSERT INTO observe (id) VALUES (?)`, 42).Observer(observer).Exec(); err != nil {
		t.Fatal("insert:", err)
	} else if observedErr != nil {
		t.Fatal("insert:", observedErr)
	} else if observedKeyspace != keyspace {
		t.Fatal("insert: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `INSERT INTO observe (id) VALUES (?)` {
		t.Fatal("insert: unexpected observed stmt", observedStmt)
	}

	resetObserved()
	value = 0
	if err := session.Query(`SELECT id FROM observe WHERE id = ?`, 42).Observer(observer).Scan(&value); err != nil {
		t.Fatal("select:", err)
	} else if value != 42 {
		t.Fatalf("value: expected %d, got %d", 42, value)
	} else if observedErr != nil {
		t.Fatal("select:", observedErr)
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM observe WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}

	// also works from session observer
	resetObserved()
	oSession := createSession(
		t,
		func(config *gocql.ClusterConfig) { config.QueryObserver = observer },
	)
	defer oSession.Close()
	if err := oSession.Query(`SELECT id FROM observe WHERE id = ?`, 42).Scan(&value); err != nil {
		t.Fatal("select:", err)
	} else if observedErr != nil {
		t.Fatal("select:", observedErr)
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM observe WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}

	// reports errors when the query is poorly formed
	resetObserved()
	value = 0
	if err := session.Query(`SELECT id FROM unknown_table WHERE id = ?`, 42).Observer(observer).Scan(&value); err == nil {
		t.Fatal("select: expecting error")
	} else if observedErr == nil {
		t.Fatal("select: expecting observed error")
	} else if observedKeyspace != keyspace {
		t.Fatal("select: unexpected observed keyspace", observedKeyspace)
	} else if observedStmt != `SELECT id FROM unknown_table WHERE id = ?` {
		t.Fatal("select: unexpected observed stmt", observedStmt)
	}
}

func TestBatch(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE batch_table 
		(id INT64 NOT NULL OPTIONS (cassandra_type = 'int'))
		PRIMARY KEY (id)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE batch_table 
			(id int primary key)`)
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

// TestBatchLimit tests gocql to make sure batch operations larger than the
// maximum
// statement limit are not submitted to a cassandra node.
func TestBatchLimit(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	var batchLimit int
	if env == "spanner" {
		batchLimit = 1001
		createSpannerTable(t, `CREATE TABLE batch_table2 (
			id INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
			) PRIMARY KEY (id)`)
	} else {
		batchLimit = 65537
		createCqlTable(t, session, `CREATE TABLE batch_table2 
			(id int primary key)`)
	}

	batch := session.NewBatch(gocql.LoggedBatch)
	for i := 0; i < batchLimit; i++ {
		batch.Query(`INSERT INTO batch_table2 (id) VALUES (?)`, i)
	}
	if err := session.ExecuteBatch(batch); err == nil {
		t.Fatal("expected too many statements error, got nil")
	}
}

func TestWhereIn(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(
			t,
			`CREATE TABLE where_in_table (
			id INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
			cluster INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
			)PRIMARY KEY (id)`,
		)
	} else {
		createCqlTable(t, session, `CREATE TABLE where_in_table (
			id int, cluster int, 
			primary key (id,cluster))`)
	}

	if err := session.Query("INSERT INTO where_in_table (id, cluster) VALUES (?,?)", 100, 200).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	iter := session.Query("SELECT * FROM where_in_table WHERE id = ? AND cluster IN (?)", 100, 200).
		Iter()
	var id, cluster int
	count := 0
	for iter.Scan(&id, &cluster) {
		count++
	}

	if id != 100 || cluster != 200 {
		t.Fatalf(
			"Was expecting id and cluster to be (100,200) but were (%d,%d)",
			id,
			cluster,
		)
	}
}

// TestTooManyQueryArgs tests to make sure the library correctly handles the
// application level bug
// whereby too many query arguments are passed to a query
func TestTooManyQueryArgs(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(
			t,
			`CREATE TABLE too_many_query_args (
				id INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
				value INT64 OPTIONS (cassandra_type = 'int')
			) PRIMARY KEY (id)`,
		)
	} else {
		createCqlTable(t, session, `CREATE TABLE too_many_query_args (
			id int primary key, 
			value int
		)`)
	}

	_, err := session.Query(`SELECT * FROM too_many_query_args WHERE id = ?`, 1, 2).
		Iter().
		SliceMap()

	if err == nil {
		t.Fatal(
			"'`SELECT * FROM too_many_query_args WHERE id = ?`, 1, 2' should return an error",
		)
	}

	batch := session.NewBatch(gocql.UnloggedBatch)
	batch.Query(
		"INSERT INTO too_many_query_args (id, value) VALUES (?, ?)",
		1,
		2,
		3,
	)
	err = session.ExecuteBatch(batch)

	if err == nil {
		t.Fatal(
			"'`INSERT INTO too_many_query_args (id, value) VALUES (?, ?)`, 1, 2, 3' should return an error",
		)
	}
}

// TestNotEnoughQueryArgs tests to make sure the library correctly handles the
// application level bug
// whereby not enough query arguments are passed to a query
func TestNotEnoughQueryArgs(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE not_enough_query_args (
				id INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
				cluster INT64 NOT NULL OPTIONS (cassandra_type = 'int'),
				value INT64 OPTIONS (cassandra_type = 'int')
			) PRIMARY KEY (id, cluster)`,
		)
	} else {
		createCqlTable(t, session, `CREATE TABLE not_enough_query_args (
			id int, 
			cluster int, 
			value int, 
			primary key (id, cluster)
		)`)
	}

	_, err := session.Query(`SELECT * FROM not_enough_query_args WHERE id = ? and cluster = ?`, 1).
		Iter().
		SliceMap()

	if err == nil {
		t.Fatal(
			"'`SELECT * FROM not_enough_query_args WHERE id = ? and cluster = ?`, 1' should return an error",
		)
	}

	batch := session.NewBatch(gocql.UnloggedBatch)
	batch.Query(
		"INSERT INTO not_enough_query_args (id, cluster, value) VALUES (?, ?, ?)",
		1,
		2,
	)
	err = session.ExecuteBatch(batch)

	if err == nil {
		t.Fatal(
			"'`INSERT INTO not_enough_query_args (id, cluster, value) VALUES (?, ?, ?)`, 1, 2' should return an error",
		)
	}
}

// TestCreateSessionTimeout tests to make sure the CreateSession function
// timeouts out correctly
// and prevents an infinite loop of connection retries.
func TestCreateSessionTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-time.After(2 * time.Second):
			t.Error("no startup timeout")
		case <-ctx.Done():
		}
	}()

	localCluster := *cluster
	localCluster.Hosts = []string{"127.0.0.1:1"}
	session, err := localCluster.CreateSession()
	if err == nil {
		session.Close()
		t.Fatal("expected ErrNoConnectionsStarted, but no error was returned.")
	}
}

type FullName struct {
	FirstName string
	LastName  string
}

func (n FullName) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return []byte(n.FirstName + " " + n.LastName), nil
}

func (n *FullName) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	t := strings.SplitN(string(data), " ", 2)
	n.FirstName, n.LastName = t[0], t[1]
	return nil
}

func TestMapScanWithRefMap(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE scan_map_ref_table (
				testtext STRING(MAX) NOT NULL OPTIONS (cassandra_type = 'text'),
				testfullname STRING(MAX) OPTIONS (cassandra_type = 'text'),
				testint INT64 OPTIONS (cassandra_type = 'int')
			) PRIMARY KEY (testtext)`,
		)
	} else {
		createCqlTable(t, session, `CREATE TABLE scan_map_ref_table (
			testtext text PRIMARY KEY,
			testfullname text,
			testint int
		)`)
	}

	m := make(map[string]interface{})
	m["testtext"] = "testtext"
	m["testfullname"] = FullName{"John", "Doe"} // Assuming FullName is defined
	m["testint"] = 100

	if err := session.Query(`INSERT INTO scan_map_ref_table (testtext, testfullname, testint) values (?,?,?)`,
		m["testtext"], m["testfullname"], m["testint"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	var testText string
	var testFullName FullName // Assuming FullName is defined
	ret := map[string]interface{}{
		"testtext":     &testText,
		"testfullname": &testFullName,
		// testint is not set here.
	}
	iter := session.Query(`SELECT * FROM scan_map_ref_table`).Iter()
	if ok := iter.MapScan(ret); !ok {
		t.Fatal("select:", iter.Close())
	} else {
		if ret["testtext"] != "testtext" {
			t.Fatal("returned testtext did not match")
		}
		f := ret["testfullname"].(FullName)
		if f.FirstName != "John" || f.LastName != "Doe" {
			t.Fatal("returned testfullname did not match")
		}
		if ret["testint"] != 100 {
			t.Fatal("returned testint did not match") // Corrected typo from testinit
		}
	}
	if testText != "testtext" {
		t.Fatal("returned testtext did not match")
	}
	if testFullName.FirstName != "John" || testFullName.LastName != "Doe" {
		t.Fatal("returned testfullname did not match")
	}

	// using MapScan to read a nil int value
	intp := new(int64)
	ret = map[string]interface{}{ // ret is re-assigned
		"testint": &intp,
	}
	if err := session.Query("INSERT INTO scan_map_ref_table(testtext, testint) VALUES(?, ?)", "null-int", nil).Exec(); err != nil {
		t.Fatal(err)
	}

	err := session.Query(`SELECT testint FROM scan_map_ref_table WHERE testtext = ?`, "null-int").
		MapScan(ret)
	if err != nil {
		t.Fatal(err)
	} else {
		v := intp // Corrected to check the variable 'intp' that MapScan populates.
		if v != nil {
			t.Fatalf("testint should be nil got %+#v", v) // Corrected error message
		}
	}
}

func TestMapScan(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE scan_map_table (
				fullname STRING(MAX) NOT NULL OPTIONS (cassandra_type = 'text'),
				age INT64 OPTIONS (cassandra_type = 'int'),
				address STRING(MAX) OPTIONS (cassandra_type = 'inet'),
				data BYTES(MAX) OPTIONS (cassandra_type = 'blob'),
			) PRIMARY KEY (fullname)`,
		)
	} else {
		createCqlTable(t, session, `CREATE TABLE scan_map_table (
			fullname text PRIMARY KEY,
			age int,
			address inet,
			data blob
		)`)
	}

	if err := session.Query(`INSERT INTO scan_map_table (fullname, age, address) values (?,?,?)`,
		"Grace Hopper", 31, net.ParseIP("10.0.0.1")).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if err := session.Query(`INSERT INTO scan_map_table (fullname, age, address, data) values (?,?,?,?)`,
		"Ada Lovelace", 30, net.ParseIP("10.0.0.2"), []byte(`{"foo": "bar"}`)).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	iter := session.Query(`SELECT * FROM scan_map_table`).Iter()

	// First iteration
	row := make(map[string]interface{})
	if !iter.MapScan(
		row,
	) { // Preserving original boolean check, assuming specific testbed behavior
		t.Fatal("select:", iter.Close())
	}
	assertEqual(t, "fullname", "Ada Lovelace", row["fullname"])
	assertEqual(t, "age", 30, row["age"])
	assertEqual(
		t,
		"address",
		"10.0.0.2",
		row["address"],
	) // Assuming assertEqual handles net.IP comparison
	assertDeepEqual(t, "data", []byte(`{"foo": "bar"}`), row["data"])

	// Second iteration using a new map
	row = make(map[string]interface{})
	if !iter.MapScan(row) { // Preserving original boolean check
		t.Fatal("select:", iter.Close())
	}
	assertEqual(t, "fullname", "Grace Hopper", row["fullname"])
	assertEqual(t, "age", 31, row["age"])
	assertEqual(
		t,
		"address",
		"10.0.0.1",
		row["address"],
	) // Assuming assertEqual handles net.IP comparison
	assertDeepEqual(
		t,
		"data",
		[]byte{},
		row["data"],
	)
}

func matchSliceMap(
	t *testing.T,
	sliceMap []map[string]interface{},
	testMap map[string]interface{},
) {
	if sliceMap[0]["testuuid"] != testMap["testuuid"] {
		t.Fatal("returned testuuid did not match")
	}
	if sliceMap[0]["testtimestamp"] != testMap["testtimestamp"] {
		t.Fatal("returned testtimestamp did not match")
	}
	if sliceMap[0]["testvarchar"] != testMap["testvarchar"] {
		t.Fatal("returned testvarchar did not match")
	}
	if sliceMap[0]["testbigint"] != testMap["testbigint"] {
		t.Fatal("returned testbigint did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testblob"], testMap["testblob"]) {
		t.Fatal("returned testblob did not match")
	}
	if sliceMap[0]["testbool"] != testMap["testbool"] {
		t.Fatal("returned testbool did not match")
	}
	if sliceMap[0]["testfloat"] != testMap["testfloat"] {
		t.Fatal("returned testfloat did not match")
	}
	if sliceMap[0]["testdouble"] != testMap["testdouble"] {
		t.Fatal("returned testdouble did not match")
	}
	if sliceMap[0]["testinet"] != testMap["testinet"] {
		t.Fatal("returned testinet did not match")
	}

	expectedDecimal := sliceMap[0]["testdecimal"].(*inf.Dec)
	returnedDecimal := testMap["testdecimal"].(*inf.Dec)

	if expectedDecimal.Cmp(returnedDecimal) != 0 {
		t.Fatal("returned testdecimal did not match")
	}

	if !reflect.DeepEqual(sliceMap[0]["testlist"], testMap["testlist"]) {
		t.Fatal("returned testlist did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testset"], testMap["testset"]) {
		t.Fatal("returned testset did not match")
	}
	if !reflect.DeepEqual(sliceMap[0]["testmap"], testMap["testmap"]) {
		t.Fatal("returned testmap did not match")
	}
	if sliceMap[0]["testint"] != testMap["testint"] {
		t.Fatal("returned testint did not match")
	}
}

func TestSliceMap(t *testing.T) {
	session := createSession(t)
	defer session.Close()
	if env == "spanner" {
		createSpannerTable(t, `CREATE TABLE slice_map_table (
			testuuid STRING(36) NOT NULL OPTIONS (cassandra_type = 'timeuuid'),
			testtimestamp TIMESTAMP OPTIONS (cassandra_type = 'timestamp'),
			testvarchar STRING(MAX) OPTIONS (cassandra_type = 'varchar'),
			testbigint INT64 OPTIONS (cassandra_type = 'bigint'),
			testblob BYTES(MAX) OPTIONS (cassandra_type = 'blob'),
			testbool BOOL OPTIONS (cassandra_type = 'boolean'),
			testfloat FLOAT32 OPTIONS (cassandra_type = 'float'),
			testdouble FLOAT64 OPTIONS (cassandra_type = 'double'),
			testint INT64 OPTIONS (cassandra_type = 'int'),
			testdecimal NUMERIC OPTIONS (cassandra_type = 'decimal'),
			testlist ARRAY<STRING(MAX)> OPTIONS (cassandra_type = 'list<text>'),
			testset ARRAY<INT64> OPTIONS (cassandra_type = 'set<int>'),
			testmap JSON OPTIONS (cassandra_type = 'map<varchar,varchar>'),
			testvarint NUMERIC OPTIONS (cassandra_type = 'varint'),
			testinet STRING(MAX) OPTIONS (cassandra_type = 'inet')
			) PRIMARY KEY (testuuid)`)
	} else {
		createCqlTable(t, session, `CREATE TABLE slice_map_table (
			testuuid timeuuid PRIMARY KEY,
			testtimestamp timestamp,
			testvarchar varchar,
			testbigint bigint,
			testblob blob,
			testbool boolean,
			testfloat float,
			testdouble double,
			testint int,
			testdecimal decimal,
			testlist list<text>,
			testset set<int>,
			testmap map<varchar, varchar>,
			testvarint varint,
			testinet inet
		)`)
	}

	m := make(map[string]interface{})

	bigInt := new(big.Int)

	// original big int value is larger but this would overflow NUMERIC type in
	// googlesql when running against spanner.
	// if _, ok := bigInt.SetString("830169365738487321165427203929228",
	// 10); !ok {
	// 	t.Fatal("Failed setting bigint by string")
	// }
	if _, ok := bigInt.SetString("83016936573848732116542720392", 10); !ok {
		t.Fatal("Failed setting bigint by string")
	}

	m["testuuid"] = gocql.TimeUUID()
	m["testvarchar"] = "Test VarChar"
	m["testbigint"] = time.Now().Unix()
	m["testtimestamp"] = time.Now().Truncate(time.Millisecond).UTC()
	m["testblob"] = []byte("test blob")
	m["testbool"] = true
	m["testfloat"] = float32(4.564)
	m["testdouble"] = float64(4.815162342)
	m["testint"] = 2343
	m["testdecimal"] = inf.NewDec(100, 0)
	m["testlist"] = []string{"quux", "foo", "bar", "baz", "quux"}
	m["testset"] = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	m["testmap"] = map[string]string{
		"field1": "val1",
		"field2": "val2",
		"field3": "val3",
	}
	m["testvarint"] = bigInt
	m["testinet"] = "213.212.2.19"
	sliceMap := []map[string]interface{}{m}
	if err := session.Query(`INSERT INTO slice_map_table (testuuid, testtimestamp, testvarchar, testbigint, testblob, testbool, testfloat, testdouble, testint, testdecimal, testlist, testset, testmap, testvarint, testinet) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m["testuuid"], m["testtimestamp"], m["testvarchar"], m["testbigint"], m["testblob"], m["testbool"], m["testfloat"], m["testdouble"], m["testint"], m["testdecimal"], m["testlist"], m["testset"], m["testmap"], m["testvarint"], m["testinet"]).Exec(); err != nil {
		t.Fatal("insert:", err)
	}
	if returned, retErr := session.Query(`SELECT * FROM slice_map_table`).Iter().SliceMap(); retErr != nil {
		t.Fatal("select:", retErr)
	} else {
		matchSliceMap(t, sliceMap, returned[0])
	}

	// Test for Iter.MapScan()
	{
		testMap := make(map[string]interface{})
		if !session.Query(`SELECT * FROM slice_map_table`).Iter().MapScan(testMap) {
			t.Fatal("MapScan failed to work with one row")
		}
		matchSliceMap(t, sliceMap, testMap)
	}

	// Test for Query.MapScan()
	{
		testMap := make(map[string]interface{})
		if session.Query(`SELECT * FROM slice_map_table`).MapScan(testMap) != nil {
			t.Fatal("MapScan failed to work with one row")
		}
		matchSliceMap(t, sliceMap, testMap)
	}
}
