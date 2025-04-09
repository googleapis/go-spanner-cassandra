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

package samples

// [START spanner_cassandra_quick_start]
import (
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"time"

	spanner "github.com/googleapis/go-spanner-cassandra/cassandra/gocql"
)

// This sample assumes your spanner database <your_db> contains a table <users>
// with the following schema:
//
// CREATE TABLE users (
//	id   	 	INT64          OPTIONS (cassandra_type = 'int'),
//	active    	BOOL           OPTIONS (cassandra_type = 'boolean'),
//	username  	STRING(MAX)    OPTIONS (cassandra_type = 'text'),
// ) PRIMARY KEY (id);

func quickStart(databaseURI string, w io.Writer) error {
	opts := &spanner.Options{
		DatabaseUri: databaseURI,
	}
	cluster := spanner.NewCluster(opts)
	if cluster == nil {
		return fmt.Errorf("failed to create cluster")
	}
	defer spanner.CloseCluster(cluster)

	// You can still configure your cluster as usual after connecting to your
	// spanner database
	cluster.Timeout = 5 * time.Second
	cluster.Keyspace = "your_db_name"

	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	randomUserId := rand.IntN(math.MaxInt32)
	if err = session.Query("INSERT INTO users (id, active, username) VALUES (?, ?, ?)",
			       randomUserId, true, "John Doe").
		Exec(); err != nil {
		return err
	}

	var id int
	var active bool
	var username string
	if err = session.Query("SELECT id, active, username FROM users WHERE id = ?",
			       randomUserId).
		Scan(&id, &active, &username); err != nil {
		return err
	}
	fmt.Fprintf(w, "%d %v %s\n", id, active, username)
	return nil
}
// [END spanner_cassandra_quick_start]
