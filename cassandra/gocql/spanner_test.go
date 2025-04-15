//go:build unit
// +build unit

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
	"fmt"
	"net"
	"testing"

	"github.com/googleapis/go-spanner-cassandra/adapter"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sample command to run all unit tests
// go test -tags=unit ./...

func setupCluster(
	t *testing.T,
	returnResponsesInChunks bool,
) (*gocql.ClusterConfig, *gocql.Session) {
	t.Cleanup(adapter.ResetGrpcFuncs())
	adapter.MockCreateSessionGrpc()
	adapter.MockAdaptMessageGrpc(returnResponsesInChunks)
	opts := &Options{
		DatabaseUri: "projects/test/instances/test/databases/test",
	}

	cluster := NewCluster(opts)

	// Assert that the underneath initial OPTIONS, STARTUP, QUERY and
	// REGISTER messages are successful.
	session, err := cluster.CreateSession()
	assert.Nil(t, err, fmt.Sprintf("Create session failed: %v", err))
	return cluster, session
}

func teardownCluster(t *testing.T, cluster *gocql.ClusterConfig) {
	CloseCluster(cluster)
	assert.NotContains(t, proxyMap, cluster)
}

func TestNewCluster(t *testing.T) {
	t.Cleanup(adapter.ResetGrpcFuncs())
	testCases := []struct {
		name                    string
		returnResponsesInChunks bool
	}{
		{
			name:                    "SingleResponseStartup",
			returnResponsesInChunks: false,
		},
		{
			name:                    "ChunkedResponsesStartup",
			returnResponsesInChunks: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cluster, _ := setupCluster(t, tc.returnResponsesInChunks)
			assert.NotNil(t, cluster)
			assert.Equal(t, cluster.ProtoVersion, 4)

			// Assert that the proxy is created and stored in the proxyMap
			assert.Contains(t, proxyMap, cluster)
			proxy := proxyMap[cluster]
			assert.NotNil(t, proxy)

			// Assert that the cluster config is correctly set up to connect to the
			// proxy
			addr := proxy.Addr().(*net.TCPAddr)
			assert.Equal(t, cluster.Hosts, []string{addr.IP.String()})
			assert.Equal(t, cluster.Port, addr.Port)
			teardownCluster(t, cluster)
		})
	}
}

func TestSelectQuery(t *testing.T) {
	t.Cleanup(adapter.ResetGrpcFuncs())
	testCases := []struct {
		name                    string
		returnResponsesInChunks bool
	}{
		{
			name:                    "SingleResponseQuery",
			returnResponsesInChunks: false,
		},
		{
			name:                    "ChunkedResponsesQuery",
			returnResponsesInChunks: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter.MockCreateSessionGrpc()
			adapter.MockAdaptMessageGrpc(tc.returnResponsesInChunks)
			cluster, session := setupCluster(t, tc.returnResponsesInChunks)
			assert.NotNil(t, cluster)
			assert.NotNil(t, session)
			var key, val string
			err := session.Query("SELECT key,val FROM demo.keyval WHERE key = ?", "test_key").
				Scan(&key, &val)
			assert.Nil(t, err, fmt.Sprintf("Query select message failed failed: %v",
				err))
			assert.Equal(t, "test_val", val)
			teardownCluster(t, cluster)
		})
	}
}
func TestDML(t *testing.T) {
	t.Cleanup(adapter.ResetGrpcFuncs())
	testCases := []struct {
		name                    string
		returnResponsesInChunks bool
	}{
		{
			name:                    "SingleResponseDML",
			returnResponsesInChunks: false,
		},
		{
			name:                    "ChunkedResponsesDML",
			returnResponsesInChunks: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cluster, session := setupCluster(t, tc.returnResponsesInChunks)
			assert.NotNil(t, cluster)
			assert.NotNil(t, session)
			err := session.Query("UPDATE demo.keyval SET val = 'test_val' WHERE key = ?", "test_key").
				Exec()
			assert.Nil(
				t,
				err,
				fmt.Sprintf("Query dml message failed failed: %v", err),
			)
			teardownCluster(t, cluster)
		})
	}
}

func TestBatch(t *testing.T) {
	t.Cleanup(adapter.ResetGrpcFuncs())
	testCases := []struct {
		name                    string
		returnResponsesInChunks bool
	}{
		{
			name:                    "SingleResponseBatct",
			returnResponsesInChunks: false,
		},
		{
			name:                    "ChunkedResponsesBatch",
			returnResponsesInChunks: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter.MockCreateSessionGrpc()
			adapter.MockAdaptMessageGrpc(tc.returnResponsesInChunks)
			cluster, session := setupCluster(t, tc.returnResponsesInChunks)
			assert.NotNil(t, cluster)
			assert.NotNil(t, session)
			b := session.NewBatch(gocql.UnloggedBatch)
			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: "INSERT INTO demo.keyval (key) VALUES (?)",
				Args: []interface{}{"test_key"},
			})
			b.Entries = append(b.Entries, gocql.BatchEntry{
				Stmt: "UPDATE demo.keyval SET val = 'test_val' WHERE key = ?",
				Args: []interface{}{"test_key"},
			})
			err := session.ExecuteBatch(b)
			assert.Nil(t, err, fmt.Sprintf("Batch message failed failed: %v", err))
			teardownCluster(t, cluster)
		})
	}
}

func TestNewClusterPanicsOnInvalidLogLevel(t *testing.T) {
	t.Cleanup(adapter.ResetGrpcFuncs())
	testCases := []struct {
		name        string
		logLevel    string
		expectPanic bool
	}{
		{
			name:        "ShouldPanicOnInvalidLevel",
			logLevel:    "invalid",
			expectPanic: true,
		},
		{
			name:        "ShouldNotPanicOnEmptyLevel",
			logLevel:    "",
			expectPanic: false,
		},
		{
			name:        "ShouldNotPanicOnInfoLevel",
			logLevel:    "info",
			expectPanic: false,
		},
		{
			name:        "ShouldNotPanicOnWarnLevel",
			logLevel:    "warn",
			expectPanic: false,
		},
		{
			name:        "ShouldNotPanicOnErrorLevel",
			logLevel:    "error",
			expectPanic: false,
		},
		{
			name:        "ShouldNotPanicOnFatalLevel",
			logLevel:    "fatal",
			expectPanic: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter.MockCreateSessionGrpc()

			opts := &Options{
				DatabaseUri: "projects/test/instances/test/databases/test",
				LogLevel:    tc.logLevel,
			}

			callNewCluster := func() {
				cluster := NewCluster(opts)
				if cluster != nil {
					teardownCluster(t, cluster)
				}
			}

			if tc.expectPanic {
				require.Panics(
					t,
					callNewCluster,
					"NewCluster should panic with invalid log level",
				)
			} else {
				require.NotPanics(t, callNewCluster, "NewCluster should not panic with valid log level")
			}
		})
	}
}
