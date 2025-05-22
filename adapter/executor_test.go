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

package adapter

import (
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func TestIsDML(t *testing.T) {
	testCases := []struct {
		name     string
		frame    *frame.Frame
		expected bool
	}{
		// Execute message
		{
			name: "Execute message with DML QueryId prefix",
			frame: newFrameWithMessage(&message.Execute{
				QueryId: []byte(writeActionQueryIdPrefix + "test-id-123"),
			}),
			expected: true,
		},
		{
			name: "Execute message with select QueryId prefix",
			frame: newFrameWithMessage(&message.Execute{
				QueryId: []byte(
					"R" + "test-id-456",
				),
			}),
			expected: false,
		},

		// Batch message
		{
			name: "Batch message",
			frame: newFrameWithMessage(&message.Batch{
				Type: primitive.BatchTypeLogged,
			}),
			expected: true,
		},

		// Query message
		{
			name: "Query message with 'select' (lowercase)",
			frame: newFrameWithMessage(&message.Query{
				Query: "select * from users where id = 1",
			}),
			expected: false,
		},
		{
			name: "Query message with 'SELECT' (uppercase)",
			frame: newFrameWithMessage(&message.Query{
				Query: "SELECT * from users where id = 1",
			}),
			expected: false,
		},
		{
			name: "Query message with 'SeLeCt' (mixed case)",
			frame: newFrameWithMessage(&message.Query{
				Query: "SeLeCt * from users where id = 1",
			}),
			expected: false,
		},
		{
			name: "Query message with non-select prefix",
			frame: newFrameWithMessage(&message.Query{
				Query: "insert into users (id, name) values (1, 'John')",
			}),
			expected: true,
		},

		// Other messages
		{
			name:     "Default message type - Options",
			frame:    newFrameWithMessage(&message.Options{}),
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := isDML(tc.frame)
			if actual != tc.expected {
				var queryDetail string

				switch msg := tc.frame.Body.Message.(type) {
				case *message.Query:
					queryDetail = "Query: " + msg.Query
				case *message.Execute:
					queryDetail = "Execute QueryId: " + string(msg.QueryId)
				default:
					queryDetail = "Message Type: " + tc.frame.Body.Message.GetOpCode().String()
				}
				t.Errorf(
					"isDML() for test case '%s' (%s) = %v, want %v",
					tc.name,
					queryDetail,
					actual,
					tc.expected,
				)
			}
		})
	}
}
