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
	"fmt"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"google.golang.org/grpc/codes"
)

// TODO: Add unit tests to verify metric population affter integration with
// AdaptMessage is done.

// TestGenerateClientHash tests the generateClientHash function.
func TestGenerateClientHash(t *testing.T) {
	tests := []struct {
		name             string
		clientUID        string
		expectedValue    string
		expectedLength   int
		expectedMaxValue int64
	}{
		{"Simple UID", "exampleUID", "00006b", 6, 0x3FF},
		{"Empty UID", "", "000000", 6, 0x3FF},
		{"Special Characters", "!@#$%^&*()", "000389", 6, 0x3FF},
		{
			"Very Long UID",
			"aVeryLongUniqueIdentifierThatExceedsNormalLength",
			"000125",
			6,
			0x3FF,
		},
		{"Numeric UID", "1234567890", "00003e", 6, 0x3FF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := generateClientHash(tt.clientUID)
			if hash != tt.expectedValue {
				t.Errorf("expected hash value %s, got %s", tt.expectedValue, hash)
			}
			// Check if the hash length is 6
			if len(hash) != tt.expectedLength {
				t.Errorf(
					"expected hash length %d, got %d",
					tt.expectedLength,
					len(hash),
				)
			}

			// Check if the hash is in the range [000000, 0003ff]
			hashValue, err := parseHex(hash)
			if err != nil {
				t.Errorf("failed to parse hash: %v", err)
			}
			if hashValue < 0 || hashValue > tt.expectedMaxValue {
				t.Errorf(
					"expected hash value in range [0, %d], got %d",
					tt.expectedMaxValue,
					hashValue,
				)
			}
		})
	}
}

// parseHex converts a hexadecimal string to an int64.
func parseHex(hexStr string) (int64, error) {
	var value int64
	_, err := fmt.Sscanf(hexStr, "%x", &value)
	return value, err
}

func TestConvertResponseMessageToGRPCStatus(t *testing.T) {
	testCases := []struct {
		name           string
		frame          *frame.Frame
		expectedGOCode codes.Code
	}{
		{
			name:           "Non-error message",
			frame:          newFrameWithMessage(&message.RowsResult{}),
			expectedGOCode: codes.OK,
		},
		{
			name: "ServerError",
			frame: newFrameWithMessage(&message.ServerError{
				ErrorMessage: "Simulated server error",
			}),
			expectedGOCode: codes.Internal,
		},
		{
			name: "ReadFailure",
			frame: newFrameWithMessage(&message.ReadFailure{
				ErrorMessage: "Simulated read failure",
			}),
			expectedGOCode: codes.Internal,
		},
		{
			name: "WriteFailure",
			frame: newFrameWithMessage(&message.WriteFailure{
				ErrorMessage: "Simulated write failure",
			}),
			expectedGOCode: codes.Internal,
		},
		{
			name: "SyntaxError",
			frame: newFrameWithMessage(&message.SyntaxError{
				ErrorMessage: "Simulated syntax error",
			}),
			expectedGOCode: codes.InvalidArgument,
		},
		{
			name: "Invalid",
			frame: newFrameWithMessage(&message.Invalid{
				ErrorMessage: "Simulated invalid error",
			}),
			expectedGOCode: codes.InvalidArgument,
		},
		{
			name: "ConfigError",
			frame: newFrameWithMessage(&message.ConfigError{
				ErrorMessage: "Simulated config error",
			}),
			expectedGOCode: codes.FailedPrecondition,
		},
		{
			name: "Unprepared statement error",
			frame: newFrameWithMessage(
				&message.Unprepared{
					ErrorMessage: "Simulated unprepared statement",
					Id:           []byte{0xDE, 0xAD},
				},
			),
			expectedGOCode: codes.FailedPrecondition,
		},
		{
			name: "Other error(default case)",
			frame: newFrameWithMessage(&message.AuthenticationError{
				ErrorMessage: "Simulated authentication error",
			}),
			expectedGOCode: codes.Unknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualStatusString := ConvertResponseMessageToGRPCStatus(
				tc.frame,
			) // Function being tested
			expectedStatusString := tc.expectedGOCode.String()

			if actualStatusString != expectedStatusString {
				var messageDetail string
				if tc.frame.Header == nil {
					messageDetail = "Frame Header is nil"
				} else if tc.frame.Body == nil {
					messageDetail = fmt.Sprintf("OpCode: %s, Frame Body is nil", tc.frame.Header.OpCode)
				} else if tc.frame.Body.Message == nil {
					messageDetail = fmt.Sprintf("OpCode: %s, MessageType: nil message content", tc.frame.Header.OpCode)
				} else {
					messageDetail = fmt.Sprintf("OpCode: %s, MessageType: %T", tc.frame.Header.OpCode, tc.frame.Body.Message)
				}

				t.Errorf(
					"ConvertResponseMessageToGRPCStatus() for test case '%s' (%s) = '%s', want '%s'",
					tc.name,
					messageDetail,
					actualStatusString,
					expectedStatusString,
				)
			}
		})
	}
}

func TestConvertRequestMessageToGRPCMethodName(t *testing.T) {
	testCases := []struct {
		name     string
		frame    *frame.Frame
		expected string
	}{
		{
			name: "Prepare message",
			frame: newFrameWithMessage(
				&message.Prepare{Query: "SELECT * FROM test"},
			),
			expected: metricLabelValuePrepare,
		},
		{
			name: "Execute message",
			frame: newFrameWithMessage(
				&message.Execute{QueryId: []byte{0x01, 0x02}},
			),
			expected: metricLabelValueExecute,
		},
		{
			name: "Batch message",
			frame: newFrameWithMessage(
				&message.Batch{Type: primitive.BatchTypeLogged},
			),
			expected: metricLabelValueBatch,
		},
		{
			name: "Query message",
			frame: newFrameWithMessage(
				&message.Query{Query: "INSERT INTO test VALUES (1)"},
			),
			expected: metricLabelValueQuery,
		},
		{
			name: "Other message (default case)",
			frame: newFrameWithMessage(
				&message.Startup{Options: map[string]string{}},
			),
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := ConvertRequestMessageToGRPCMethodName(
				tc.frame,
			)
			if actual != tc.expected {
				var messageDetail string
				if tc.frame.Body != nil && tc.frame.Body.Message != nil {
					messageDetail = fmt.Sprintf("MessageType: %T", tc.frame.Body.Message)
				} else if tc.frame.Body != nil {
					messageDetail = "MessageType: nil message content"
				} else {
					messageDetail = "Frame Body is nil"
				}

				t.Errorf(
					"ConvertRequestMessageToGRPCMethodName() for test case '%s' (%s) = '%s', want '%s'",
					tc.name,
					messageDetail,
					actual,
					tc.expected,
				)
			}
		})
	}
}
