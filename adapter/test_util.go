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

package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/googleapis/go-spanner-cassandra/adapter/apiv1/adapterpb"
	"google.golang.org/grpc/metadata"
)

var (
	selectPreparedId = "select_id"
	dmlPreparedId    = "dml_id"
)

type GrpcFuncs struct {
	CreateSession func(ctx context.Context, req *adapterpb.CreateSessionRequest, cl *AdapterClient) (*adapterpb.Session, error)
	AdaptMessage  func(
		ctx context.Context,
		req *adapterpb.AdaptMessageRequest,
		cl *AdapterClient,
	) (adapterpb.Adapter_AdaptMessageClient, error)
}

func currentGrpcFuncs() GrpcFuncs {
	return GrpcFuncs{
		CreateSession: CreateSessionGrpc,
		AdaptMessage:  AdaptMessageGrpc,
	}
}

func restoreGrpcFuncs(funcs GrpcFuncs) {
	CreateSessionGrpc = funcs.CreateSession
	AdaptMessageGrpc = funcs.AdaptMessage
}

func ResetGrpcFuncs() func() {
	origFuncs := currentGrpcFuncs()
	return func() {
		restoreGrpcFuncs(origFuncs)
	}
}

func MockCreateSessionGrpc(mock_session_names ...string) {
	CreateSessionGrpc = func(ctx context.Context, req *adapterpb.CreateSessionRequest, cl *AdapterClient) (*adapterpb.Session, error) {
		sessionName := "default-test-session" // Default value
		if len(mock_session_names) > 0 {
			sessionName = mock_session_names[0] // Use provided value
		}
		return &adapterpb.Session{Name: sessionName}, nil
	}
}

type Mock_Cassandra_AdaptMessageClient struct {
	reqFrame                *frame.Frame
	eof                     bool
	returnResponsesInChunks bool
	bodyResponsesReturned   bool
}

func (mc *Mock_Cassandra_AdaptMessageClient) CloseSend() error {
	return nil
}

func (mc *Mock_Cassandra_AdaptMessageClient) Context() context.Context {
	panic("unimplemented")
}

func (mc *Mock_Cassandra_AdaptMessageClient) Header() (metadata.MD, error) {
	panic("unimplemented")
}

func (mc *Mock_Cassandra_AdaptMessageClient) RecvMsg(m any) error {
	panic("unimplemented")
}

func (mc *Mock_Cassandra_AdaptMessageClient) SendMsg(m any) error {
	panic("unimplemented")
}

func (mc *Mock_Cassandra_AdaptMessageClient) Trailer() metadata.MD {
	panic("unimplemented")
}

func (mc *Mock_Cassandra_AdaptMessageClient) Recv() (*adapterpb.AdaptMessageResponse, error) {
	if mc.eof {
		return nil, io.EOF
	}
	if mc.returnResponsesInChunks {
		if mc.bodyResponsesReturned {
			mc.eof = true
		}
	} else {
		mc.eof = true
	}
	switch msg := mc.reqFrame.Body.Message.(type) {
	case *message.Startup:
		return mc.handleStartup(mc.reqFrame.Header)
	case *message.Options:
		return mc.handleOptions(mc.reqFrame.Header)
	case *message.Query:
		return mc.handleQuery(mc.reqFrame.Header, msg)
	case *message.Register:
		return mc.handleRegister(mc.reqFrame.Header)
	case *message.Prepare:
		return mc.handlePrepare(mc.reqFrame.Header, msg)
	case *message.Execute:
		return mc.handleExecute(mc.reqFrame.Header, msg)
	case *message.Batch:
		return mc.handleBatch(mc.reqFrame.Header)
	default:
		return nil, fmt.Errorf("unsupported frame type")
	}
}

type mockAdaptMessageRespOpts struct {
	stateUpdates map[string]string
}

func (mc *Mock_Cassandra_AdaptMessageClient) constructAdaptMessageResponse(
	hdr *frame.Header,
	msg message.Message,
	opts ...mockAdaptMessageRespOpts,
) (*adapterpb.AdaptMessageResponse, error) {
	options := mockAdaptMessageRespOpts{}
	if len(opts) > 0 {
		options = opts[0]
	}
	out := hdr
	out.IsResponse = true
	out.OpCode = msg.GetOpCode()
	frm := &frame.Frame{
		Header: out,
		Body:   &frame.Body{Message: msg},
	}

	var payload []byte
	if mc.returnResponsesInChunks {
		rawCodec := frame.NewRawCodec()
		if !mc.bodyResponsesReturned {
			rawFrame, err := rawCodec.ConvertToRawFrame(frm)
			if err != nil {
				return nil, err
			}
			mc.bodyResponsesReturned = true
			payload = rawFrame.Body
		} else {
			rawHeader := bytes.NewBuffer(nil)
			if err := rawCodec.EncodeHeader(out, rawHeader); err != nil {
				return nil, err
			}
			payload = rawHeader.Bytes()
		}
	} else {
		codec := frame.NewCodec()
		buf := bytes.NewBuffer(nil)
		err := codec.EncodeFrame(frm, buf)
		if err != nil {
			return nil, err
		}
		payload = buf.Bytes()
	}
	resp := &adapterpb.AdaptMessageResponse{
		Payload:      payload,
		StateUpdates: options.stateUpdates,
	}
	return resp, nil
}

func (mc *Mock_Cassandra_AdaptMessageClient) handleOptions(
	hdr *frame.Header) (*adapterpb.AdaptMessageResponse, error) {
	return mc.constructAdaptMessageResponse(hdr, &message.Supported{
		Options: map[string][]string{
			"COMPRESSION": {},
			"CQL_VERSION": {"3.0.0"},
		},
	})
}

func (mc *Mock_Cassandra_AdaptMessageClient) handleStartup(
	hdr *frame.Header) (*adapterpb.AdaptMessageResponse, error) {
	return mc.constructAdaptMessageResponse(hdr, &message.Ready{})
}

func (mc *Mock_Cassandra_AdaptMessageClient) handleRegister(
	hdr *frame.Header) (*adapterpb.AdaptMessageResponse, error) {
	return mc.constructAdaptMessageResponse(hdr, &message.Ready{})
}

func (mc *Mock_Cassandra_AdaptMessageClient) handleQuery(
	hdr *frame.Header,
	msg *message.Query,
) (*adapterpb.AdaptMessageResponse, error) {
	if strings.HasPrefix(msg.Query, "SELECT * FROM system.local") {
		return mc.constructAdaptMessageResponse(hdr, &message.RowsResult{
			Metadata: &message.RowsMetadata{
				ColumnCount: 9,
				Columns: []*message.ColumnMetadata{
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "key",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "rpc_address",
						Type:     datatype.Inet,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "datacenter",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "rack",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "release_version",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "partitioner",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "cluster_name",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "cql_version",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "local",
						Name:     "native_protocol_version",
						Type:     datatype.Varchar,
					},
				},
			},
			Data: message.RowSet{
				message.Row{
					[]byte("local"),
					[]byte("\x7f\x00\x00\x01"),
					[]byte("datacenter1"),
					[]byte("rack1"),
					[]byte("4.0.0"),
					[]byte("org.apache.cassandra.dht.Murmur3Partitioner"),
					[]byte("spanner-proxy"),
					[]byte("3.0.0"),
					[]byte("ProtocolVersion OSS 4"),
				},
			},
		})
	} else if strings.HasPrefix(msg.Query, "SELECT * FROM system.peers") {
		return mc.constructAdaptMessageResponse(hdr, &message.RowsResult{
			Metadata: &message.RowsMetadata{
				ColumnCount: 12,
				Columns: []*message.ColumnMetadata{
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "peer",
						Type:     datatype.Inet,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "peer_port",
						Type:     datatype.Int,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "datacenter",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "rack",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "host_id",
						Type:     datatype.Uuid,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "preferred_ip",
						Type:     datatype.Inet,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "preferred_port",
						Type:     datatype.Int,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "native_address",
						Type:     datatype.Inet,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "native_port",
						Type:     datatype.Int,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "release_version",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "schema_version",
						Type:     datatype.Uuid,
					},
					{
						Keyspace: "system",
						Table:    "peers_v2",
						Name:     "state",
						Type:     datatype.Varchar,
					},
				},
			},
		})
	}
	return nil, errors.New("unrecognized query")
}

func (mc *Mock_Cassandra_AdaptMessageClient) handlePrepare(
	hdr *frame.Header,
	msg *message.Prepare,
) (*adapterpb.AdaptMessageResponse, error) {
	var stateUpdates map[string]string
	var preparedQueryId []byte
	if strings.HasPrefix(msg.Query, "SELECT") {
		stateUpdates = map[string]string{
			"pqid/" + string([]byte(selectPreparedId)): "hashed_select_query",
		}
		preparedQueryId = []byte(selectPreparedId)
	} else {
		stateUpdates = map[string]string{
			"pqid/" + string([]byte(dmlPreparedId)): "hashed_dml_query",
		}
		preparedQueryId = []byte(dmlPreparedId)
	}
	return mc.constructAdaptMessageResponse(
		hdr,
		&message.PreparedResult{
			PreparedQueryId: preparedQueryId,
			VariablesMetadata: &message.VariablesMetadata{
				PkIndices: []uint16{1},
				Columns: []*message.ColumnMetadata{
					{
						Keyspace: "demo",
						Table:    "keyval",
						Name:     "key",
						Type:     datatype.Varchar,
					},
				},
			},
			ResultMetadata: &message.RowsMetadata{
				ColumnCount: 2,
				Columns: []*message.ColumnMetadata{
					{
						Keyspace: "demo",
						Table:    "keyval",
						Name:     "key",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "demo",
						Table:    "keyval",
						Name:     "val",
						Type:     datatype.Varchar,
					},
				},
			},
		},
		mockAdaptMessageRespOpts{stateUpdates: stateUpdates},
	)
}

func (mc *Mock_Cassandra_AdaptMessageClient) handleExecute(
	hdr *frame.Header,
	msg *message.Execute,
) (*adapterpb.AdaptMessageResponse, error) {
	if bytes.Equal(msg.QueryId, []byte(selectPreparedId)) {
		return mc.constructAdaptMessageResponse(hdr, &message.RowsResult{
			Metadata: &message.RowsMetadata{
				ColumnCount: 2,
				Columns: []*message.ColumnMetadata{
					{
						Keyspace: "demo",
						Table:    "keyval",
						Name:     "key",
						Type:     datatype.Varchar,
					},
					{
						Keyspace: "demo",
						Table:    "keyval",
						Name:     "val",
						Type:     datatype.Varchar,
					},
				},
			},
			Data: message.RowSet{
				message.Row{
					[]byte("test_key"),
					[]byte("test_val"),
				},
			},
		})
	} else if bytes.Equal(msg.QueryId, []byte(dmlPreparedId)) {

		return mc.constructAdaptMessageResponse(hdr, &message.VoidResult{})
	}
	return nil, errors.New("unexpected mocked query for unit test")
}

func (mc *Mock_Cassandra_AdaptMessageClient) handleBatch(
	hdr *frame.Header) (*adapterpb.AdaptMessageResponse, error) {
	// Since batch are always DMLs, we always return void result.
	return mc.constructAdaptMessageResponse(hdr, &message.VoidResult{})
}

func MockAdaptMessageGrpc(returnResponsesInChunks bool) {
	AdaptMessageGrpc = func(
		ctx context.Context,
		req *adapterpb.AdaptMessageRequest,
		cl *AdapterClient,
	) (adapterpb.Adapter_AdaptMessageClient, error) {
		if req.Protocol != "cassandra" {
			return nil, errors.New("unsupported protocol type")
		}
		codec := frame.NewCodec()
		frame, err := codec.DecodeFrame(bytes.NewBuffer(req.Payload))
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		request := &Mock_Cassandra_AdaptMessageClient{
			reqFrame:                frame,
			eof:                     false,
			returnResponsesInChunks: returnResponsesInChunks,
		}
		return request, nil
	}
}
