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
	"context"

	"strings"

	"cloud.google.com/go/spanner/adapter/apiv1/adapterpb"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	AdaptMessageGrpc = func(
		ctx context.Context,
		req *adapterpb.AdaptMessageRequest,
		cl *AdapterClient,
	) (adapterpb.Adapter_AdaptMessageClient, error) {
		var md metadata.MD
		request, err := cl.gapicClient.AdaptMessage(
			contextWithOutgoingMetadata(ctx, cl.getMetadata()),
			req,
			gax.WithGRPCOptions(grpc.Header(&md)),
		)
		if err != nil {
			return nil, err
		}
		return request, nil
	}
)

type requestExecutor struct {
	protocol     Protocol
	client       *AdapterClient
	globalState  *globalState
	xGoogHeaders []string
}

func (re *requestExecutor) tryInsertAttachment(
	queryID []byte, attachments map[string]string,
) message.Message {
	var key strings.Builder
	key.WriteString(preparedQueryIdAttachmentPrefix)
	key.WriteString(string(queryID))
	if val, found := re.globalState.Load(key.String()); found {
		attachments[key.String()] = val
		return nil
	}
	return &message.Unprepared{
		ErrorMessage: "Unknown prepared query in client side cache",
		Id:           queryID,
	}
}

func (re *requestExecutor) prepareCassandraAttachments(
	frame *frame.Frame, req *requestState) message.Message {
	switch msg := frame.Body.Message.(type) {
	case *message.Execute:
		req.pb.Attachments = make(map[string]string)
		err := re.tryInsertAttachment(msg.QueryId, req.pb.Attachments)
		if err != nil {
			return err
		}
	case *message.Batch:
		req.pb.Attachments = make(map[string]string)
		for _, child := range msg.Children {
			// Only prepare <pqid, cql_query> attachment pair for prepared child in
			// batch.
			if child.Query == "" {
				// Reject entire batch and return an unprepared error back to driver on
				// first seen local prepared query cache miss.
				err := re.tryInsertAttachment(child.Id, req.pb.Attachments)
				if err != nil {
					return err
				}
			}
		}
	default:
		return nil
	}
	return nil
}

func (re *requestExecutor) submit(
	ctx context.Context,
	req *requestState,
) (adapterpb.Adapter_AdaptMessageClient, error) {
	pbCli, err := RunAdaptMessageWithRetry(
		ctx,
		re.client.opts.DisableAdaptMessageRetry,
		func(ctx context.Context) (adapterpb.Adapter_AdaptMessageClient, error) {
			return AdaptMessageGrpc(
				ctx,
				req.pb,
				re.client,
			)
		},
	)
	if err != nil {
		return nil, err
	}
	if err := pbCli.CloseSend(); err != nil {
		return nil, err
	}

	return pbCli, nil
}
