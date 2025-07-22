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
	"bytes"
	"context"
	"errors"
	"io"
	"net"

	"cloud.google.com/go/spanner/adapter/apiv1/adapterpb"
	"github.com/googleapis/go-spanner-cassandra/logger"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// driverConnection encapsulates a connection from a native database driver.
type driverConnection struct {
	connectionID  int
	protocol      Protocol
	driverConn    net.Conn
	adapterClient *AdapterClient
	executor      *requestExecutor
	globalState   *globalState
	md            metadata.MD
	codec         frame.Codec
	rawCodec      frame.RawCodec
}

func (dc *driverConnection) constructPayload() (*[]byte, *frame.Header, error) {
	// Decode cassandra frame to Header + raw body.
	rawFrame, err := dc.rawCodec.DecodeRawFrame(dc.driverConn)
	if err != nil {
		return nil, nil, err
	}

	rawHeader := bytes.NewBuffer(nil)
	if err := dc.rawCodec.EncodeHeader(rawFrame.Header, rawHeader); err != nil {
		return nil, nil, err
	}

	// Assemble payload.
	body := rawFrame.Body
	payload := append(rawHeader.Bytes(), body...)
	return &payload, rawFrame.Header, nil
}

func (dc *driverConnection) writeMessageBackToTcp(
	header *frame.Header,
	msg message.Message,
) error {
	header.IsResponse = true
	header.OpCode = msg.GetOpCode()
	// Clear all flags in manually constructed error response
	header.Flags = 0
	frm := &frame.Frame{
		Header: header,
		Body:   &frame.Body{Message: msg},
	}
	buf := bytes.NewBuffer(nil)
	err := dc.codec.EncodeFrame(frm, buf)
	if err != nil {
		return err
	}
	_, err = dc.driverConn.Write(buf.Bytes())
	if err != nil {
		logger.Error("Error writing message back to tcp ",
			zap.Int("connectionID", dc.connectionID),
			zap.Error(err))
		return err
	}
	return nil
}

func (dc *driverConnection) writeGrpcResponseToTcp(
	pbCli adapterpb.Adapter_AdaptMessageClient,
) error {
	var err error
	var resp *adapterpb.AdaptMessageResponse
	var payloads [][]byte

	for err == nil {
		resp, err = pbCli.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Debug(
				"Error reading AdaptMessageResponse. ",
				zap.Error(err),
			)
			return err
		}
		if resp.GetStateUpdates() != nil {
			for k, v := range resp.GetStateUpdates() {
				dc.globalState.Store(k, v)
			}
		}
		if resp.Payload != nil {
			payloads = append(payloads, resp.Payload)
		}
	}
	payloadsLen := len(payloads)
	var payloadToWrite []byte
	if payloadsLen == 0 {
		return nil // No payload received, nothing to write.
	}

	// If there is only one response, it consists a complete message frame and we
	// can directly wirte it back.
	if payloadsLen == 1 {
		payloadToWrite = payloads[0]
	} else {
		// Merge payloads (last + first...second last) since last payload is always
		// the header when there are more than one responses received.
		lastPayload := payloads[payloadsLen-1]
		mergedPayload := bytes.Buffer{}
		mergedPayload.Write(lastPayload)

		for i := range payloads[:payloadsLen-1] {
			mergedPayload.Write(payloads[i])
		}
		payloadToWrite = mergedPayload.Bytes()
	}

	_, err = dc.driverConn.Write(payloadToWrite)
	if err != nil {
		logger.Debug("Error writing merged payload to connection",
			zap.Int("connectionID", dc.connectionID),
			zap.Error(err),
		)
		return err
	}

	return nil
}

func (dc *driverConnection) handleConnection(ctx context.Context) {
	defer func() {
		logger.Debug(
			"Exiting recv loop",
			zap.Int("connection id", dc.connectionID),
		)
		dc.driverConn.Close()
	}()
	for {
		payload, header, err := dc.constructPayload()
		if err != nil {
			// Only EOF error is expected if the peer closes the connection
			// gracefully.
			if !errors.Is(err, io.EOF) {
				logger.Error("Error constructing AdaptMessagePayload ",
					zap.Int("connectionID", dc.connectionID),
					zap.Error(err))
			}
			// Break whenever there is a non-retriable error(ie: when peer force
			// closed connection, invalid header, etc) and we can not write any
			// responses back to the driver.
			break
		}

		frame, err := dc.codec.DecodeFrame(bytes.NewBuffer(*payload))
		if err != nil {
			logger.Error("Error decoding frame from payload ",
				zap.Int("connectionID", dc.connectionID),
				zap.Error(err))
			// Return a syntax error back to the driver if the received payload is not
			// a valid Cassandra frame protocol.
			_ = dc.writeMessageBackToTcp(
				header,
				&message.SyntaxError{ErrorMessage: err.Error()},
			)
			continue
		}

		session, err := dc.adapterClient.getOrRefreshSession(ctx)
		if err != nil {
			logger.Error("Error getting or refreshing session ",
				zap.Int("connectionID", dc.connectionID),
				zap.Error(err))
			// Return a server error back to the driver if session retrieval or
			// recreation is failed.
			_ = dc.writeMessageBackToTcp(
				frame.Header,
				&message.ServerError{ErrorMessage: err.Error()},
			)
			continue
		}

		req := &requestState{
			pb: &adapterpb.AdaptMessageRequest{
				Name:     session.name,
				Protocol: dc.protocol.Name(),
				Payload:  *payload,
			},
			frame: *frame,
		}

		// Pass attachments, send back any error messages to the driver and skips
		// later grpc call.
		if errMsg := dc.executor.prepareCassandraAttachments(frame, req); errMsg != nil {
			_ = dc.writeMessageBackToTcp(frame.Header, errMsg)
			// Since a manual constructed message was already sent back to the
			// driver from this client successfully, skip rest of grpc calls to the
			// server.
			continue
		}

		// Send the grpc request.
		var pbCli adapterpb.Adapter_AdaptMessageClient
		pbCli, err = dc.executor.submit(ctx, req, isDML(&req.frame))
		if err != nil {
			logger.Error("Error sending AdaptMessageRequest to server",
				zap.Int("connectionID", int(dc.connectionID)),
				zap.Error(err),
			)
			// If requests was not successfully sent to server, return a server error
			// and skip reading responses
			// from the server.
			_ = dc.writeMessageBackToTcp(
				frame.Header,
				&message.ServerError{ErrorMessage: err.Error()},
			)
			continue
		}
		// Read grpc response and write back to local tcp connection.
		if err = dc.writeGrpcResponseToTcp(pbCli); err != nil {
			logger.Error("Error writing grpc response back to tcp",
				zap.Int("connectionID", int(dc.connectionID)),
				zap.Error(err),
			)
			_ = dc.writeMessageBackToTcp(
				frame.Header,
				&message.ServerError{ErrorMessage: err.Error()},
			)
		}
	}
}
