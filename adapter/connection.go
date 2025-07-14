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
	"sync"

	"cloud.google.com/go/spanner/adapter/apiv1/adapterpb"
	"github.com/googleapis/go-spanner-cassandra/logger"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// driverConnection encapsulates a connection from a native database driver.
type driverConnection struct {
	connectionID          int
	protocol              Protocol
	driverConn            net.Conn
	adapterClient         *AdapterClient
	executor              *requestExecutor
	globalState           *globalState
	md                    metadata.MD
	codec                 frame.Codec
	rawCodec              frame.RawCodec
	maxConcurrencyPerConn int
}

// requestJob holds the data needed for a worker to process a single request.
type requestJob struct {
	payload *[]byte
	header  *frame.Header
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
	header.Flags = 0;
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
	if len(payloads) == 0 {
		return nil // No payload received, nothing to write.
	}

	// If there is only one response, it consists a complete message frame and we
	// can directly wirte it back.
	if len(payloads) == 1 {
		_, err := dc.driverConn.Write(payloads[0])
		return err
	}

	// Merge payloads (last + first...second last) since last payload is always
	// the header when there are more than one responses received.
	totalSize := 0
	for _, p := range payloads {
		totalSize += len(p)
	}

	payloadToWrite := make([]byte, totalSize)

	lastPayload := payloads[len(payloads)-1]
	offset := copy(payloadToWrite, lastPayload)
	for _, p := range payloads[:len(payloads)-1] {
		offset += copy(payloadToWrite[offset:], p)
	}

	_, err = dc.driverConn.Write(payloadToWrite)
	return err
}

// worker is a long-lived goroutine that processes requests from the
// requestChan.
func (dc *driverConnection) worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	requestChan <-chan requestJob,
) {
	for job := range requestChan {
		dc.processAndRespond(ctx, wg, job.payload, job.header)
	}
}

func (dc *driverConnection) handleConnection(ctx context.Context) {
	// Use a WaitGroup to track all in-flight request goroutines.
	var wg sync.WaitGroup
	// Create a channel to dispatch jobs to a fixed pool of worker goroutines.
	requestChan := make(chan requestJob)

	// Start a fixed pool of worker goroutines.
	for range dc.maxConcurrencyPerConn {
		go dc.worker(ctx, &wg, requestChan)
	}

	defer func() {
		// Signal to workers that no more jobs will be sent.
		close(requestChan)
		// Before closing the connection, wait for all processing goroutines to
		// finish.
		wg.Wait()
		logger.Debug(
			"All in-flight requests finished. Exiting recv loop.",
			zap.Int("connection id", dc.connectionID),
		)
		dc.driverConn.Close()
	}()

	// This loop now acts as a reader/dispatcher.
	for {
		payload, header, err := dc.constructPayload()
		if err != nil {
			// Only EOF error is expected if the peer closes the connection
			// gracefully.
			if !errors.Is(err, io.EOF) {
				logger.Error("Error constructing payload, closing connection",
					zap.Int("connectionID", dc.connectionID),
					zap.Error(err))
			}
			// Break whenever there is a non-retriable error(ie: when peer force
			// closed connection, invalid header, etc) and we can not write any
			// responses back to the driver.
			break
		}

		job := requestJob{
			payload: payload,
			header:  header,
		}

		wg.Add(1)
		// Dispatch the job to a free worker. This will block if all workers are
		// busy.
		requestChan <- job
	}
}

// processAndRespond handles the full lifecycle of a single request. It is
// called by a worker goroutine.
func (dc *driverConnection) processAndRespond(
	ctx context.Context,
	wg *sync.WaitGroup,
	payload *[]byte,
	header *frame.Header,
) {
	// Decrease wait group counter.
	defer wg.Done()

	frame, err := dc.codec.DecodeFrame(bytes.NewBuffer(*payload))
	if err != nil {
		logger.Error("Error decoding frame from payload",
			zap.Int("connectionID", dc.connectionID),
			zap.Error(err))
		// Return a syntax error back to the driver if the received payload is not
		// a valid Cassandra frame protocol.
		_ = dc.writeMessageBackToTcp(
			header,
			&message.SyntaxError{ErrorMessage: err.Error()},
		)
		dc.driverConn.Close()
		return
	}

	session, err := dc.adapterClient.getOrRefreshSession(ctx)
	if err != nil {
		logger.Error("Error getting or refreshing session",
			zap.Int("connectionID", dc.connectionID),
			zap.Error(err))
		// Return a server error back to the driver if session retrieval or
		// recreation is failed.
		_ = dc.writeMessageBackToTcp(
			frame.Header,
			&message.ServerError{ErrorMessage: err.Error()},
		)
		return
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
		// Since a manual constructed message was already sent back to the
		// driver from this client successfully, skip rest of grpc calls to the
		// server.
		_ = dc.writeMessageBackToTcp(frame.Header, errMsg)
		return
	}

	// Send the grpc request.
	pbCli, err := dc.executor.submit(ctx, req, isDML(&req.frame))
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
		return
	}
	// Read grpc response and write back to local tcp connection.
	if err = dc.writeGrpcResponseToTcp(pbCli); err != nil {
		logger.Error("Error writing grpc response back to tcp",
			zap.Int("connectionID", int(dc.connectionID)),
			zap.Error(err),
		)
	}
}
