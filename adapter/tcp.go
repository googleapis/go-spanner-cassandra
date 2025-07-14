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
	"errors"
	"fmt"
	"net"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/googleapis/go-spanner-cassandra/logger"

	"go.uber.org/zap"
)

// TCPProxy encapsulates a Spanner Adapter proxy.
type TCPProxy struct {
	opts             Options
	listener         net.Listener
	client           *AdapterClient
	nextConnectionID int
	globalState      *globalState
}

// NewTCPProxy returns a new Spanner Adapter proxy.
func NewTCPProxy(opts Options) (*TCPProxy, error) {
	ctx := context.Background()
	if opts.Protocol == nil {
		return nil, fmt.Errorf("nil protocol adapter provided to spanner TCPProxy")
	}
	if opts.NumGrpcChannels <= 0 {
		opts.NumGrpcChannels = 4
	}

	// Create spanner adapter client.
	cl, err := newAdapterClient(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Create initial session
	err = cl.createSession(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Get or create global state cache.
	globalState, err := NewDefaultGlobalState(maxGlobalStateSize)
	if err != nil {
		return nil, err
	}

	// Create TCP proxy.
	proxy := &TCPProxy{
		opts:        opts,
		client:      cl,
		globalState: globalState,
	}

	// Start local listener.
	if opts.TCPEndpoint == "" {
		opts.TCPEndpoint = "localhost:9042"
	}
	proxy.listener, err = net.Listen("tcp", opts.TCPEndpoint)
	if err != nil {
		return nil, fmt.Errorf(
			"spanner proxy failed to listen on local port: %w",
			err,
		)
	}
	logger.Info(
		"Spanner proxy listening on ",
		zap.String("tcp_port", proxy.listener.Addr().String()),
	)

	// Init codec for all connection to reuse
	codec := frame.NewCodec()
	rawCodec := frame.NewRawCodec()

	if opts.MaxConcurrencyPerConn <= 0 {
		opts.MaxConcurrencyPerConn = 32768
	}

	// Start accept loop.
	go func() {
		for {
			// Wait for a connection.
			conn, err := proxy.listener.Accept()

			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					break
				} else {
					logger.Error("Spanner proxy failed to accept connection", zap.Error(err))
					break
				}
			}
			logger.Debug(
				"Spanner proxy received a connection, assigning ID",
				zap.Int("connection_id", proxy.nextConnectionID),
			) // Prepare to accept next connection.

			dc := &driverConnection{
				connectionID:  proxy.nextConnectionID,
				protocol:      opts.Protocol,
				adapterClient: proxy.client,
				executor: &requestExecutor{
					protocol:    opts.Protocol,
					client:      proxy.client,
					globalState: proxy.globalState,
				},
				driverConn:            conn,
				globalState:           proxy.globalState,
				md:                    cl.md,
				codec:                 codec,
				rawCodec:              rawCodec,
				maxConcurrencyPerConn: opts.MaxConcurrencyPerConn,
			}

			go dc.handleConnection(ctx)
			proxy.nextConnectionID++
		}

		logger.Debug("Spanner proxy accept loop exited")
	}()

	return proxy, nil
}

// Addr returns the address of the proxy.
func (proxy *TCPProxy) Addr() net.Addr {
	return proxy.listener.Addr()
}

// Close closes the proxy.
func (proxy *TCPProxy) Close() {
	proxy.listener.Close()
}
