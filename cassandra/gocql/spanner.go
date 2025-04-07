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

// Package spanner implements a thin proxy for CQL <-> gRPC Spanner.
package spanner

import (
	"encoding/binary"
	"net"
	"time"

	"github.com/gocql/gocql"
	"github.com/googleapis/go-spanner-cassandra/adapter"
)

// Map from cluster config to local proxies.
var proxyMap = make(
	map[*gocql.ClusterConfig]*adapter.TCPProxy,
)

// Options represents the configuration for a virtual Spanner cluster.
type Options struct {
	SpannerEndpoint string
	TCPEndpoint     string
	DatabaseUri     string
	// Number of channels when dial grpc connection.
	NumGrpcChannels int
	// Whether to disable automatic grpc retry for AdaptMessage API
	DisableAdaptMessageRetry bool
}

type ProxyAddressTranslator struct {
	proxyIP   net.IP
	proxyPort int
}

func (t *ProxyAddressTranslator) Translate(ip net.IP, port int) (net.IP, int) {
	// Redirect all connections to the proxy
	return t.proxyIP, t.proxyPort
}

// NewCluster returns a new cluster for the CQL driver.
func NewCluster(
	opts *Options,
) *gocql.ClusterConfig {
	// Create a new local Cassandra proxy.
	proxy, err := adapter.NewTCPProxy(
		adapter.Options{
			DatabaseUri:              opts.DatabaseUri,
			SpannerEndpoint:          opts.SpannerEndpoint,
			TCPEndpoint:              opts.TCPEndpoint,
			Protocol:                 &cassandraProtocol{},
			NumGrpcChannels:          opts.NumGrpcChannels,
			DisableAdaptMessageRetry: opts.DisableAdaptMessageRetry,
		},
	)
	if err != nil {
		panic(
			err,
		)
	}

	// Point the driver to this local proxy.
	//
	// TODO: Passing proxy.Addr().String() to NewCluster does not work, the port
	// has to be specified explicitly. This is likely because the driver uses data
	// returned from the peers query to determine the address to connect to. We
	// should probably find a better scheme for this.
	addr := proxy.Addr().(*net.TCPAddr)
	cfg := gocql.NewCluster(
		addr.IP.String(),
	)
	cfg.Port = addr.Port
	cfg.ProtoVersion = 4

	cfg.WriteCoalesceWaitTime = 0

	// Override default timeout settings.
	cfg.Timeout = 60 * time.Second
	cfg.ConnectTimeout = 60 * time.Second

	// Record the mapping between the cluster and the proxy.
	proxyMap[cfg] = proxy

	return cfg
}

// CloseCluster closes the local proxy for the given cluster.
func CloseCluster(
	cfg *gocql.ClusterConfig,
) {
	if proxy, ok := proxyMap[cfg]; ok {
		proxy.Close()
		delete(
			proxyMap,
			cfg,
		)
	}
}

type cassandraProtocol struct {
}

func (ca *cassandraProtocol) Name() string {
	return "cassandra"
}

func (ca *cassandraProtocol) FrameHeaderLength() int {
	return 9
}

func (ca *cassandraProtocol) FrameBodyLength(header []byte) int {
	return int(binary.BigEndian.Uint32(header[5:9]))
}

func (ca *cassandraProtocol) ExtractKeys(payload []byte) []string {
	// TODO: Bounds check.
	if payload[4] != 0x0A {
		return nil
	}

	idLen := int(binary.BigEndian.Uint16(payload[9:11]))
	id := string(payload[11 : 11+idLen])

	return []string{id}
}
