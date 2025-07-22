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

import "google.golang.org/api/option"

// Options for configuring the adapter.
type Options struct {
	// Spanner database uri to connect to.
	DatabaseUri string
	// Optional Spanner service endpoint. Defaults to spanner.googleapis.com:443
	SpannerEndpoint string
	// Protocol type (ie: cassandra).
	Protocol Protocol
	// Number of channels when dial grpc connection. Defaults to 4.
	NumGrpcChannels int
	// Optional Endpoint to start TCP server. Defaults to localhost:9042
	TCPEndpoint string
	// Optional boolean indicate whether to disable automatic grpc retry for
	// AdaptMessage API. Defauls to false.
	DisableAdaptMessageRetry bool
	// The maximum delay in milliseconds. Default is 0 (disabled).
	MaxCommitDelay int
	// Optional google api opts. Default to empty.
	GoogleApiOpts []option.ClientOption
}
