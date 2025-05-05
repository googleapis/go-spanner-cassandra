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
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	vkit "cloud.google.com/go/spanner/adapter/apiv1"
	"cloud.google.com/go/spanner/adapter/apiv1/adapterpb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"

	"google.golang.org/grpc/metadata"
	_ "google.golang.org/grpc/xds/googledirectpath"

	// Install RLS load balancer policy, which is needed for gRPC RLS.
	_ "google.golang.org/grpc/balancer/rls"
)

const (
	// defaultSpannerEndpoint is the default spanner APIs grpc endpoint.
	defaultSpannerEndpoint = "spanner.googleapis.com:443"
	// current version
	version = "0.3.0" // x-release-please-version
	// resourcePrefixHeader is the name of the metadata header used to indicate
	// the resource being operated on.
	resourcePrefixHeader = "google-cloud-resource-prefix"
	// routeToLeaderHeader is the name of the metadata header if given
	// batch/execute/query message need to route to leader.
	routeToLeaderHeader = "x-goog-spanner-route-to-leader"
)

var (
	// SessionRefreshTimeInterval defines the interval for refreshing Adapter
	// sessions. Adapter Sessions have a 7-day lifetime and are refreshed 1 day
	// before expiry to provide a buffer against potential delays.
	SessionRefreshTimeInterval = 6 * 24 * time.Hour
	CreateSessionGrpc          = func(ctx context.Context, req *adapterpb.CreateSessionRequest, cl *AdapterClient) (*adapterpb.Session, error) {
		var md metadata.MD
		resp, err := cl.gapicClient.CreateSession(
			ctx,
			req,
			gax.WithGRPCOptions(grpc.Header(&md)),
		)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
)

// The adapterClient encapsulates the gRPC connection / adapter stub creation.
// It is also responsible for refreshing the multiplexed session.
type AdapterClient struct {
	opts        Options
	gapicClient *vkit.Client
	md          metadata.MD

	mu      sync.RWMutex
	session session
}

type session struct {
	name       string
	createTime time.Time
}

func contextWithOutgoingMetadata(
	ctx context.Context,
	md metadata.MD,
	enableRouteToLeader bool,
) context.Context {
	existing, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		md = metadata.Join(existing, md)
	}
	if enableRouteToLeader {
		md = metadata.Join(md, metadata.Pairs(routeToLeaderHeader, "true"))
	}
	return metadata.NewOutgoingContext(ctx, md)
}

func newAdapterClient(
	ctx context.Context,
	opts Options,
) (*AdapterClient, error) {
	// Create a client.
	cl := &AdapterClient{
		opts: opts,
		md:   metadata.Pairs(resourcePrefixHeader, opts.DatabaseUri),
	}

	// Build grpc options.
	dialOpts := getAllClientOpts(opts)

	// Create a default gapic client.
	var err error
	cl.gapicClient, err = vkit.NewClient(ctx, dialOpts...)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// TODO: Export a generated client opts function from
// google-cloud-go/spanner/adapter rather than manually constructing here
func generatedGRPCClientOptions() []option.ClientOption {
	return []option.ClientOption{
		internaloption.WithDefaultEndpoint("spanner.googleapis.com:443"),
		internaloption.WithDefaultEndpointTemplate("spanner.UNIVERSE_DOMAIN:443"),
		internaloption.WithDefaultMTLSEndpoint("spanner.mtls.googleapis.com:443"),
		internaloption.WithDefaultUniverseDomain("googleapis.com"),
		internaloption.WithDefaultAudience("https://spanner.googleapis.com/"),
		internaloption.WithDefaultScopes(vkit.DefaultAuthScopes()...),
		internaloption.EnableJwtWithScope(),
		internaloption.EnableNewAuthLibrary(),
		option.WithGRPCDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32))),
	}
}

// Combines the default options from the generated client, the default options
// of the hand-written client and the user options to one list of options.
// Precedence: user provided GoogleApiOpts > clientDefaultOpts >
// generatedDefaultOpts
func getAllClientOpts(
	opts Options,
) []option.ClientOption {
	if opts.SpannerEndpoint == "" {
		opts.SpannerEndpoint = defaultSpannerEndpoint
	}

	generatedDefaultOpts := generatedGRPCClientOptions()
	clientDefaultOpts := []option.ClientOption{
		option.WithEndpoint(opts.SpannerEndpoint),
		option.WithGRPCConnectionPool(opts.NumGrpcChannels),
		option.WithUserAgent(
			fmt.Sprintf("go-spanner-cassandra/v%s", version),
		),
		option.WithGRPCConnectionPool(opts.NumGrpcChannels),
		internaloption.AllowNonDefaultServiceAccount(true),
	}

	if enableDirectPathXds, _ := strconv.ParseBool(os.Getenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS")); enableDirectPathXds {
		clientDefaultOpts = append(
			clientDefaultOpts,
			internaloption.EnableDirectPath(true),
			internaloption.EnableDirectPathXds(),
		)
	}

	allDefaultOpts := append(generatedDefaultOpts, clientDefaultOpts...)

	return append(allDefaultOpts, opts.GoogleApiOpts...)
}

func (cl *AdapterClient) getMetadata() metadata.MD {
	return cl.md
}

func (cl *AdapterClient) getSession() session {
	return cl.session
}

func (cl *AdapterClient) setSession(s session) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.session = s
}

func (cl *AdapterClient) createSession(ctx context.Context,
	opts Options) error {
	req := &adapterpb.CreateSessionRequest{
		Parent:  opts.DatabaseUri,
		Session: &adapterpb.Session{},
	}

	err := RunCreateAdapterSessionWithRetry(
		ctx,
		func(ctx context.Context) error {
			createTime := time.Now()
			ctxWithMd := contextWithOutgoingMetadata(
				ctx,
				cl.getMetadata(),
				false,
			)
			resp, err := CreateSessionGrpc(
				ctxWithMd,
				req,
				cl,
			)
			if err != nil {
				return err
			}
			cl.setSession(session{resp.Name, createTime})
			return nil
		},
	)
	if err != nil {
		return err
	}
	return nil
}

// Gets the current Adapter session that should be used for all requests.
// Refresh the session if the current session is about to expire.
func (cl *AdapterClient) getOrRefreshSession(
	ctx context.Context,
) (session, error) {
	currentSession := cl.getSession()

	if time.Now().
		After(currentSession.createTime.Add(SessionRefreshTimeInterval)) {
		if err := cl.createSession(ctx, cl.opts); err != nil {
			return session{}, err
		}
		return cl.getSession(), nil
	}
	return currentSession, nil
}
