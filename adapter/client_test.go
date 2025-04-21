//go:build unit
// +build unit

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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetOrRefreshSession(t *testing.T) {
	t.Cleanup(ResetGrpcFuncs())
	MockCreateSessionGrpc("refreshed-session")
	ctx := context.Background()

	tests := []struct {
		name            string
		initialSession  session
		refreshInterval time.Duration
		wantSession     session
	}{
		{
			name: "Session exists and is valid",
			initialSession: session{
				name:       "valid-session",
				createTime: time.Now(),
			},
			refreshInterval: 10 * time.Hour,
			wantSession:     session{name: "valid-session"},
		},
		{
			name: "Session needs refresh",
			initialSession: session{
				name:       "about-to-expire-session",
				createTime: time.Now().Add(-time.Hour),
			},
			refreshInterval: 1 * time.Second,
			wantSession:     session{name: "refreshed-session"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SessionRefreshTimeInterval = tt.refreshInterval
			cl, err := newAdapterClient(context.Background(), Options{
				DatabaseUri:   "test",
				GoogleApiOpts: SkipAuthOpts,
			})
			assert.NoError(t, err)
			cl.session = tt.initialSession
			gotSession, err := cl.getOrRefreshSession(ctx)
			assert.NoError(t, err)
			if gotSession.name != tt.wantSession.name {
				t.Errorf(
					"getOrRefreshSession() session name = %v, want %v",
					gotSession.name,
					tt.wantSession.name,
				)
			}

		})
	}
}
