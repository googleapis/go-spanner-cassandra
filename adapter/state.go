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
	"github.com/googleapis/go-spanner-cassandra/adapter/apiv1/adapterpb"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	lru "github.com/hashicorp/golang-lru"
)

// State maintained for a single request.
type requestState struct {
	pb    *adapterpb.AdaptMessageRequest
	frame frame.Frame
}

// globalStateEntry is a thread safe states cache maintained across all
// requests.
type globalState struct {
	cache *lru.Cache
}

// NewDefaultGlobalState creates a new default prepared cache capping the max
// item capacity to `size`.
func NewDefaultGlobalState(size int) (*globalState, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &globalState{cache}, nil
}

func (d globalState) Store(key string, val string) {
	d.cache.Add(key, val)
}

func (d globalState) Load(key string) (val string, ok bool) {
	if val, ok := d.cache.Get(key); ok {
		return val.(string), true
	}
	return "nil", false
}
