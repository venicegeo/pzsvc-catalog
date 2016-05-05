// Copyright 2016, RadiantBlue Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import "gopkg.in/redis.v3"

var client *redis.Client

// RedisClient is a factory method for a Redis instance
func RedisClient(options *redis.Options) *redis.Client {
	if client == nil {
		if options == nil {
			options = &redis.Options{Addr: "127.0.0.1:6379"}
		}
		client = redis.NewClient(options)
	}
	return client
}
