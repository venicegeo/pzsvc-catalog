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

import (
	"log"
	"os"
	"testing"
)

// TestRedisClient tests the Redis connection
func TestRedisClient(t *testing.T) {
	vcapServicesStr := os.Getenv("VCAP_SERVICES")
	log.Printf("VCAP_SERVICES: %v", vcapServicesStr)
	if c, err := RedisClient(); err == nil {
		b := c.Exists("asdf")
		if _, err = b.Result(); err != nil {
			t.Error(err.Error())
		}
		key := prefix + ":SetKey"
		if err = SetKey(key, "foo"); err != nil {
			t.Error(err.Error())
		}
		var val string
		if val, err = GetKey(key); err != nil {
			t.Error(err.Error())
		} else if val != "foo" {
			t.Errorf("Expected foo, got %v", val)
		}
		c.Del(key)
	} else {
		t.Error(err.Error())
	}
}
