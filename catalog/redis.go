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
	"encoding/json"
	"log"
	"os"
	"strconv"

	"gopkg.in/redis.v3"
)

var client *redis.Client
var clientError error

// RedisClient is a factory method for a Redis instance
func RedisClient() (*redis.Client, error) {
	if client == nil {
		vcapServicesStr := os.Getenv("VCAP_SERVICES")
		var vcapServices VcapServices
		if err := json.Unmarshal([]byte(vcapServicesStr), &vcapServices); err != nil {
			return nil, err
		}
		client = redis.NewClient(vcapServices.RedisOptions())
	}
	return client, nil
}

// RedisError closes down the connection so that other operations can
// fail somewhat gracefully. We should never fail in normal settings.
func RedisError(red *redis.Client, err error) {

	if clientError == nil {
		clientError = err
	}
	log.Printf("Redis operation failed: %v", clientError.Error())
	red.Close()
}

// VcapServices is the container for the VCAP_SERVICES environment variable
type VcapServices struct {
	Redis []VcapRedis `json:"p-redis"`
}

// VcapRedis is the p-redis element of VCAP_SERVICES
type VcapRedis struct {
	// Label       string          `json:"label"`
	// Name        string          `json:"name"`
	// Plan        string          `json:"plan"`
	// Tags        []string        `json:"tags"`
	Credentials VcapCredentials `json:"credentials"`
}

// VcapCredentials is the credentials element of VCAP_SERVICES
type VcapCredentials struct {
	Host     string `json:"host"`
	Password string `json:"password"`
	Port     int    `json:"port"`
}

// RedisOptions is a factory method for redis.Options
// If the object is not complete, a default of 127.0.0.1:6379 is returned
func (services VcapServices) RedisOptions() *redis.Options {
	var (
		result redis.Options
	)
	log.Printf("Received Redis options of: %#v", services)
	ok := true
	if len(services.Redis) == 0 {
		ok = false
	} else {
		redis := services.Redis[0]
		result.Password = redis.Credentials.Password
		if redis.Credentials.Host == "" {
			ok = false
		} else {
			addr := redis.Credentials.Host + ":" + strconv.FormatInt(int64(redis.Credentials.Port), 10)
			result.Addr = addr
		}
	}
	if !ok {
		result.Addr = "127.0.0.1:6379"
	}
	log.Printf("Interpreted Redis options as: %#v", result)
	return &result
}
