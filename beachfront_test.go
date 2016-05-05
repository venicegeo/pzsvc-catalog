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

package main

import (
	"encoding/json"
	"testing"

	"github.com/venicegeo/pzsvc-catalog/catalog"
	"gopkg.in/redis.v3"
)

func TestBeachfront(t *testing.T) {
	var (
		err       error
		idmBytes  []byte
		red       *redis.Client
		idm, idID string
	)
	setName := "test_images"
	imageDescriptor := &catalog.ImageDescriptor{ID: "12345", Name: "Whatever"}

	if red = catalog.RedisClient(nil); red == nil {
		t.Fatal("Failed to create Redis client")
	}
	defer red.Close()

	if idmBytes, err = json.Marshal(imageDescriptor); err != nil {
		t.Error(err)
	}
	idm = string(idmBytes)
	idID = "test" + imageDescriptor.ID
	red.Set(idID, idm, 0)
	red.SAdd(setName, idID)

	images := catalog.GetImages(setName, nil)

	t.Logf("%#v", images)
	if len(images) < 1 {
		t.Error("Where are the images?")
	}
	for _, curr := range images {
		t.Logf("%v", curr)
	}
	red.Del(setName)
}
