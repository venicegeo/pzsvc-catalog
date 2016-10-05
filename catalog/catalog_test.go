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
	"testing"
	"time"

	"github.com/venicegeo/geojson-go/geojson"
)

const prefix = "catalog-test"

func TestBeachfront(t *testing.T) {
	var (
		id  string
		err error
	)
	properties := make(map[string]interface{})
	properties["name"] = "Whatever"

	SetImageCatalogPrefix(prefix)
	imageDescriptor := geojson.NewFeature(nil, "12345", properties)
	if id, err = StoreFeature(imageDescriptor, false); err != nil {
		t.Errorf("Failed to store feature: %v", err.Error())
	}
	if indexSize := IndexSize(); indexSize != 1 {
		t.Errorf("expected index size of 1, got %v", indexSize)
	}
	rc, _ := RedisClient()
	boolResult := rc.Exists(id)
	if !boolResult.Val() {
		t.Errorf("Where is the feature %v?", id)
	}

	sliceResult := rc.ZRange(prefix, 0, -1)
	if len(sliceResult.Val()) == 0 {
		t.Errorf("Why is the ordered set empty? %v", sliceResult.Err())
	}

	// NoCache search
	options := SearchOptions{MinimumIndex: 0, MaximumIndex: -1, NoCache: true}
	feature := geojson.NewFeature(nil, "", nil)
	scenes, _, _ := GetScenes(feature, options)
	if len(scenes.Scenes.Features) < 1 {
		t.Error("Where are the images?")
	}

	// Cache search
	options2 := SearchOptions{MinimumIndex: 0, MaximumIndex: -1}
	cacheName := getDiscoverCacheName(feature)
	if cacheName != `catalog-test{"type":"Feature","geometry":null,"properties":{}}` {
		t.Errorf("Unexpected cache name %v", cacheName)
	}
	go populateCache(feature, cacheName)
	for count := 0; ; count++ {
		if completeCache(cacheName, options2) {
			break
		}
		if count > 2 {
			t.Error("completeCache never completed")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	scenes2, _, _ := GetScenes(feature, options2)

	if len(scenes2.Scenes.Features) < 1 {
		t.Error("Where are the images?")
	}
}

func TestDropIndex(t *testing.T) {
	SetImageCatalogPrefix(prefix)
	DropIndex()
	options := SearchOptions{MinimumIndex: 0, MaximumIndex: -1, NoCache: true}
	feature := geojson.NewFeature(nil, "", nil)
	scenes, _, _ := GetScenes(feature, options)
	count := len(scenes.Scenes.Features)
	if count > 0 {
		t.Errorf("Expected 0 scenes but found %v.", count)
	}
}

func TestNilFeature(t *testing.T) {
	SetImageCatalogPrefix(prefix)
	options := SearchOptions{MinimumIndex: 0, MaximumIndex: -1, NoCache: true}
	if _, _, err := GetScenes(nil, options); err == nil {
		t.Errorf("Expected an error on a nil feature.")
	}
}
