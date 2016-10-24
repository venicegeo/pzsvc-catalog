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
const imageID = "12345"

func TestBeachfront(t *testing.T) {
	var (
		id  string
		err error
	)
	properties := make(map[string]interface{})
	properties["name"] = "Whatever"

	if _, err = RedisClient(); err != nil {
		t.Fatal("Can't find Redis.")
	}

	SetImageCatalogPrefix(prefix)
	if _, err = GetSceneMetadata(imageID); err == nil {
		t.Errorf("Expected to not find scene")
	}
	imageDescriptor := geojson.NewFeature(nil, imageID, properties)
	if id, err = StoreFeature(imageDescriptor, false); err != nil {
		t.Errorf("Failed to store feature: %v", err.Error())
	}
	if indexSize := IndexSize(); indexSize != 1 {
		t.Errorf("expected index size of 1, got %v", indexSize)
	}
	if _, err = GetSceneMetadata(imageID); err != nil {
		t.Errorf("Expected to find scene")
	}
	if _, err = StoreFeature(imageDescriptor, false); err == nil {
		t.Errorf("Expected an error since imageDescriptor is already there")
	}
	if _, err = StoreFeature(imageDescriptor, true); err != nil {
		t.Errorf("Expected to re-store image but instead received %v", err.Error())
	}
	properties["foo"] = "bar"
	if err = SaveFeatureProperties(imageID, properties); err != nil {
		t.Errorf("Failed to save feature properties for %v: %v", id, err.Error())
	}

	// NoCache search
	options := SearchOptions{MinimumIndex: 0, MaximumIndex: -1, NoCache: true}
	feature := geojson.NewFeature(nil, nil, nil)
	scenes, _, _ := GetScenes(feature, options)
	if len(scenes.Scenes.Features) < 1 {
		t.Error("Where are the images?")
	}

	// Cache search
	options2 := SearchOptions{MinimumIndex: 0, MaximumIndex: -1}
	cacheName := getDiscoverCacheName(feature)
	if cacheName != `catalog-test{"type":"Feature","geometry":null}` {
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

	if err = RemoveFeature(imageDescriptor); err != nil {
		t.Errorf("Failed to remove feature %v: %v", id, err.Error())
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
