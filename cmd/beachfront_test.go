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

package cmd

import (
	"testing"

	"github.com/venicegeo/geojson-go/geojson"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

const prefix = "test_images"

func TestBeachfront(t *testing.T) {
	properties := make(map[string]interface{})
	properties["name"] = "Whatever"

	catalog.SetImageCatalogPrefix(prefix)
	imageDescriptor := geojson.NewFeature(nil, "12345", properties)
	catalog.StoreFeature(imageDescriptor, -5)
	rc, _ := catalog.RedisClient()
	boolResult := rc.Exists(prefix + ":" + imageDescriptor.ID)
	if !boolResult.Val() {
		t.Error("Where is the feature?")
	}
	sliceResult := rc.ZRange(prefix, 0, -1)
	if len(sliceResult.Val()) == 0 {
		t.Errorf("Why is the ordered set empty? %v", sliceResult.Err())
	}

	images, _, _ := catalog.GetImages(nil, 0, -1)

	if len(images.Images.Features) < 1 {
		t.Error("Where are the images?")
	}
	for _, curr := range images.Images.Features {
		t.Logf("%v", curr)
	}
	// rc.Del(prefix)
}
