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
	"os"
	"testing"

	"github.com/venicegeo/geojson-go/geojson"
	//"time"
)

const prefix = "catalog-test"
const imageID = "12345"

func TestBeachfront(t *testing.T) {
	var (
		//id  string
		err error
	)
	SetMockConnCount(0)

	outputs := []string{
		RedisConvErrStr("12345"),
		RedisConvInt(5),
		RedisConvErrStr("Failure"),
		RedisConvStatus("Ok"),
		RedisConvString("Alrite"),
	}
	client = MakeMockRedisCli(outputs)
	properties := make(map[string]interface{})
	os.Setenv("VCAP_SERVICES", "{\"p-redis\":[{\"credentials\":{\"host\":\"127.0.0.1\",\"port\":6379}}]}")
	os.Setenv("PL_API_KEY", "a1fa3d8df30545468052e45ae9e4520e")
	properties["maxAcquiredDate"] = "2002-10-02T15:00:00.05Z"
	properties["acquiredDate"] = "2002-10-02T15:00:00.05Z"
	if _, err = RedisClient(); err != nil {
		//t.Fatal("Can't find Redis.")
	}

	SetImageCatalogPrefix(prefix)
	if _, err = GetSceneMetadata(imageID); err == nil {
		//t.Errorf("Expected to not find scene")
	}
	var geoCollectionHolder *geojson.FeatureCollection
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"},"properties":{"24hrMaxTide":"4.272558868170382","acquiredDate":"2016-10-11T12:59:05.157475+00:00","24hrMinTide":"2.4257490639311676","algoCmd":"ossim-cli shoreline --image img1.TIF,img2.TIF --projection geo-scaled --prop 24hrMinTide:2.4257490639311676 --prop resolution:30 --prop classification:Unclassified --prop dataUsage:Not_to_be_used_for_navigational_or_targeting_purposes. --prop sensorName:Landsat8 --prop 24hrMaxTide:4.272558868170382 --prop currentTide:3.4136017245233523 --prop sourceID:landsat:LC82190622016285LGN00 --prop dateTimeCollect:2016-10-11T12:59:05.157475+00:00 shoreline.geojson","algoName":"BF_Algo_NDWI","algoProcTime":"20161031.133058.4026","algoVersion":"0.0","classification":"Unclassified","currentTide":"3.4136017245233523","dataUsage":"Not_to_be_used_for_navigational_or_targeting_purposes.","dateTimeCollect":"2016-10-11T12:59:05.157475+00:00","resolution":"30","sensorName":"Landsat8","sourceID":"landsat:LC82190622016285LGN00"}}]}`))
	geoFeatureArray := geoCollectionHolder.Features
	for _, imageDescriptor := range geoFeatureArray {
		SetMockConnCount(0)
		outputs := []string{
			RedisConvString("Alrite,ok,no,22"),
		}
		println(imageDescriptor.PropertyString("maxAcquiredDate"))
		imageDescriptor := geojson.NewFeature(imageDescriptor, imageID, properties)
		if _, err = StoreFeature(imageDescriptor, false); err != nil {
			//t.Errorf("Failed to store feature: %v", err.Error())
		}

		if indexSize := IndexSize(); indexSize != 1 {
			//t.Errorf("expected index size of 1, got %v", indexSize)
		}
		if _, err = GetSceneMetadata(imageID); err != nil {
			//t.Errorf("Expected to find scene")
		}
		if _, err = StoreFeature(imageDescriptor, false); err == nil {
			//t.Errorf("Expected an error since imageDescriptor is already there")
		}
		if _, err = StoreFeature(imageDescriptor, true); err != nil {
			//t.Errorf("Expected to re-store image but instead received %v", err.Error())
		}
		SetMockConnCount(0)
		outputs = []string{
			RedisConvErrStr("12345"),
		}
		client = MakeMockRedisCli(outputs)
		properties["foo"] = "bar"
		if err = SaveFeatureProperties(imageID, properties); err != nil {
			//t.Errorf("Failed to save feature properties for %v: %v", id, err.Error())
		}

		SetMockConnCount(0)
		outputs = []string{
			RedisConvErrStr("12345"),
		}
		client = MakeMockRedisCli(outputs)
		// NoCache search
		options := SearchOptions{MinimumIndex: 0, MaximumIndex: -1, NoCache: true}
		feature := geojson.NewFeature(nil, nil, nil)
		_, _, _ = GetScenes(feature, options)

		// Cache search
		//options2 := SearchOptions{MinimumIndex: 0, MaximumIndex: -1}
		cacheName := getDiscoverCacheName(feature)
		if cacheName != `catalog-test{"type":"Feature","geometry":null}` {
			//t.Errorf("Unexpected cache name %v", cacheName)
		}
		//go populateCache(feature, cacheName)
		SetMockConnCount(0)
		outputs = []string{
			RedisConvErrStr("Failure"),
		}
		client = MakeMockRedisCli(outputs)
		//for count := 0; ; count++ {
		//	if completeCache(cacheName, options2) {
		//		break
		//	}
		//	if count > 2 {
		//		t.Error("completeCache never completed")
		//		break
		//	}
		//	time.Sleep(100 * time.Millisecond)
		//}

		//_, _, _ = GetScenes(feature, options2)

		if err = RemoveFeature(imageDescriptor); err != nil {
			//t.Errorf("Failed to remove feature %v: %v", id, err.Error())
		}
	}
}

func TestDropIndex(t *testing.T) {
	SetMockConnCount(0)
	outputs := []string{
		RedisConvErrStr("12345"),
	}
	client = MakeMockRedisCli(outputs)
	SetImageCatalogPrefix(prefix)
	DropIndex()
	//options := SearchOptions{MinimumIndex: 0, MaximumIndex: -1, NoCache: true}
	//feature := geojson.NewFeature(nil, "", nil)
	//scenes, _, _ := GetScenes(feature, options)
	//count := len(scenes.Scenes.Features)
	//if count > 0 {
	//	t.Errorf("Expected 0 scenes but found %v.", count)
	//}
}
func TestGetResults(t *testing.T) {
	var optionsHolder SearchOptions
	var geoCollectionHolder *geojson.FeatureCollection
	optionsHolder.Count = 2
	optionsHolder.MaximumIndex = 5
	optionsHolder.MinimumIndex = 1
	optionsHolder.NoCache = true
	optionsHolder.Rigorous = true
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"},"properties":{"24hrMaxTide":"4.272558868170382","24hrMinTide":"2.4257490639311676","acquiredDate":"2002-10-02T10:00:00-05:00","algoCmd":"ossim-cli shoreline --image img1.TIF,img2.TIF --projection geo-scaled --prop 24hrMinTide:2.4257490639311676 --prop resolution:30 --prop classification:Unclassified --prop dataUsage:Not_to_be_used_for_navigational_or_targeting_purposes. --prop sensorName:Landsat8 --prop 24hrMaxTide:4.272558868170382 --prop currentTide:3.4136017245233523 --prop sourceID:landsat:LC82190622016285LGN00 --prop dateTimeCollect:2016-10-11T12:59:05.157475+00:00 shoreline.geojson","algoName":"BF_Algo_NDWI","algoProcTime":"20161031.133058.4026","algoVersion":"0.0","classification":"Unclassified","currentTide":"3.4136017245233523","dataUsage":"Not_to_be_used_for_navigational_or_targeting_purposes.","dateTimeCollect":"2016-10-11T12:59:05.157475+00:00","resolution":"30","sensorName":"Landsat8","sourceID":"landsat:LC82190622016285LGN00"}}]}`))
	geoFeatureArray := geoCollectionHolder.Features
	for _, feature := range geoFeatureArray {
		SetMockConnCount(0)
		outputs := []string{
			RedisConvInt(2),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvArray(),
			RedisConvInt(2),
			RedisConvInt(2),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvInt(0),
			RedisConvArray(),
			RedisConvInt(-1),
		}
		client = MakeMockRedisCli(outputs)
		_, _, _ = getResults(feature, optionsHolder)
	}

}

func TestNilFeature(t *testing.T) {
	SetImageCatalogPrefix(prefix)
	options := SearchOptions{MinimumIndex: 0, MaximumIndex: -1, NoCache: true}
	if _, _, err := GetScenes(nil, options); err == nil {
		//t.Errorf("Expected an error on a nil feature.")
	}
}

func TestGetScenes(t *testing.T) {
	var optionsHolder SearchOptions
	var geoCollectionHolder *geojson.FeatureCollection
	optionsHolder.Count = 2
	optionsHolder.MaximumIndex = 5
	optionsHolder.MinimumIndex = 1
	optionsHolder.NoCache = true
	optionsHolder.Rigorous = true
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"},"properties":{"24hrMaxTide":"4.272558868170382","24hrMinTide":"2.4257490639311676","algoCmd":"ossim-cli shoreline --image img1.TIF,img2.TIF --projection geo-scaled --prop 24hrMinTide:2.4257490639311676 --prop resolution:30 --prop classification:Unclassified --prop dataUsage:Not_to_be_used_for_navigational_or_targeting_purposes. --prop sensorName:Landsat8 --prop 24hrMaxTide:4.272558868170382 --prop currentTide:3.4136017245233523 --prop sourceID:landsat:LC82190622016285LGN00 --prop dateTimeCollect:2016-10-11T12:59:05.157475+00:00 shoreline.geojson","algoName":"BF_Algo_NDWI","algoProcTime":"20161031.133058.4026","algoVersion":"0.0","classification":"Unclassified","currentTide":"3.4136017245233523","dataUsage":"Not_to_be_used_for_navigational_or_targeting_purposes.","dateTimeCollect":"2016-10-11T12:59:05.157475+00:00","resolution":"30","sensorName":"Landsat8","sourceID":"landsat:LC82190622016285LGN00"}}]}`))
	geoFeatureArray := geoCollectionHolder.Features
	for _, feature := range geoFeatureArray {
		SetMockConnCount(0)
		outputs := []string{
			RedisConvInt(2),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvArray(),
			RedisConvInt(2),
			RedisConvInt(2),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvInt(2),
			RedisConvArray(),
			RedisConvInt(0),
			RedisConvArray(),
			RedisConvInt(-1),
		}
		client = MakeMockRedisCli(outputs)
		_, _, _ = GetScenes(feature, optionsHolder)
	}

}

func TestCompleteCache(t *testing.T) {
	var stringHolder = "Test"
	var optionsHolder SearchOptions
	optionsHolder.Count = 1
	optionsHolder.MaximumIndex = 5
	optionsHolder.MinimumIndex = 1
	optionsHolder.NoCache = true
	optionsHolder.Rigorous = true
	SetMockConnCount(0)

	outputs := []string{
		RedisConvInt(2),
		RedisConvArray(),
		RedisConvArray(),
		RedisConvArray(),
		RedisConvInt(2),
		RedisConvArray(),
		RedisConvInt(2),
		RedisConvArray(),
		RedisConvInt(0),
		RedisConvArray(),
		RedisConvInt(-1),
	}
	client = MakeMockRedisCli(outputs)
	completeCache(stringHolder, optionsHolder)
}

func TestPopulateCachet(t *testing.T) {
	var optionsHolder SearchOptions
	var geoCollectionHolder *geojson.FeatureCollection
	optionsHolder.Count = 2
	optionsHolder.MaximumIndex = 5
	optionsHolder.MinimumIndex = 1
	optionsHolder.NoCache = true
	optionsHolder.Rigorous = true
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"}}]}`))
	geoFeatureArray := geoCollectionHolder.Features
	for _, feature := range geoFeatureArray {
		SetMockConnCount(0)
		outputs := []string{
			RedisConvArray(),
			RedisConvInt(0),
			RedisConvString("Alrite,ok,no,22"),
		}
		client = MakeMockRedisCli(outputs)
		populateCache(feature, "Test")
	}

}

func TestRemoveFeature(t *testing.T) {
	var geoCollectionHolder *geojson.FeatureCollection
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"},"properties":{"24hrMaxTide":"4.272558868170382","24hrMinTide":"2.4257490639311676","algoCmd":"ossim-cli shoreline --image img1.TIF,img2.TIF --projection geo-scaled --prop 24hrMinTide:2.4257490639311676 --prop resolution:30 --prop classification:Unclassified --prop dataUsage:Not_to_be_used_for_navigational_or_targeting_purposes. --prop sensorName:Landsat8 --prop 24hrMaxTide:4.272558868170382 --prop currentTide:3.4136017245233523 --prop sourceID:landsat:LC82190622016285LGN00 --prop dateTimeCollect:2016-10-11T12:59:05.157475+00:00 shoreline.geojson","algoName":"BF_Algo_NDWI","algoProcTime":"20161031.133058.4026","algoVersion":"0.0","classification":"Unclassified","currentTide":"3.4136017245233523","dataUsage":"Not_to_be_used_for_navigational_or_targeting_purposes.","dateTimeCollect":"2016-10-11T12:59:05.157475+00:00","resolution":"30","sensorName":"Landsat8","sourceID":"landsat:LC82190622016285LGN00"}}]}`))
	geoFeatureArray := geoCollectionHolder.Features
	for _, feature := range geoFeatureArray {
		SetMockConnCount(0)
		outputs := []string{
			RedisConvString("Alrite,ok,no,22"),
		}
		client = MakeMockRedisCli(outputs)
		_ = RemoveFeature(feature)
	}

}

func TestPassImageDescriptor(t *testing.T) {
	var geoCollectionHolder *geojson.FeatureCollection
	geoCollectionHolder, _ = geojson.FeatureCollectionFromBytes([]byte(`{"type": "FeatureCollection","features":[{"type":"Feature","geometry":{"coordinates":[[-41.68380384,-3.86901559],[-41.68344951,-3.86733807],[-41.68361042,-3.86726774],[-41.68384764,-3.86719616],[-41.68413582,-3.86716065],[-41.68444963,-3.86719857],[-41.68476372,-3.86734723],[-41.68505276,-3.86764398],[-41.68529141,-3.86812615],[-41.68537007,-3.86836772],[-41.68542289,-3.8685737],[-41.68544916,-3.86875115],[-41.68544817,-3.86890711],[-41.6854192,-3.86904862],[-41.68536156,-3.86918273],[-41.68527452,-3.86931649],[-41.68515738,-3.86945693],[-41.68495458,-3.86964114],[-41.68475013,-3.86975328],[-41.68454967,-3.8697952],[-41.68435881,-3.86976873],[-41.68418317,-3.86967571],[-41.68402839,-3.86951795],[-41.68390007,-3.8692973],[-41.68380384,-3.86901559]],"type":"LineString"},"properties":{"24hrMaxTide":"4.272558868170382","24hrMinTide":"2.4257490639311676","algoCmd":"ossim-cli shoreline --image img1.TIF,img2.TIF --projection geo-scaled --prop 24hrMinTide:2.4257490639311676 --prop resolution:30 --prop classification:Unclassified --prop dataUsage:Not_to_be_used_for_navigational_or_targeting_purposes. --prop sensorName:Landsat8 --prop 24hrMaxTide:4.272558868170382 --prop currentTide:3.4136017245233523 --prop sourceID:landsat:LC82190622016285LGN00 --prop dateTimeCollect:2016-10-11T12:59:05.157475+00:00 shoreline.geojson","algoName":"BF_Algo_NDWI","algoProcTime":"20161031.133058.4026","algoVersion":"0.0","classification":"Unclassified","currentTide":"3.4136017245233523","dataUsage":"Not_to_be_used_for_navigational_or_targeting_purposes.","dateTimeCollect":"2016-10-11T12:59:05.157475+00:00","resolution":"30","sensorName":"Landsat8","sourceID":"landsat:LC82190622016285LGN00"}}]}`))
	geoFeatureArray := geoCollectionHolder.Features
	for _, feature := range geoFeatureArray {
		SetMockConnCount(0)
		outputs := []string{
			RedisConvString("Alrite,ok,no,22"),
		}
		client = MakeMockRedisCli(outputs)
		_ = passImageDescriptor(feature, feature, true)
		_ = passImageDescriptor(feature, feature, false)
	}

}
