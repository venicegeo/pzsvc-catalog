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

package pzsvc

import

//"fmt"
//"net/http"
//"net/url"
(
	//"bytes"
	"net/http"
	"testing"
)

func TestEventType(t *testing.T) {
	var dummyMap map[string]interface{}
	_, err := GetEventType("EVENT", dummyMap, "Test", "Test")
	if err != nil {
		t.Error("GetEventType Failed")
	}
	rr, _, _ := GetMockResponseWriter()
	r := http.Request{}
	r.Method = "POST"
	r.Body = GetMockReadCloser(`{"name":what?}`)
	WriteEventTypes(rr, &r)

}

func TestEvent(t *testing.T) {
	_, err := Events("EVENT", "Test", "Test")
	if err != nil {
		t.Error("Events Failed")
	}
	var dummyEvent Event
	_, err2 := AddEvent(dummyEvent, "Test", "Test")
	if err2 != nil {
		t.Error("AddEvent Failed")
	}
}
func TestGetAletrs(t *testing.T) {
	_, err := GetAlerts("Test", "Test", "Test", "Test", "Test")
	if err != nil {
		t.Error("GetAlerts Failed")
	}
}
func TestAddTrigger(t *testing.T) {
	var dummyTrigger Trigger
	_, err := AddTrigger(dummyTrigger, "Test", "Test")
	if err != nil {
		t.Error("AddTrigger Failed")
	}
}
