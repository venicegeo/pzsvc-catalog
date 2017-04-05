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

import (
	"encoding/json"
	//"fmt"
	//"net/http"
	//"net/url"
	"testing"
)

func TestManageRegistration(t *testing.T) {
	url := "http://testURL.net"
	authKey := "testAuthKey"
	svcName := "testJobID"
	svcDesc := "testDesc"
	svcURL := "http://testSvcURL.net"
	svcVers := "0.0"

	metaObj := ResMeta{Name: svcName,
		Description: svcDesc,
		ClassType:   ClassType{Classification: "Unclassified"},
		Version:     svcVers,
		Metadata:    make(map[string]string)}
	svcL1 := SvcList{Data: []Service{Service{ServiceID: "123", URL: svcURL, Method: "POST", ResMeta: metaObj}}}
	svcJSON, _ := json.Marshal(svcL1)

	outStrs := []string{string(svcJSON), `{"Data":[]}`}
	SetMockClient(outStrs, 250)

	sProps := map[string]string{"prop1": "1", "prop2": "2", "prop3": "3"}

	err := ManageRegistration(svcName, svcDesc, svcURL, url, svcVers, authKey, sProps)
	if err != nil {
		t.Error(`TestManageRegistration: failed on full registration.  Error: `, err.Error())
	}
	err = ManageRegistration(svcName, svcDesc, svcURL, url, svcVers, authKey, sProps)
	if err != nil {
		t.Error(`TestManageRegistration: failed on empty registration.  Error: `, err.Error())
	}
}
