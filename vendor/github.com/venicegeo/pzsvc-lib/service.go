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
	"fmt"
	"net/http"
	"net/url"
)

// FindMySvc Searches Pz for a service matching the input information.  If it finds
// one, it returns the service ID.  If it does not, returns an empty string.  Currently
// searches on service name and submitting user.
func FindMySvc(svcName, pzAddr, authKey string) (string, error) {
	query := pzAddr + "/service/me?per_page=1000&keyword=" + url.QueryEscape(svcName)
	var respObj SvcList
	_, err := RequestKnownJSON("GET", "", query, authKey, &respObj)
	if err != nil {
		return "", TraceErr(err)
	}

	for _, checkServ := range respObj.Data {
		if checkServ.ResMeta.Name == svcName {
			return checkServ.ServiceID, nil
		}
	}

	return "", nil
}

// ManageRegistration Handles Pz registration for a service.  It checks the current
// service list to see if it has been registered already.  If it has not, it performs
// initial registration.  If it has not, it re-registers.  Best practice is to do this
// every time your service starts up.  For those of you code-reading, the filter is
// still somewhat rudimentary.  It will improve as better tools become available.
func ManageRegistration(svcName, svcDesc, svcURL, pzAddr, svcVers, authKey string,
	attributes map[string]string) error {

	fmt.Println("Finding")
	svcID, err := FindMySvc(svcName, pzAddr, authKey)
	if err != nil {
		return TraceErr(err)
	}

	svcClass := ClassType{"UNCLASSIFIED"} // TODO: this will have to be updated at some point.
	metaObj := ResMeta{Name: svcName,
		Description: svcDesc,
		ClassType:   svcClass,
		Version:     svcVers,
		Metadata:    make(map[string]string)}
	for key, val := range attributes {
		metaObj.Metadata[key] = val
	}
	svcObj := Service{ServiceID: svcID, URL: svcURL, Method: "POST", ResMeta: metaObj}
	svcJSON, err := json.Marshal(svcObj)

	if svcID == "" {
		fmt.Println("Registering")
		_, err = SubmitSinglePart("POST", string(svcJSON), pzAddr+"/service", authKey)
	} else {
		fmt.Println("Updating")
		_, err = SubmitSinglePart("PUT", string(svcJSON), pzAddr+"/service/"+svcID, authKey)
	}
	if err != nil {
		return TraceErr(err)
	}

	return nil
}

// TestPiazzaAuth returns an error if it is unable to authenticate
// with the gateway and authorization provided
func TestPiazzaAuth(pzGateway, auth string) error {

	if pzGateway == "" {
		return &HTTPError{Message: "This request requires a 'pzGateway'.", Status: http.StatusBadRequest}
	}
	_, err := SubmitSinglePart("GET", "", pzGateway+"/eventType", auth)
	return err
}
