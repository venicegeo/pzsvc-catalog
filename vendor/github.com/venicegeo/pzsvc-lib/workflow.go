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
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
)

var (
	eventTypeMap = make(map[string]EventType)
)

// GetEventType returns the event type ID and fully qualified name
// for the specified EventType and its root
func GetEventType(root string, mapping map[string]interface{}, pzGateway, auth string) (EventType, error) {
	var (
		err            error
		eventTypes     EventTypeList
		result         EventType
		ok             bool
		foundDeepMatch bool
		foundMatch     bool
		bytes          []byte
	)
	if result, ok = eventTypeMap[root]; ok {
		return result, nil
	}
	if bytes, err = RequestKnownJSON("GET", "", pzGateway+"/eventType?perPage=10000", auth, &eventTypes); err != nil {
		return result, ErrWithTrace(err.Error() + "\n" + string(bytes))
	}

	// Look for an event type with the same root and same mapping
	// and add the EventType if needed
	for version := 0; ; version++ {
		foundMatch = false
		eventTypeName := fmt.Sprintf("%v:%v", root, version)
		for _, eventType := range eventTypes.Data {
			if eventType.Name == eventTypeName {
				foundMatch = true
				if reflect.DeepEqual(eventType.Mapping, mapping) {
					foundDeepMatch = true
					fmt.Printf("Found deep match for Event Type %v: %v", eventTypeName, eventType.EventTypeID)
					result = eventType
				}
				break
			}
		}
		if foundDeepMatch {
			break
		}
		if !foundMatch {
			fmt.Printf("Found no match for Event Type %v; adding.", eventTypeName)
			eventType := EventType{Name: eventTypeName, Mapping: mapping}
			if result, err = AddEventType(eventType, pzGateway, auth); err == nil {
				foundDeepMatch = true
				break
			} else {
				return result, err
			}
		}
	}

	if foundDeepMatch {
		eventTypeMap[root] = result
	}
	return result, err
}

// GetEventTypeMap returns the EventTypeMap. This should not be modified.
func GetEventTypeMap() EventTypeMap {
	return EventTypeMap{EventTypeMap: eventTypeMap}
}

// AddEventType adds the requested EventType and returns a pointer to what was created
func AddEventType(eventType EventType, pzGateway, auth string) (EventType, error) {
	var (
		err error
		etInputBytes,
		etOutputBytes []byte
		etr    EventTypeResponse
		result EventType
	)
	if etInputBytes, err = json.Marshal(&eventType); err != nil {
		return result, err
	}

	if etOutputBytes, err = RequestKnownJSON("POST", string(etInputBytes), pzGateway+"/eventType", auth, &etr); err != nil {
		err = errors.New(err.Error() + "\n" + string(etOutputBytes))
	}
	result = etr.Data

	return result, err
}

// Events returns the events for the event type ID provided
func Events(eventTypeID string, pzGateway, auth string) ([]Event, error) {

	var (
		err       error
		eventList EventList
	)

	_, err = RequestKnownJSON("GET", "", pzGateway+"/event?eventTypeId="+string(eventTypeID), auth, &eventList)

	return eventList.Data, err
}

// AddEvent adds the requested Event and returns what was created
func AddEvent(event Event, pzGateway, auth string) (EventResponse, error) {
	var (
		err        error
		eventBytes []byte
		result     EventResponse
	)
	if eventBytes, err = json.Marshal(&event); err != nil {
		return result, err
	}

	if _, err = RequestKnownJSON("POST", string(eventBytes), pzGateway+"/event", auth, &result); err != nil {
		log.Printf("Failed to post event %#v\n%v", event, err.Error())
	}

	return result, err
}

// GetAlerts will return the group of alerts associated with the given trigger ID,
// under the given pagination.
func GetAlerts(perPage, pageNo, trigID, pzAddr, pzAuth string) ([]Alert, error) {

	qParams := "triggerId=" + trigID + "&sortBy=createdOn&order=desc"
	if perPage != "" {
		qParams += "&perPage=" + perPage
	}
	if pageNo != "" {
		qParams += "&page=" + pageNo
	}

	var outpObj AlertList

	if _, err := RequestKnownJSON("GET", "", pzAddr+"/alert?"+qParams, pzAuth, &outpObj); err != nil {
		return nil, fmt.Errorf("Error: pzsvc.RequestKnownJSON: fail on alert check: " + err.Error())
	}
	return outpObj.Data, nil
}

// WriteEventTypes returns the event types known to bf-handle
func WriteEventTypes(writer http.ResponseWriter, request *http.Request) {
	etm := GetEventTypeMap()
	if b, err := json.Marshal(etm); err == nil {
		HTTPOut(writer, string(b), http.StatusOK)
	} else {
		http.Error(writer, "Marshalling error: "+err.Error()+".", http.StatusInternalServerError)
	}
}

// AddTrigger adds the requested Trigger and returns what was created
func AddTrigger(trigger Trigger, pzGateway, auth string) (TriggerResponse, error) {
	var (
		err          error
		triggerBytes []byte
		result       TriggerResponse
	)
	if triggerBytes, err = json.Marshal(&trigger); err != nil {
		return result, err
	}

	if _, err = RequestKnownJSON("POST", string(triggerBytes), pzGateway+"/trigger", auth, &result); err != nil {
		log.Printf("Failed to post trigger %#v\n%v", trigger, err.Error())
	}

	return result, err
}
