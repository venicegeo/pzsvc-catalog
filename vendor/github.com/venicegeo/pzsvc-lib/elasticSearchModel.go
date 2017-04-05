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

/*
This is merely the beginning of an attempt to lay out the elasticsearch grammar
in go structs.  The overall objective is to enable better JSON marshaling and
demarshaling while maintaining typing, and without requiring a bunch of casting
from interface{} every time you want to do anything.  The antipathy that Go has
for polymorphism hurts here, given how enthusiastically polymorphic both the
elasticsearch grammar and the piazza backend are in some places
*/

// JobData ...
type JobData struct {
	ServiceID  string              `json:"serviceId,omitempty"`
	DataInputs map[string]DataType `json:"dataInputs"`
	DataOutput []DataType          `json:"dataOutput,omitempty"`
}

// TrigJob ...
type TrigJob struct {
	JobType struct {
		Type string  `json:"type,omitempty"`
		Data JobData `json:"data,omitempty"`
	} `json:"jobType,omitempty"`
}

// CompClause ...
type CompClause struct {
	LTE    interface{} `json:"lte,omitempty"`
	GTE    interface{} `json:"gte,omitempty"`
	Format string      `json:"format,omitempty"`
}

// QueryClause ...
type QueryClause struct {
	Match map[string]string     `json:"match,omitempty"`
	Range map[string]CompClause `json:"range,omitempty"`
}

// TrigCondition ...
type TrigCondition struct {
	Query struct {
		Bool struct {
			Filter []QueryClause `json:"filter"`
		} `json:"bool"`
	} `json:"query"`
}
