// Copyright (c) 2015-2016 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, without warranties or
// conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
// specific language governing permissions and limitations under the License.

package common

const (
	FieldNameDocumentKind = "documentKind"
)

type ServiceDocument struct {
	Description          string `json:"documentDescription,omitempty"`
	Version              int64  `json:"documentVersion,omitempty"`
	Kind                 string `json:"documentKind,omitempty"`
	SelfLink             string `json:"documentSelfLink,omitempty"`
	Signature            string `json:"documentSignature,omitempty"`
	UpdateTimeMicros     int64  `json:"documentUpdateTimeMicros,omitempty"`
	ExpirationTimeMicros int64  `json:"documentExpirationTimeMicros,omitempty"`
	Owner                string `json:"documentOwner,omitempty"`
}

type ServiceDocumentQueryResult struct {
	ServiceDocument

	// Collection of self links associated with each document found. The self
	// link acts as the primary key for a document.
	DocumentLinks []string `json:"documentLinks,omitempty"`

	// If the query included an expand directory, this map contains service state
	// document associated with each link.
	Documents map[string]interface{} `json:"documents,omitempty"`
}

// Need to add more fields to support query task,
// For now, just used to retrieve result from odata query
type QueryTask struct {
	Results ServiceDocumentQueryResult `json:"results,omitempty"`
}
