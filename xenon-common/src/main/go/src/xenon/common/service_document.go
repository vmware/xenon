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
