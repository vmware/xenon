package common

type ServiceErrorResponse struct {
	Message    string   `json:"message,omitempty"`
	StackTrace []string `json:"stackTrace,omitempty"`
	StatusCode int      `json:"statusCode,omitempty"`
}
