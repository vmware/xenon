package common

type SystemHostInfo struct {
	Properties              map[string]string `json:"properties"`
	Environment             map[string]string `json:"environmentVariables"`
	AvailableProcessorCount int64             `json:"availableProcessorCount"`
	FreeMemoryByteCount     int64             `json:"freeMemoryByteCount"`
	TotalMemoryByteCount    int64             `json:"totalMemoryByteCount"`
	MaxMemoryByteCount      int64             `json:"maxMemoryByteCount"`
	IPAddresses             []string          `json:"ipAddresses"`
}
