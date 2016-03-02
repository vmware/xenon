package common

type ServiceHostState struct {
	ServiceDocument
	BindAddress                  string                 `json:"bindAddress"`
	HTTPPort                     int                    `json:"httpPort"`
	MaintenanceInterval          uint64                 `json:"maintenanceIntervalMicros"`
	OperationTimeout             uint64                 `json:"operationTimeoutMicros"`
	StorageSandboxFile           string                 `json:"storageSandboxFileReference"`
	AuthorizationService         string                 `json:"authorizationServiceReference"`
	ID                           string                 `json:"id"`
	Started                      bool                   `json:"isStarted"`
	SystemHostInfo               map[string]interface{} `json:"systemInfo"`
	LastMaintenanceTime          uint64                 `json:"lastMaintenanceTimeUtcMicros"`
	PendingMaintenanceOperations int                    `json:"pendingMaintenanceOperations"`
	ProcessOwner                 bool                   `json:"isProcessOwner"`
}
