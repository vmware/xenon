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
