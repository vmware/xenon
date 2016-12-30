/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.xenon.gateway;

public enum GatewayStatus {
    /**
     * Gateway is not ready to serve requests
     */
    UNAVAILABLE,

    /**
     * Gateway is in a Paused state. Incoming requests
     * will be queued or discarded depending on the
     * gateway configuration.
     */
    PAUSED,

    /**
     * Gateway is active and serving requests.
     */
    AVAILABLE
}
