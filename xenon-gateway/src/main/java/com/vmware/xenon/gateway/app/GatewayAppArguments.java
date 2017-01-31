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

package com.vmware.xenon.gateway.app;

import com.vmware.xenon.common.ServiceHost;

public class GatewayAppArguments extends ServiceHost.Arguments {

    public static final int DEFAULT_DISPATCH_PORT = 8080;

    /**
     * Port used for the Dispatch Host.
     */
    public int dispatchPort = DEFAULT_DISPATCH_PORT;

    /**
     * Network interface address to bind to for the Dispatch Host.
     */
    public String dispatchBindAddress = ServiceHost.DEFAULT_BIND_ADDRESS;

    /**
     * A stable identity associated with the Dispatch host
     */
    public String dispatchId;

    /**
     * The maintenance interval used for the Dispatch and Configuration hosts.
     * Used to override default maintenance interval in unit-tests.
     */
    public Long maintenanceIntervalMicros;
}
