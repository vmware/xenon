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

package com.vmware.xenon.gateway.hosts;

import java.net.URI;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.gateway.GatewayService;

/**
 * A Stateless Service Host dedicated to the GatewayService, to receive
 * incoming traffic and dispatch it to a backend node-group.
 *
 * The dispatch host has no other services running on it except
 * for the GatewayService. It does not even need the core services
 * such as node-groups, document-index. This is because the dispatch
 * host acts as a stateless non-clustered endpoint that only serves
 * as a proxy/ router of incoming requests.
 */
public class GatewayDispatchHost extends ServiceHost {

    protected URI configHostUri;

    protected  GatewayDispatchHost() {
    }

    /**
     * Because the Gateway Service uses configuration metadata that is
     * stored by the Configuration host, we pass it the Config Host URI.
     */
    public static GatewayDispatchHost create(URI configHostUri) {
        if (configHostUri == null) {
            throw new IllegalArgumentException("configHostUri cannot be null");
        }
        GatewayDispatchHost host = new GatewayDispatchHost();
        host.configHostUri = configHostUri;
        return host;
    }

    @Override
    public ServiceHost initialize(Arguments arguments) throws Throwable {
        ServiceHost serviceHost = super.initialize(arguments);
        serviceHost.setDocumentIndexingService(null);
        serviceHost.setManagementService(null);
        serviceHost.setAuthenticationService(null);
        return serviceHost;
    }

    /**
     * The dispatch host ONLY starts the GatewayService.
     */
    @Override
    public ServiceHost start() throws Throwable {
        super.start();
        super.startService(new GatewayService(this.configHostUri));
        return this;
    }
}
