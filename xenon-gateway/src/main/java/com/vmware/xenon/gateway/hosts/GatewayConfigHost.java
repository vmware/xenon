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

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.gateway.GatewayConfigService;
import com.vmware.xenon.gateway.GatewayPathFactoryService;
import com.vmware.xenon.services.common.RootNamespaceService;

/**
 * A Service Host used to store Configuration state of the Gateway.
 */
public class GatewayConfigHost extends ServiceHost {

    /**
     * Starts Core xenon services and factories of
     * stateful services used to store configuration state.
     */
    @Override
    public ServiceHost start() throws Throwable {
        super.start();
        startDefaultCoreServicesSynchronously();

        // Starting root namespace service to allow enumeration of
        // all factories on this host.
        super.startService(new RootNamespaceService());

        // Starting factory services for Gateway configuration
        super.startFactory(new GatewayConfigService());

        GatewayPathFactoryService factoryService = new GatewayPathFactoryService();
        factoryService.setUseBodyForSelfLink(true);
        super.startService(factoryService);

        return this;
    }
}
