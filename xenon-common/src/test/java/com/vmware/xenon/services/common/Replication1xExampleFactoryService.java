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

package com.vmware.xenon.services.common;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

/**
 * Test factory service that uses the limited replication selector to construct example service instances
 */
public class Replication1xExampleFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.CORE
            + "/test/1x-replication-examples";

    public Replication1xExampleFactoryService() {
        super(ExampleServiceState.class);
        super.setPeerNodeSelectorPath(ServiceUriPaths.DEFAULT_1X_NODE_SELECTOR);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new ExampleService();
    }
}