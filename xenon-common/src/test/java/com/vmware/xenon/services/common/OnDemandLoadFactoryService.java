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

import java.util.EnumSet;
import java.util.logging.Level;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;

public class OnDemandLoadFactoryService extends FactoryService {
    public static final String SELF_LINK = "test/on-demand-load-services";
    public static final String FACTORY_LINK = "test/on-demand-load-services";

    public static String create(ServiceHost h, ServiceOption... extraOptions)
            throws Throwable {
        // create an on demand load factory and services
        TestContext ctx = TestContext.create(1, h.getOperationTimeoutMicros());
        OnDemandLoadFactoryService s = new OnDemandLoadFactoryService();
        EnumSet<ServiceOption> childOptions = EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.REPLICATION, ServiceOption.OWNER_SELECTION);

        if (extraOptions != null) {
            for (ServiceOption eo : extraOptions) {
                childOptions.add(eo);
            }
        }
        s.setChildServiceCaps(childOptions);
        Operation factoryPost = Operation.createPost(
                UriUtils.buildUri(h, s.getClass()))
                .setCompletion(ctx.getCompletion());
        h.startService(factoryPost, s);
        ctx.await();
        String factoryLink = s.getSelfLink();
        h.log(Level.INFO, "Started on demand load factory at %s", factoryLink);
        return factoryLink;
    }

    public OnDemandLoadFactoryService() {
        super(ExampleService.ExampleServiceState.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
    }

    private EnumSet<ServiceOption> childServiceCaps;

    /**
     * Test use only.
     */
    public void setChildServiceCaps(EnumSet<ServiceOption> caps) {
        this.childServiceCaps = caps;
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        Service s = new OnDemandLoadService();
        if (this.childServiceCaps != null) {
            for (ServiceOption c : this.childServiceCaps) {
                s.toggleOption(c, true);
            }
        }

        return s;
    }
}
