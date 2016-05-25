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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.test.MinimalTestServiceState;

public class MinimalFactoryTestService extends FactoryService {

    public MinimalFactoryTestService() {
        super(MinimalTestServiceState.class);
    }

    private EnumSet<ServiceOption> childServiceCaps;

    /**
     * Test use only. We use a single factory service to create instances of a child service with
     * capabilities determined at runtime. THis is not typical of a production factory service that
     * is tuned to making one type of child service instance
     *
     * @param caps
     */
    public void setChildServiceCaps(EnumSet<ServiceOption> caps) {
        this.childServiceCaps = caps;
    }

    public boolean gotStopped;

    public boolean gotDeleted;

    @Override
    public void handleStop(Operation op) {
        this.gotStopped = true;
        op.complete();
    }

    @Override
    public void handleDelete(Operation op) {
        if (op.hasBody()) {
            // fail delete if it has body, to try out delete abort behavior
            op.fail(new IllegalStateException("failing intentionally"));
            return;
        }
        this.gotDeleted = true;
        op.complete();
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        Service s = new MinimalTestService();
        if (this.childServiceCaps != null) {
            for (ServiceOption c : this.childServiceCaps) {
                s.toggleOption(c, true);
            }
        }
        return s;
    }

    @Override
    public void handlePost(Operation op) {
        setOperationHandlerInvokeTimeStat(op);
        super.handlePost(op);
        setOperationDurationStat(op);
    }
}
