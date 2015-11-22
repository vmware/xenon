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

package com.vmware.xenon.common.test.monitoring;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.test.TestResultServiceState;

/**
 * Gather long-running statistics for test cases, initiated from e.g., Jenkins, local, etc.
 */
public class TestResultService extends StatefulService {

    /**
     * Need to persist, and have a single, replicated view of the service
     */
    public TestResultService() {
        super(TestResultServiceState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.REPLICATION, true);
    }

    /**
     * Require a body, i.e., not create anonymous instances (validate simply documentSelfLink though)
     */
    @Override
    public void handleStart(Operation start) {
        try {
            if (!start.hasBody()) {
                throw new IllegalArgumentException("body is required");
            }
            start.complete();
        } catch (Throwable e) {
            start.fail(e);
        }
    }
}
