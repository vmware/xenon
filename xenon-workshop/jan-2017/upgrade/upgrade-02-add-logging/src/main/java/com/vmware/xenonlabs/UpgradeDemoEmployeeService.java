/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenonlabs;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

public class UpgradeDemoEmployeeService extends StatefulService {
    public static final String FACTORY_LINK = "/quickstart/employees";

    public static class Employee extends ServiceDocument {
        // must call Utils.mergeWithState to leverage this
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        // must call Utils.validateState to leverage this
        @UsageOption(option = PropertyUsageOption.REQUIRED)
        public String name;

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        @UsageOption(option = PropertyUsageOption.LINK)
        public String managerLink;

        /**
         * Added for 'add-logging' project.
         */
        @Override
        public String toString() {
            return String.format("Employee [documentSelfLink=%s] [name=%s] [managerLink=%s]",
                    this.documentSelfLink, this.name, this.managerLink);
        }
    }

    public UpgradeDemoEmployeeService() {
        super(Employee.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleCreate(Operation startPost) {
        Employee s = startPost.getBody(Employee.class);
        // Checks for REQUIRED fields
        Utils.validateState(getStateDescription(), s);
        startPost.complete();

        // Added for 'add-logging' project.
        logInfo("Successfully created via POST: %s", s);
    }

    @Override
    public void handlePut(Operation put) {
        Employee newState = getBody(put);
        // Checks for REQUIRED fields
        Utils.validateState(getStateDescription(), newState);
        setState(put, newState);
        put.complete();

        // Added for 'add-logging' project.
        logInfo("Successfully replaced via PUT: %s", newState);
    }

    @Override
    public void handlePatch(Operation patch) {
        Employee state = getState(patch);
        Employee patchBody = getBody(patch);

        Utils.mergeWithState(getStateDescription(), state, patchBody);
        patch.setBody(state);
        patch.complete();

        // Added for 'add-logging' project.
        logInfo("Successfully patched %s. New body is:%s", state.documentSelfLink, state);
    }
}
