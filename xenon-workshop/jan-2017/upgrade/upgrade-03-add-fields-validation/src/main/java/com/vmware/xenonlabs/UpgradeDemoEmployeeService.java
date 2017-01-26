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

import java.util.function.Consumer;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceHost;
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

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        @UsageOption(option = PropertyUsageOption.REQUIRED)
        public String location;

        @Override
        public String toString() {
            return String.format("Employee [documentSelfLink=%s] [name=%s] [managerLink=%s] [location=%s]",
                    this.documentSelfLink, this.name, this.managerLink, this.location);
        }
    }

    public UpgradeDemoEmployeeService() {
        super(Employee.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    /**
     * Helper method to ensure that {@code employee}'s {@code managerLink} currently exists.
     *
     * @param employee - the employee to validate
     * @param service  - the service performing the validation
     * @param consumer - validation callback. if validation fails, it receives Throwable, otherwise null.
     */
    static void validateManagerLink(Employee employee, Service service, Consumer<Throwable> consumer) {

        if (employee.managerLink == null) {
            consumer.accept(null);
            return;
        }

        ServiceHost host = service.getHost();
        host.log(Level.INFO, "Verifying [managerLink=%s] exists...", employee.managerLink);

        Operation.createGet(service, employee.managerLink)
                .setReferer(service.getUri())
                .setCompletion((o,e) -> consumer.accept(e))
                .sendWith(host);
    }

    @Override
    public void handleCreate(Operation startPost) {
        Employee s = startPost.getBody(Employee.class);
        // Checks for REQUIRED fields
        Utils.validateState(getStateDescription(), s);
        validateManagerLink(s, this, (e) -> {
            if (e != null) {
                startPost.fail(e);
                return;
            }
            startPost.complete();
            logInfo("Successfully created via POST: %s", s);
        });
    }

    @Override
    public void handlePut(Operation put) {
        Employee newState = getBody(put);
        // Checks for REQUIRED fields
        Utils.validateState(getStateDescription(), newState);
        setState(put, newState);
        put.complete();

        logInfo("Successfully replaced via PUT: %s", newState);
    }

    @Override
    public void handlePatch(Operation patch) {
        Employee state = getState(patch);
        Employee patchBody = getBody(patch);

        Utils.mergeWithState(getStateDescription(), state, patchBody);
        patch.setBody(state);
        patch.complete();

        logInfo("Successfully patched %s. New body is:%s", state.documentSelfLink, state);
    }
}
