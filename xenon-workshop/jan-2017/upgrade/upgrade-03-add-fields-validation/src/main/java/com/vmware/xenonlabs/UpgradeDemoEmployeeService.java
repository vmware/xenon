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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
     * @throws IllegalArgumentException if the validation fails for any reason
     */
    static void validateManagerLink(Employee employee, Service service) {
        if (employee.managerLink != null) {
            ServiceHost host = service.getHost();
            host.log(Level.INFO, "Verifying [managerLink=%s] exists...", employee.managerLink);

            // Wait on returning until we get a result back
            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean foundResult = new AtomicBoolean(false);

            Operation getManager = Operation.createGet(service, employee.managerLink)
                    .setReferer(service.getUri())
                    .setCompletion((op, err) -> {

                        if (err != null) {
                            host.log(Level.WARNING, "Couldn't verify [managerLink=%s]: Error: %s",
                                    employee.managerLink, Utils.toString(err));
                        } else {
                            Employee manager = op.getBody(Employee.class);
                            host.log(Level.INFO, "Verified [managerLink=%s] exists: %s",
                                    employee.managerLink, manager);
                            foundResult.getAndSet(true);
                        }
                        latch.countDown();
                    });
            host.sendRequest(getManager);

            /* Seems to have performance problems when done in bulk...
            QueryTaskClientHelper.create(Employee.class)
                    .setDocumentLink(employee.managerLink)
                    .setResultHandler((result, e) -> {
                        if (e != null) {
                            host.log(Level.WARNING, "Couldn't verify [managerLink=%s]: Error: %s",
                                    employee.managerLink, Utils.toString(e));
                        } else if (result.hasResult()) {
                            host.log(Level.INFO, "Verified [managerLink=%s] exists: %s", employee.managerLink,
                                    result.getResult());
                            foundResult.getAndSet(true);
                        } else {
                            host.log(Level.FINE, "No more results");
                        }
                        latch.countDown();
                    })
                    .sendWith(host);
                    */

            // Wait on latch to ensure managerLink exists
            try {
                if (!latch.await(host.getState().operationTimeoutMicros, TimeUnit.MICROSECONDS)) {
                    throw new IllegalArgumentException("Timed out while verifying: " + employee.managerLink);
                }
                if (!foundResult.get()) {
                    throw new IllegalArgumentException(employee.managerLink + ": manager does not exist");
                }
            } catch (InterruptedException e) {
                throw new IllegalArgumentException("Interrupted while verifying whether managerLink exists", e);
            }
        }
    }

    @Override
    public void handleCreate(Operation startPost) {
        Employee s = startPost.getBody(Employee.class);
        // Checks for REQUIRED fields
        Utils.validateState(getStateDescription(), s);
        validateManagerLink(s, this);
        startPost.complete();

        logInfo("Successfully created via POST: %s", s);
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
