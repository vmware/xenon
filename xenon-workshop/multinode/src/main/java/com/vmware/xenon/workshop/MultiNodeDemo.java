/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.workshop;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.services.common.RootNamespaceService;

public class MultiNodeDemo {

    public static class MultiNodeDemoEmployee extends ServiceDocument {
        public String name;
        public String managerName;
    }

    public static class MultiNodeDemoEmployeeService extends StatefulService {
        public static final String FACTORY_LINK = "/sample/employees";


        public MultiNodeDemoEmployeeService() {
            super(MultiNodeDemoEmployee.class);

            toggleOption(ServiceOption.PERSISTENCE, true);
            toggleOption(ServiceOption.OWNER_SELECTION, true);
            toggleOption(ServiceOption.REPLICATION, true);
        }

        /**
         * handleCreate() is called when a service is first created. This happens on POST to the factory.
         * When xenon is shut down and restarted, this method will NOT be called. If you want a method that will be called
         * whenever a service is started (both on creation and when the service is restarted), then override
         * handleStart(). Note that we only need to override handleCreate() because we want to do some verification on
         * the proposed document body before we persist it. If you don't want to validate any inputs, you can
         * exclude this method.
         *
         * @param startPost - use this to fetch the state and body as necessary and call complete() when done
         */
        @Override
        public void handleCreate(Operation startPost) {
            MultiNodeDemoEmployee s = getBody(startPost);

            if (s == null) {
                startPost.fail(new IllegalArgumentException("missing body"));
                return;
            }

            if (s.name == null || s.name.isEmpty()) {
                startPost.fail(new IllegalArgumentException("name cannot be null"));
                return;
            }

            // When you call complete(), not only will the object be created, but the REST POST operation will
            // return to the sender.
            startPost.complete();

            // You can still do processing after the complete - very common in an async world.
            // You can also not call complete() this function but rather call complete() at some later time. The REST
            // caller will be blocked until complete() is called.
        }

        /**
         * With PUT, the old version is discarded and replaced with the new complete object.
         * PUT *cannot* be used to create services - it can only be used to update an already running service.
         *
         * @param put - use this to fetch the state and body as necessary and call complete() when done
         */
        @Override
        public void handlePut(Operation put) {
            MultiNodeDemoEmployee newState = getBody(put);
            MultiNodeDemoEmployee currentState = getState(put);

            if (newState == null) {
                put.fail(new IllegalArgumentException("missing body"));
                return;
            }

            if (newState.name == null) {
                put.fail(new IllegalArgumentException("employee name cannot be set to null"));
                return;
            }

            if (!newState.name.equals(currentState.name)) {
                put.fail(new IllegalArgumentException("employee name cannot be changed"));
                return;
            }

            setState(put, newState);
            put.complete();
        }

        /**
         * The HTTP PATCH is used to update a subset of fields on the service/document. The PATCH request specifies
         * just the fields that need to be updated. The rest of the fields are left unchanged.
         *
         * @param patch - use this to fetch the state and body as necessary and call complete() when done
         */
        @Override
        public void handlePatch(Operation patch) {
            MultiNodeDemoEmployee newState = getBody(patch);
            MultiNodeDemoEmployee currentState = getState(patch);

            if (newState == null) {
                patch.fail(new IllegalArgumentException("missing body"));
                return;
            }

            if (newState.name != null) {
                patch.fail(new IllegalArgumentException("employee name cannot be changed in a PATCH"));
                return;
            }

            if (newState.managerName != null) {
                currentState.managerName = newState.managerName;
            }

            patch.setBody(currentState);
            patch.complete();
        }
    }

    public static void main(String[] stringArgs) throws Throwable {
        ServiceHost host = ServiceHost.create("--sandbox=/tmp/multinode/xenondb");
        host.initialize(stringArgs);
        host.start();
        host.startDefaultCoreServicesSynchronously();

        // A stateless service that enumerates all the
        // factory services started on the Service host.
        host.startService(new RootNamespaceService());

        // Starting the employee factory service.
        host.startFactory(new MultiNodeDemoEmployeeService());
    }

    private MultiNodeDemo() {
    }
}
