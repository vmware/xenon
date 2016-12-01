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

package example.group;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

/**
 * Provides an implementation of highly scalable, highly available employee record service. Since most of the mechanisms
 * are provided natively by Xenon, this is a very simple class.
 *
 */

public class EmployeeService extends StatefulService {
    // FACTORY_LINK is a special variable that Xenon looks for to know where to host your REST API
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
    }

    public EmployeeService() {
        super(Employee.class);

        // Employee records will indexed, searchable and versioned. Also service can be paused under
        // memory pressure, and resumed on demand. If you disable, you can now longer discover via query task.
        toggleOption(ServiceOption.PERSISTENCE, true);

        // In multi-node, each employee record will be replicated to all nodes (see docs for limited 3x replication)
        toggleOption(ServiceOption.REPLICATION, true);

        // statistics will be collected on API calls to the service
        toggleOption(ServiceOption.INSTRUMENTATION, true);

        // in multi-node, for each employee, one node will be elected leader and all GET/PUT/POST/PATCH/DELETE
        // calls will be routed to that node. That leader node will first commit writes and then replicate to other
        // nodes in the cluster. Thus an individual thread will see "read your writes" consistency,
        // and cluster-wide, there will be a consistent view of a service across nodes.
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    /**
     * handleCreate() is called when a service is first created. This happens on POST to the factory.
     * When xenon is shut down and restarted, this method will NOT be called. If you want a method that will be called
     * whenever a service is started (both on creation and when the service is restarted), then override
     * handleStart(). Note that we only need to overide handleCreate() because we want to do some verification on
     * the proposed document body before we persist it. If you don't want to validate any inputs, you can
     * exclude this method.
     * @param startPost - use this to fetch the state and body as necessary and call complete() when done
     */
    @Override
    public void handleCreate(Operation startPost) {
        Employee s = startPost.getBody(Employee.class);
        Utils.validateState(getStateDescription(), s);    // Checks for REQUIRED fields, can throw exception.

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
     * @see <a href="https://github.com/vmware/xenon/wiki/Programming-Model#verb-semantics-and-associated-actions"/>
     * programming model</a>. Note that we only override the default here so that we can call validateState(),
     * otherwise, no state will be validated.
     * @param put - use this to fetch the state and body as necessary and call complete() when done
     */
    @Override
    public void handlePut(Operation put) {
        Employee newState = getBody(put);
        Utils.validateState(getStateDescription(), newState);    // Checks for REQUIRED fields, can throw exception.
        setState(put, newState);
        put.complete();
    }

    /**
     * The HTTP PATCH is used to update a subset of fields on the service/document. The PATCH request specifies
     * just the fields that need to be updated. The rest of the fields are left unchanged. PATCH is commonly
     * used with Xenon services talking to each other - in particular, it is used to update the state of a task
     * (most tasks in Xenon are state-machineb based). However it is efficient and convenient for all updates.
     * @param patch - use this to fetch the state and body as necessary and call complete() when done
     */
    @Override
    public void handlePatch(Operation patch) {
        Employee state = getState(patch);
        Employee patchBody = getBody(patch);

        /*
          Looks for differences between the 'body' object (the new version) and the 'state' object (old version).
          the object already stored in Xenon (the state). If a field differs and is non-null in the body,
          and if the AUTO_MERGE_IF_NOT_NULL property is set on that field, then the field is updated with the new
          value found in the body. The merged version is returned.
         */
        Utils.mergeWithState(getStateDescription(), state, patchBody);

        /*
         * For a PATCH, the default body returned to caller will just have the properties that actually changed plus all the
         * system properties. This may not be intuitive. By calling setBody with the new state, we'll return all
         * the properties that have been set (in this patch or previously).
         * setBody(null) may be the most optimized solution which will return an empty document to the user,
         * although it can be confusing when first exploring Xenon.
         */
        patch.setBody(state);
        patch.complete();
    }
}
