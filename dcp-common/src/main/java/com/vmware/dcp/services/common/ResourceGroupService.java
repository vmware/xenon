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

package com.vmware.dcp.services.common;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.services.common.QueryTask.Query;

public class ResourceGroupService extends StatefulService {
    /**
     * The {@link ResourceGroupState} holds the query that is used to represent a group of users.
     */
    public static class ResourceGroupState extends ServiceDocument {
        public Query query;
    }

    public ResourceGroupService() {
        super(ResourceGroupState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        ResourceGroupState state = op.getBody(ResourceGroupState.class);
        if (!validate(op, state)) {
            return;
        }

        op.complete();
    }

    private boolean validate(Operation op, ResourceGroupState state) {
        if (state.query == null) {
            op.fail(new IllegalArgumentException("query is required"));
            return false;
        }

        return true;
    }
}
