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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;

/**
 * Represents a tenant. Service documents can reference tenant(s) using a {@code tenantLinks} field which can
 * be used to enforce tenant isolation in a multi-tenant application. If tenant ID is not specified,
 * a unique identifier is generated during creation.
 */
public class TenantService extends StatefulService {
    public static class TenantState extends ServiceDocument {
        /**
         * Unique identifier for the tenant. If not specified during creation, a random one is automatically set.
         * This value cannot be changed once set.
         */
        @UsageOption(option = PropertyUsageOption.ID)
        public String id;

        /**
         * Name of the tenant.
         */
        @Documentation(exampleString = "VMware Inc.")
        public String name;

        /**
         * A reference to the parent tenant.
         */
        @UsageOption(option = PropertyUsageOption.OPTIONAL)
        @Documentation(description = "The parent (if any) of this tenant")
        public String parentLink;
    }

    public TenantService() {
        super(TenantState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handlePatch(Operation patch) {
        TenantState currentState = getState(patch);
        TenantState newState = patch.getBody(TenantState.class);
        mergeState(currentState, newState);
        patch.complete();
    }

    private void mergeState(TenantState currentState, TenantState newState) {
        if (newState.name != null) {
            currentState.name = newState.name;
        }
        if (newState.parentLink != null) {
            currentState.parentLink = newState.parentLink;
        }
    }
}
