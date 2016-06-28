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

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.Query;

public class ResourceGroupService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS;

    public static Service createFactory() {
        // workaround for GSON issue https://github.com/google/gson/issues/764
        // We serialize the complex type once, on service creation, to avoid possible GSON race
        ResourceGroupState st = new ResourceGroupState();
        st.query = QueryTask.Query.Builder.create().addFieldClause("one", "one").build();
        Utils.toJson(st);
        return FactoryService.createIdempotent(ResourceGroupService.class);
    }

    /**
     * The {@link ResourceGroupState} holds a query that is used to represent a group of
     * resources (services). {@link ResourceGroupState} and {@link UserGroupService.UserGroupState)
     * are used together in a {@link AuthorizationContextService.Role} to specify what resources
     * a set of users has access to.
     */
    public static class ResourceGroupState extends ServiceDocument {
        /**
         * A standard query to the index service.
         *
         * The result of this query will be the set of resources (services) that a user has
         * access to. Typical queries might be "all services with a documentAuthPrincipalLink
         * field that matches the user's" or "all services with documentKind with a particular
         * kind". These may be typical queries, but you can use any query that matches the set
         * of documents you want a user to have access to.
         */
        public Query query;
    }

    public ResourceGroupService() {
        super(ResourceGroupState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleRequest(Operation request, OperationProcessingStage opProcessingStage) {
        if (request.getAction() == Action.DELETE || request.getAction() == Action.PUT) {
            ResourceGroupState resourceGroupState = null;
            if (request.isFromReplication() && request.hasBody()) {
                resourceGroupState = getBody(request);
            } else {
                resourceGroupState = getState(request);
            }
            if (resourceGroupState != null) {
                AuthorizationCacheUtils.clearAuthzCacheForResourceGroup(this, request, resourceGroupState);
            }
        }
        super.handleRequest(request, opProcessingStage);
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

    @Override
    public void handlePut(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        ResourceGroupState newState = op.getBody(ResourceGroupState.class);
        if (!validate(op, newState)) {
            return;
        }

        ResourceGroupState currentState = getState(op);
        ServiceDocumentDescription documentDescription = getStateDescription();
        if (ServiceDocument.equals(documentDescription, currentState, newState)) {
            op.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED);
        } else {
            setState(op, newState);
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
