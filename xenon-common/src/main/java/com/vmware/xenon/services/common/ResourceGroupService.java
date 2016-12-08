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

import java.util.Objects;
import java.util.concurrent.CancellationException;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceRuntimeContext;
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

        public static class Builder {
            private ResourceGroupService.ResourceGroupState resourceGroupState;

            private Builder() {
                this.resourceGroupState = new ResourceGroupState();
            }

            public static Builder create() {
                return  new Builder();
            }

            public Builder withQuery(QueryTask.Query query) {
                this.resourceGroupState.query = query;
                return this;
            }

            public Builder withSelfLink(String userGroupSelfLink) {
                this.resourceGroupState.documentSelfLink = userGroupSelfLink;
                return this;
            }

            public ResourceGroupService.ResourceGroupState build() {
                return this.resourceGroupState;
            }
        }
    }

    public ResourceGroupService() {
        super(ResourceGroupState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    public static class PatchQueryRequest {
        public static final String KIND = Utils.buildKind(PatchQueryRequest.class);
        public Query clause;
        public boolean removeClause = false;
        public String kind;

        private PatchQueryRequest(Query clause, boolean removeClause) {
            this.clause = clause;
            this.removeClause = removeClause;
            this.kind = KIND;
        }

        public static PatchQueryRequest create(Query clause, boolean removeClause) {
            return new PatchQueryRequest(clause, removeClause);
        }
    }

    @Override
    public void processCompletionStageUpdateAuthzArtifacts(Operation op) {
        if (AuthorizationCacheUtils.isAuthzCacheClearApplicableOperation(op)) {
            AuthorizationCacheUtils.clearAuthzCacheForResourceGroup(this, op);
        }
        op.complete();
    }

    @Override
    public ServiceRuntimeContext setProcessingStage(Service.ProcessingStage stage) {
        if (stage == Service.ProcessingStage.PAUSED) {
            throw new CancellationException("Cannot pause service.");
        }
        return super.setProcessingStage(stage);
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

    @Override
    public void handlePatch(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }
        ResourceGroupState currentState = getState(op);
        PatchQueryRequest patchQuery = op.getBody(PatchQueryRequest.class);
        if (!Objects.equals(patchQuery.kind, PatchQueryRequest.KIND)) {
            op.fail(new IllegalArgumentException("kind should be : " + PatchQueryRequest.KIND));
            return;
        }
        if (patchQuery.clause == null) {
            op.fail(new IllegalArgumentException("clause is required"));
            return;
        }
        if (patchQuery.removeClause) {
            AuthorizationCacheUtils.removeBooleanClause(currentState.query, patchQuery.clause);
        } else {
            currentState.query.addBooleanClause(patchQuery.clause);
        }
        op.setBody(currentState);
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
