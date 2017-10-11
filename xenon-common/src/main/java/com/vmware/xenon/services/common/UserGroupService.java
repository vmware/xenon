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
import com.vmware.xenon.services.common.QueryTask.Query;

public class UserGroupService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE_AUTHZ_USER_GROUPS;

    public static Service createFactory() {
        FactoryService fs = new FactoryService(UserGroupState.class) {

            @Override
            public Service createServiceInstance() throws Throwable {
                return new UserGroupService();
            }

            @Override
            public void handlePost(Operation request) {
                checkAndNestCompletionForAuthzCacheClear(this, request);
                super.handlePost(request);
            }
        };
        fs.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        return fs;
    }

    private static void checkAndNestCompletionForAuthzCacheClear(Service s, Operation op) {
        if (AuthorizationCacheUtils.isAuthzCacheClearApplicableOperation(op)) {
            UserGroupState state = AuthorizationCacheUtils.extractBody(op, s, UserGroupState.class);
            if (state != null) {
                AuthorizationCacheUtils.clearAuthzCacheForUserGroup(s, op, state);
            }
        }
    }

    /**
     * The {@link UserGroupState} holds the query that is used to represent a group of users.
     */
    public static class UserGroupState extends ServiceDocument {
        public Query query;

        public static class Builder {
            private UserGroupService.UserGroupState userGroupState;

            private Builder() {
                this.userGroupState = new UserGroupState();
            }

            public static Builder create() {
                return  new Builder();
            }

            public Builder withQuery(QueryTask.Query query) {
                this.userGroupState.query = query;
                return this;
            }

            public Builder withSelfLink(String userGroupSelfLink) {
                this.userGroupState.documentSelfLink = userGroupSelfLink;
                return this;
            }

            public UserGroupService.UserGroupState build() {
                return this.userGroupState;
            }
        }
    }

    public UserGroupService() {
        super(UserGroupState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void processCompletionStageUpdateAuthzArtifacts(Operation op) {
        checkAndNestCompletionForAuthzCacheClear(this, op);
        op.complete();
    }

    @Override
    public void setProcessingStage(Service.ProcessingStage stage) {
        super.setProcessingStage(stage);
    }

    @Override
    public void handleStart(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        UserGroupState newState = op.getBody(UserGroupState.class);
        if (!validate(op, newState)) {
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

        UserGroupState newState = op.getBody(UserGroupState.class);
        if (!validate(op, newState)) {
            return;
        }

        UserGroupState currentState = getState(op);
        ServiceDocumentDescription documentDescription = getStateDescription();
        if (ServiceDocument.equals(documentDescription, currentState, newState)) {
            // HTTP-304 spec doesn't define behavior for PUT
            // In current implementation, setting 304 will skip creating new version document in index.
            // This is not appropriate behavior from http standpoint, and it may change in future.
            // (Probably use other means such as pragma to skip creating new version)
            // The behavior for response operation is also relies on current xenon implementation and may change in future.
            op.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED);
        } else {
            setState(op, newState);
        }
        op.complete();
    }

    private boolean validate(Operation op, UserGroupState state) {
        if (state.query == null) {
            op.fail(new IllegalArgumentException("query is required"));
            return false;
        }

        return true;
    }
}
