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

import java.util.Set;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

public class RoleService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.CORE_AUTHZ_ROLES;

    public static Service createFactory() {
        FactoryService fs = new FactoryService(RoleState.class) {

            @Override
            public Service createServiceInstance() throws Throwable {
                return new RoleService();
            }

            @Override
            public void handlePost(Operation request) {
                RoleState roleState = AuthorizationCacheUtils.extractBody(request, this, RoleState.class);
                if (roleState != null) {
                    AuthorizationCacheUtils.clearAuthzCacheForRole(this, request, roleState);
                }
                super.handlePost(request);
            }
        };
        fs.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        return fs;
    }

    public enum Policy {
        ALLOW,
        DENY,
    }

    /**
     * The {@link RoleState} represents a role. A role applies to users contained in its user group,
     * to HTTP verbs in the set of applicable verbs, and to resources in its resource group.
     */
    public static class RoleState extends ServiceDocument {
        public static final String KIND = Utils.buildKind(RoleState.class);
        public static final String FIELD_NAME_USER_GROUP_LINK = "userGroupLink";
        public static final String FIELD_NAME_RESOURCE_GROUP_LINK = "resourceGroupLink";

        public String userGroupLink;
        public String resourceGroupLink;
        public Set<Action> verbs;
        public Policy policy;
        public int priority;
    }

    public RoleService() {
        super(RoleState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleRequest(Operation request, OperationProcessingStage opProcessingStage) {
        RoleState roleState = AuthorizationCacheUtils.extractBody(request, this, RoleState.class);
        if (roleState != null) {
            AuthorizationCacheUtils.clearAuthzCacheForRole(this, request, roleState);
        }
        super.handleRequest(request, opProcessingStage);
    }

    @Override
    public void handleStart(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        RoleState state = op.getBody(RoleState.class);
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

        RoleState newState = op.getBody(RoleState.class);
        if (!validate(op, newState)) {
            return;
        }

        RoleState currentState = getState(op);
        ServiceDocumentDescription documentDescription = getStateDescription();
        if (ServiceDocument.equals(documentDescription, currentState, newState)) {
            op.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED);
        } else {
            setState(op, newState);
        }
        op.complete();
    }

    private boolean validate(Operation op, RoleState state) {
        if (state.userGroupLink == null) {
            op.fail(new IllegalArgumentException("userGroupLink is required"));
            return false;
        }

        if (state.resourceGroupLink == null) {
            op.fail(new IllegalArgumentException("resourceGroupLink is required"));
            return false;
        }

        if (state.verbs == null) {
            op.fail(new IllegalArgumentException("verbs is required"));
            return false;
        }

        if (state.policy == null) {
            op.fail(new IllegalArgumentException("policy is required"));
            return false;
        }

        if (state.policy == Policy.DENY) {
            op.fail(new IllegalArgumentException("DENY policy is not supported"));
            return false;
        }

        return true;
    }
}
