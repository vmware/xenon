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

import java.util.EnumSet;
import java.util.Set;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.StatefulService;

public class UserService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.CORE_AUTHZ_USERS;

    public static Service createFactory() {
        return FactoryService.createIdempotent(UserService.class);
    }

    /**
     * The {@link UserState} represents a single user's identity.
     */
    public static class UserState extends ServiceDocument {
        public static final String FIELD_NAME_EMAIL = "email";
        public static final String FIELD_NAME_USER_GROUP_LINKS = "userGroupLinks";
        public String email;
        public Set<String> userGroupLinks;
    }

    public UserService() {
        super(UserState.class);
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

        UserState state = op.getBody(UserState.class);
        if (!validate(op, state)) {
            return;
        }
        AuthorizationCacheUtils.clearAuthzCacheForUser(this, op, state.documentSelfLink);
    }

    @Override
    public void handlePut(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        UserState newState = op.getBody(UserState.class);
        if (!validate(op, newState)) {
            return;
        }

        UserState currentState = getState(op);
        // if the email field has not changed and the userGroupsLinks field is either null
        // or the same in both the current state and the state passed in return a 304
        // response
        if (currentState.email.equals(newState.email)
                && ((currentState.userGroupLinks == null && newState.userGroupLinks == null)
                || (currentState.userGroupLinks != null && newState.userGroupLinks != null
                    && currentState.userGroupLinks.equals(newState.userGroupLinks)))) {
            op.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED);
        } else {
            setState(op, newState);
        }
        AuthorizationCacheUtils.clearAuthzCacheForUser(this, op, newState.documentSelfLink);
    }

    @Override
    public void handlePatch(Operation op) {
        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }
        UserState currentState = getState(op);
        UserState newState = op.getBody(UserState.class);
        if (newState.email != null) {
            currentState.email = newState.email;
        }
        if (newState.userGroupLinks != null) {
            if (currentState.userGroupLinks == null) {
                currentState.userGroupLinks = newState.userGroupLinks;
            } else {
                currentState.userGroupLinks.addAll(newState.userGroupLinks);
            }
        }
        op.setBody(currentState);
        AuthorizationCacheUtils.clearAuthzCacheForUser(this, op, currentState.documentSelfLink);
    }

    @Override
    public void handleDelete(Operation op) {
        UserState currentState = getState(op);
        AuthorizationCacheUtils.clearAuthzCacheForUser(this, op, currentState.documentSelfLink);
    }

    private boolean validate(Operation op, UserState state) {
        if (state.email == null) {
            op.fail(new IllegalArgumentException("email is required"));
            return false;
        }

        // This type of email checking is EXTREMELY primitive.
        // Since this is expected to be populated by another service that connects
        // to an external identity provider, this can be kept simple.
        int firstAtIndex = state.email.indexOf('@');
        int lastAtIndex = state.email.lastIndexOf('@');
        if (firstAtIndex == -1 || (firstAtIndex != lastAtIndex)) {
            op.fail(new IllegalArgumentException("email is invalid"));
            return false;
        }

        return true;
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument td = super.getDocumentTemplate();
        ServiceDocumentDescription.PropertyDescription pdGroupLinks = td.documentDescription.propertyDescriptions
                .get(UserState.FIELD_NAME_USER_GROUP_LINKS);
        if (pdGroupLinks == null) {
            throw new IllegalStateException(UserState.FIELD_NAME_USER_GROUP_LINKS
                    + " property is missing in the service document");
        }
        pdGroupLinks.indexingOptions = EnumSet
                .of(PropertyIndexingOption.EXPAND);
        return td;
    }
}
