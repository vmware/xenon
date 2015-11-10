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

public class UserService extends StatefulService {
    /**
     * The {@link UserState} represents a single user's identity.
     */
    public static class UserState extends ServiceDocument {
        public static final String FIELD_NAME_EMAIL = "email";
        public String email;
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

        op.complete();
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
}
