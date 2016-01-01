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

import java.net.URI;
import java.util.List;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;

/**
 * Describes the authentication credentials to authenticate with internal/external APIs.
 */
public class AuthCredentialsService extends StatefulService {
    public static class AuthCredentialsServiceState extends ServiceDocument {

        public static final String FIELD_NAME_EMAIL = "userEmail";
        public static final String FIELD_NAME_PRIVATE_KEY = "privateKey";

        /** Client ID. */
        public String userLink;

        /** Client email. */
        public String userEmail;

        /**
         * Service Account private key.
         *
         * When using the BasicAuthenticationService, this is the user password. Other
         * authentication services may use it differently.
         */
        public String privateKey;

        /**
         * Service Account private key id
         *
         * When using the BasicAuthenticationService, this is not used. Other authentication services
         * may use it.
         */
        public String privateKeyId;

        /**
         * Service Account public key
         *
         * When using the BasicAuthenticationService, this is
         * not used. Other authentication services may use it.
         */
        public String publicKey;

        /** Token server URI. */
        public URI tokenReference;

        /** Type of credentials */
        public String type;

        /**
         * A list of tenant links which can access this service.
         */
        public List<String> tenantLinks;

    }

    public AuthCredentialsService() {
        super(AuthCredentialsServiceState.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handlePatch(Operation patch) {
        AuthCredentialsServiceState currentState = getState(patch);
        AuthCredentialsServiceState patchBody = patch.getBody(AuthCredentialsServiceState.class);

        updateState(patchBody, currentState);
        patch.setBody(currentState).complete();
    }

    private void updateState(AuthCredentialsServiceState newState,
            AuthCredentialsServiceState currentState) {
        if (newState.userLink != null) {
            currentState.userLink = newState.userLink;
        }

        if (newState.userEmail != null) {
            currentState.userEmail = newState.userEmail;
        }

        if (newState.privateKey != null) {
            currentState.privateKey = newState.privateKey;
        }

        if (newState.publicKey != null) {
            currentState.publicKey = newState.publicKey;
        }

        if (newState.privateKeyId != null) {
            currentState.privateKeyId = newState.privateKeyId;
        }

        if (newState.tokenReference != null) {
            currentState.tokenReference = newState.tokenReference;
        }

        if (newState.type != null) {
            currentState.type = newState.type;
        }
    }
}
