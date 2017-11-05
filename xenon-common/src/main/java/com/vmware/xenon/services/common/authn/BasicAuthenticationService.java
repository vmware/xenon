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

package com.vmware.xenon.services.common.authn;

import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.config.XenonConfiguration;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserService.UserState;
import com.vmware.xenon.services.common.authn.AuthenticationRequest.AuthenticationRequestType;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils.BasicAuthenticationContext;

public class BasicAuthenticationService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CORE_AUTHN_BASIC;

    static final long AUTH_TOKEN_EXPIRATION_SECONDS = XenonConfiguration.number(
            BasicAuthenticationService.class,
            "AUTH_TOKEN_EXPIRATION_SECONDS",
            TimeUnit.HOURS.toSeconds(1)
    );

    private final long UPPER_SESSION_LIMIT_SECONDS = XenonConfiguration.number(
            BasicAuthenticationService.class,
            "UPPER_SESSION_LIMIT_SECONDS",
            0
    );

    public BasicAuthenticationService() {
        toggleOption(ServiceOption.CORE, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public boolean queueRequest(Operation op) {
        // if the incoming request is for self proceed with handling the request, else mark
        // the operation complete, the caller can then proceed with a null auth context
        // that will default to the guest context
        if (getSelfLink().equals(op.getUri().getPath())) {
            return false;
        }
        op.complete();
        return true;
    }

    @Override
    public void handlePost(Operation op) {
        // not changing the other requestTypes to pragma header  for backward compatibility
        if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN)) {
            BasicAuthenticationUtils.handleTokenVerify(this, op);
            op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);
            return;
        }
        AuthenticationRequest authRequest = op.getBody(AuthenticationRequest.class);
        // default to login for backward compatibility
        if (authRequest.requestType == null) {
            authRequest.requestType = AuthenticationRequestType.LOGIN;
        }
        switch (authRequest.requestType) {
        case LOGIN:
            String[] userNameAndPassword = BasicAuthenticationUtils.parseRequest(this, op);
            if (userNameAndPassword == null) {
                // the operation has the right failure code set; just return
                return;
            }
            BasicAuthenticationContext authContext = new BasicAuthenticationContext();
            authContext.userQuery = Query.Builder.create().addKindFieldClause(UserState.class)
            .addFieldClause(UserState.FIELD_NAME_EMAIL, userNameAndPassword[0])
            .build();

            authContext.authQuery = Query.Builder.create().addKindFieldClause(AuthCredentialsServiceState.class)
            .addFieldClause(AuthCredentialsServiceState.FIELD_NAME_EMAIL, userNameAndPassword[0])
            .addFieldClause(AuthCredentialsServiceState.FIELD_NAME_PRIVATE_KEY, userNameAndPassword[1])
            .build();
            BasicAuthenticationUtils.handleLogin(this, op, authContext,
                    getExpirationTime(authRequest));
            break;
        case LOGOUT:
            BasicAuthenticationUtils.handleLogout(this, op);
            break;
        default:
            break;
        }
    }

    /**
     * Get the expected expiration time from the current time based on system settings and passed-in
     * session expiration duration. If unspecified, then {@link #AUTH_TOKEN_EXPIRATION_SECONDS}
     * microseconds from the current time will be set. In both instances, the value will be the
     * maximum between itself and {@link #UPPER_SESSION_LIMIT_SECONDS}.
     *
     * @param authRequest The original authentication request.
     *
     * @return The expiration time of the session.
     */
    protected long getExpirationTime(AuthenticationRequest authRequest) {
        long expirationTimeSeconds = authRequest.sessionExpirationSeconds != null ?
                authRequest.sessionExpirationSeconds : AUTH_TOKEN_EXPIRATION_SECONDS;

        // Set a hard limit on the duration of a session if the expirationTimeMicros
        // exceeds the upper session expiration limit.
        if (this.UPPER_SESSION_LIMIT_SECONDS > 0 &&
                expirationTimeSeconds > this.UPPER_SESSION_LIMIT_SECONDS) {
            expirationTimeSeconds = this.UPPER_SESSION_LIMIT_SECONDS;
        }

        return Utils.fromNowMicrosUtc(TimeUnit.SECONDS.toMicros(expirationTimeSeconds));
    }
}
