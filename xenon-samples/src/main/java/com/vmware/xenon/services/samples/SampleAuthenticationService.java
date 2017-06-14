/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.services.samples;

import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SystemUserService;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;

/**
 * This service provides the sample implementation of an AuthenticationService which
 * can be used by the ServiceHost to support external authentication.
 *
 * Some of the important functionalities of this service are:
 * <ol>
 * <li> Redirect un-authenticated requests to an external authentication provider </li>
 * <li> Retrieve accessToken from external authentication provider - LOGIN </li>
 * <li> Validate an accessToken with external authentication provider - TOKEN VERIFICATION </li>
 * <li> Validate an accessToken with external authentication provider - LOGOUT </li>
 * </ol>
 * <br>
 * The role of this service is to let the developer of an AuthenticationService
 * know the contracts between the xenon framework his/her service.
 * So, most of the functionalities in this sample service are inline, however the actual service
 * implementations will need to talk to the external authentication provider
 */
public class SampleAuthenticationService extends StatelessService {

    public static final String SELF_LINK = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHN,
            "sample");

    // this sample service will use the following hard coded string as access token
    public static final String ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."
            + "eyJpc3MiOiJ4biIsInN1YiI6Ii9jb3JlL2F1dGh6L3Vz";

    /**
     * GET method needs to be implementing the LOGIN requests only, its useful to expose
     * LOGIN in GET to support redirection from external authentication providers after user
     * authenticates against them. Since we need to support external redirections we don't
     * rely on any pragma here.
     */
    @Override
    public void handleGet(Operation op) {
        // actual implementation will expect this to be called from an external
        // authentication provider and use any kind of code shared by it to
        // get an accessToken and then use the access token to create an authorization
        // context and use it.

        // actual implementations will need to generate the Claims object
        // by decoding the access token provided from the external authentication provider

        // the sample service is using a locally created Claims object
        Claims claims = getClaims(false);

        // just use the predefined ACCESS_TOKEN to create an authorization context
        // and set it.
        associateAuthorizationContext(claims, this, op, ACCESS_TOKEN);
        op.complete();
    }

    /**
     * POST methods are meant for LOGIN, TOKEN VERIFICATION and LOGOUT based on the
     * pragma's PRAGMA_DIRECTIVE_AUTHENTICATE and Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN
     * in the header.
     */

    @Override
    public void handlePost(Operation op) {
        // actual implementation will retrieve access token, verify an existing token or
        // invalidate a token for logout based on the pragma header.

        // the sample service is just doing a token verification based on pragma header
        if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN)) {
            op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN);
            String token = BasicAuthenticationUtils.getAuthToken(op);
            if (token == null) {
                op.fail(new IllegalArgumentException("Token is empty"));
                return;
            }

            if (token.equals(ACCESS_TOKEN)) {
                // create and return a claims object for system user since our test uses system user
                Claims claims = getClaims(false);
                AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
                ab.setClaims(claims);
                ab.setToken(token);
                op.setBodyNoCloning(ab.getResult());
                op.complete();
                return;
            }
            op.fail(new IllegalArgumentException("Invalid Token!"));
        } else if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_AUTHN_INVALIDATE)) {
            op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_AUTHN_INVALIDATE);

            // create claims object with ZERO expiration time
            Claims claims = getClaims(true);
            associateAuthorizationContext(claims, this, op, ACCESS_TOKEN);
            op.complete();
            return;
        } else {
            // default is authentication, no need to check for Pragma header

            // the sample service is using a locally created Claims object
            Claims claims = getClaims(false);

            // just use the predefined ACCESS_TOKEN to create an authorization context
            // and set it.
            associateAuthorizationContext(claims, this, op, ACCESS_TOKEN);
            op.complete();
        }
    }

    private void associateAuthorizationContext(Claims claims, Service service, Operation op, String token) {
        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(token);

        // this is required for the NettyClientRequestHandler to propagate the
        // access token as headers to the clients.
        ab.setPropagateToClient(true);

        // associate resulting authorization context with operation.
        service.setAuthorizationContext(op, ab.getResult());
    }

    private Claims getClaims(boolean isLogout) {
        Claims.Builder builder = new Claims.Builder();
        builder.setIssuer(AuthenticationConstants.DEFAULT_ISSUER);

        // the claims object has to be associated with a valid userLink as subject,
        // in case of actual implementation this user if already does not exist
        // will have to be created.
        builder.setSubject(SystemUserService.SELF_LINK);

        if (isLogout) {
            builder.setExpirationTime(0L);
        }

        return builder.getResult();
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    /**
     * Override this method to specify an URL to redirect any un-authenticated requests.
     */
    @Override
    public boolean queueRequest(Operation op) {
        // its important to only redirect un-authenticated requests for this service
        if (op.getUri().getPath().equals(getSelfLink())) {
            return false;
        }

        // the sample says redirect to vmware.com, it should be the url of the external
        // authentication provider.

        // the code to remember the actual url requested before redirection can go in here
        // so that the authentication service respond with a redirect later after
        // getting the token.
        op.addResponseHeader(Operation.LOCATION_HEADER, "http://www.vmware.com");
        op.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
        op.complete();
        return true;
    }
}
