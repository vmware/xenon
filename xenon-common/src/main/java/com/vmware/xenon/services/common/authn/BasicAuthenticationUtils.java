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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.AuthUtils;
import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.jwt.Verifier;
import com.vmware.xenon.common.jwt.Verifier.TokenException;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Utility class defining helper methods for basic authentication
 */
public final class BasicAuthenticationUtils {

    public static final String WWW_AUTHENTICATE_HEADER_NAME = "WWW-Authenticate";
    public static final String WWW_AUTHENTICATE_HEADER_VALUE = "Basic realm=\"xenon\"";
    public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    public static final String BASIC_AUTH_NAME = "Basic";
    private static final String BASIC_AUTH_SEPERATOR = " ";
    private static final String BASIC_AUTH_USER_SEPERATOR = ":";

    private static final long AUTH_TOKEN_EXPIRATION_MICROS = Long.getLong(
            Utils.PROPERTY_NAME_PREFIX + "BasicAuthenticationService.AUTH_TOKEN_EXPIRATION_MICROS",
            TimeUnit.HOURS.toMicros(1));

    private BasicAuthenticationUtils() {

    }

    /**
     * Holds user and auth queries
     */
    public static class BasicAuthenticationContext {
        public Query userQuery;
        public Query authQuery;
    }

    /**
     * Utility method to logout an user
     * @param service service invoking this method
     * @param op Operation context of the logout request
     */
    public static void handleLogout(StatelessService service, Operation op) {
        if (op.getAuthorizationContext() == null) {
            op.complete();
            return;
        }
        String userLink = op.getAuthorizationContext().getClaims().getSubject();
        if (!associateAuthorizationContext(service, op, userLink, 0)) {
            op.setStatusCode(Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD).complete();
            return;
        }
        op.complete();
    }

    /**
     * Utility method to log an user into the system
     * @param service service invoking this method
     * @param op Operation context of the login request
     * @param authContext authContext to perform the login checks
     */
    public static void handleLogin(StatelessService service, Operation op,
            BasicAuthenticationContext authContext) {
        queryUserService(service, op, authContext);
    }

    /**
     * Utility method to parse a request to extract the username and password
     * @param service service invoking this method
     * @param op Operation context of the login request
     * @return
     */
    public static String[] parseRequest(StatelessService service, Operation op) {
        // Attempt to fetch and use userInfo, if AUTHORIZATION_HEADER_NAME is null.
        String authHeader = op.getRequestHeader(AUTHORIZATION_HEADER_NAME);
        String userInfo = op.getUri().getUserInfo();
        String authString;
        if (authHeader != null) {
            String[] authHeaderParts = authHeader.split(BASIC_AUTH_SEPERATOR);
            // malformed header; send a 400 response
            if (authHeaderParts.length != 2 || !authHeaderParts[0].equalsIgnoreCase(BASIC_AUTH_NAME)) {
                op.fail(Operation.STATUS_CODE_BAD_REQUEST);
                return null;
            }

            try {
                authString = new String(Base64.getDecoder().decode(authHeaderParts[1]), Utils.CHARSET);
            } catch (UnsupportedEncodingException e) {
                service.logWarning("Exception decoding auth header: %s", Utils.toString(e));
                op.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST).complete();
                return null;
            }

        } else if (userInfo != null) {
            authString = userInfo;
        } else {
            // if no header or userInfo is specified, send a 401 response and a header asking for basic auth
            op.addResponseHeader(WWW_AUTHENTICATE_HEADER_NAME, WWW_AUTHENTICATE_HEADER_VALUE);
            op.fail(Operation.STATUS_CODE_UNAUTHORIZED);
            return null;
        }

        String[] userNameAndPassword = authString.split(BASIC_AUTH_USER_SEPERATOR);
        if (userNameAndPassword.length != 2) {
            op.fail(Operation.STATUS_CODE_BAD_REQUEST);
            return null;
        }
        return userNameAndPassword;
    }

    /**
     * Utility method for constructing a Basic authorization header from the provided credentials
     * @param name the username (or email)
     * @param password the password
     * @return the Base64 encoded auth request header to pass to request an auth token.
     */
    public static String constructBasicAuth(String name, String password) {
        String userPass = String.format("%s:%s", name, password);
        byte[] bytes = Base64.getEncoder().encode(userPass.getBytes(StandardCharsets.UTF_8));
        String encodedUserPass = new String(bytes, StandardCharsets.UTF_8);
        String basicAuth = "Basic " + encodedUserPass;
        return basicAuth;
    }

    /**
     * This method invokes the query specified by the service to check if the user is
     * valid
     * @param service service invoking this method
     * @param op Operation context of the login request
     * @param authContext authContext to perform the login checks
     */
    private static void queryUserService(StatelessService service, Operation parentOp, BasicAuthenticationContext authContext) {
        QueryTask q = new QueryTask();
        q.querySpec = new QueryTask.QuerySpecification();
        q.querySpec.query = authContext.userQuery;
        q.taskInfo.isDirect = true;

        Operation.CompletionHandler userServiceCompletion = (o, ex) -> {
            if (ex != null) {
                service.logWarning("Exception validating user: %s", Utils.toString(ex));
                parentOp.setBodyNoCloning(o.getBodyRaw()).fail(o.getStatusCode());
                return;
            }

            QueryTask rsp = o.getBody(QueryTask.class);
            if (rsp.results.documentLinks.isEmpty()) {
                parentOp.fail(Operation.STATUS_CODE_FORBIDDEN);
                return;
            }

            // The user is valid; query the auth provider to check if the credentials match
            String userLink = rsp.results.documentLinks.get(0);
            queryAuthStore(service, parentOp, userLink, authContext);
        };

        Operation queryOp = Operation
                .createPost(AuthUtils.buildAuthProviderHostUri(service.getHost(), ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(q)
                .setCompletion(userServiceCompletion);
        service.setAuthorizationContext(queryOp, service.getSystemAuthorizationContext());
        service.sendRequest(queryOp);
    }

    /**
     * This method invokes the query specified by the service to check if
     * the user credentials are valid
     * @param service service invoking this method
     * @param parentOop Operation context of the login request
     * @param userLink service link for the user
     * @param authContext authContext to perform the login checks
     */
    private static void queryAuthStore(StatelessService service, Operation parentOp, String userLink,
            BasicAuthenticationContext authContext) {
        // query against the auth credentials store
        QueryTask authQuery = new QueryTask();
        authQuery.querySpec = new QueryTask.QuerySpecification();
        authQuery.querySpec.query = authContext.authQuery;
        authQuery.taskInfo.isDirect = true;
        Operation.CompletionHandler authCompletionHandler = (authOp, authEx) -> {
            if (authEx != null) {
                service.logWarning("Exception validating user credentials: %s",
                        Utils.toString(authEx));
                parentOp.setBodyNoCloning(authOp.getBodyRaw()).fail(
                        Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD);
                return;
            }

            QueryTask authRsp = authOp.getBody(QueryTask.class);
            if (authRsp.results.documentLinks.isEmpty()) {
                parentOp.fail(Operation.STATUS_CODE_FORBIDDEN);
                return;
            }

            AuthenticationRequest authRequest = parentOp.getBody(AuthenticationRequest.class);
            long expirationTime;
            if (authRequest.sessionExpirationSeconds != null) {
                expirationTime = Utils.fromNowMicrosUtc(TimeUnit.SECONDS
                        .toMicros(authRequest.sessionExpirationSeconds));
            } else {
                expirationTime = Utils.fromNowMicrosUtc(AUTH_TOKEN_EXPIRATION_MICROS);
            }

            // set token validity
            if (!associateAuthorizationContext(service, parentOp, userLink, expirationTime)) {
                parentOp.fail(Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD);
                return;
            }

            parentOp.complete();
        };

        Operation queryAuth = Operation
                .createPost(AuthUtils.buildAuthProviderHostUri(service.getHost(), ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(authQuery)
                .setCompletion(authCompletionHandler);
        service.setAuthorizationContext(queryAuth, service.getSystemAuthorizationContext());
        service.sendRequest(queryAuth);
    }

    /**
     * This method associates an auth context with the input operation
     * @param service service invoking this method
     * @param op Operation context of the login request
     * @param userLink service link for the user
     * @param expirationTime expiration time for the auth token
     * @return
     */
    private static boolean associateAuthorizationContext(StatelessService service, Operation op, String userLink, long expirationTime) {
        Claims.Builder builder = new Claims.Builder();
        builder.setIssuer(AuthenticationConstants.DEFAULT_ISSUER);
        builder.setSubject(userLink);
        builder.setExpirationTime(expirationTime);

        // Generate token for set of claims
        Claims claims = builder.getResult();
        String token;

        try {
            token = service.getTokenSigner().sign(claims);
        } catch (Exception e) {
            service.logSevere(e);
            return false;
        }

        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(token);
        ab.setPropagateToClient(true);

        // Associate resulting authorization context with operation.
        service.setAuthorizationContext(op, ab.getResult());
        return true;
    }

    /**
     * Utility method to verify the token in incoming request
     * @param service service invoking this method
     * @param op Operation context of the request
     */
    public static void handleTokenVerify(StatelessService service, Operation op) {
        String token = getAuthToken(op);
        if (token == null) {
            Exception e = new IllegalArgumentException("Token is empty");
            service.logWarning("Error verifying token: %s", e.getMessage());
            op.fail(e);
            return;
        }
        try {
            // use JWT token verifier for basic auth
            Verifier verifier = service.getTokenVerifier();
            Claims claims = verifier.verify(token, Claims.class);
            op.setBody(claims);
            op.complete();
        } catch (TokenException | GeneralSecurityException e) {
            service.logWarning("Error verifying token: %s", e.getMessage());
            op.fail(e);
        }
    }

    /**
     * Extracts the auth token from the request
     * @param op Operation context of the request
     * @return auth token for the request
     */
    public static String getAuthToken(Operation op) {
        String token = op.getRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER);
        if (token == null) {
            Map<String, String> cookies = op.getCookies();
            if (cookies == null) {
                return null;
            }
            token = cookies.get(AuthenticationConstants.REQUEST_AUTH_TOKEN_COOKIE);
        }
        return token;
    }

}
