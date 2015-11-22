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
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserService.UserState;
import com.vmware.xenon.services.common.authn.AuthenticationRequest.AuthenticationRequestType;

public class BasicAuthenticationService extends StatelessService {

    public static String SELF_LINK = ServiceUriPaths.CORE_AUTHN_BASIC;

    public static final String WWW_AUTHENTICATE_HEADER_NAME = "WWW-Authenticate";
    public static final String WWW_AUTHENTICATE_HEADER_VALUE = "Basic realm=\"dcp\"";
    public static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    public static final String BASIC_AUTH_NAME = "Basic";
    private static final String BASIC_AUTH_SEPERATOR = " ";
    private static final String BASIC_AUTH_USER_SEPERATOR = ":";

    @Override
    public void handleRequest(Operation op) {
        switch (op.getAction()) {
        case POST:
            handlePost(op);
            break;
        default:
            super.handleRequest(op);
        }
    }

    private void handlePost(Operation op) {
        AuthenticationRequestType requestType = op.getBody(AuthenticationRequest.class).requestType;
        // default to login for backward compatibility
        if (requestType == null) {
            requestType = AuthenticationRequestType.LOGIN;
        }
        switch (requestType) {
        case LOGIN:
            handleLogin(op);
            break;
        case LOGOUT:
            handleLogout(op);
            break;
        default:
            break;
        }
    }

    private void handleLogout(Operation op) {
        if (op.getAuthorizationContext() == null) {
            op.complete();
            return;
        }
        String userLink = op.getAuthorizationContext().getClaims().getSubject();
        if (!associateAuthorizationContext(op, userLink, 0)) {
            op.setStatusCode(Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD).complete();
            return;
        }
        op.complete();
    }

    private void handleLogin(Operation op) {
        String authHeader = op.getRequestHeader(AUTHORIZATION_HEADER_NAME);

        // if no header specified, send a 401 response and a header asking for basic auth
        if (authHeader == null) {
            op.addResponseHeader(WWW_AUTHENTICATE_HEADER_NAME, WWW_AUTHENTICATE_HEADER_VALUE);
            op.fail(Operation.STATUS_CODE_UNAUTHORIZED);
            return;
        }
        String[] authHeaderParts = authHeader.split(BASIC_AUTH_SEPERATOR);
        // malformed header; send a 400 response
        if (authHeaderParts.length != 2 || !authHeaderParts[0].equalsIgnoreCase(BASIC_AUTH_NAME)) {
            op.fail(Operation.STATUS_CODE_BAD_REQUEST);
            return;
        }
        String authString;
        try {
            authString = new String(Base64.getDecoder().decode(authHeaderParts[1]), Utils.CHARSET);
        } catch (UnsupportedEncodingException e) {
            logWarning("Exception decoding auth header: %s", Utils.toString(e));
            op.setStatusCode(Operation.STATUS_CODE_BAD_REQUEST).complete();
            return;
        }
        String[] userNameAndPassword = authString.split(BASIC_AUTH_USER_SEPERATOR);
        if (userNameAndPassword.length != 2) {
            op.fail(Operation.STATUS_CODE_BAD_REQUEST);
            return;
        }

        // validate that the user is valid in the system
        queryUserService(op, userNameAndPassword[0], userNameAndPassword[1]);
    }

    private void queryUserService(Operation parentOp, String userName, String password) {
        QueryTask q = new QueryTask();
        q.querySpec = new QueryTask.QuerySpecification();

        String kind = Utils.buildKind(UserState.class);
        QueryTask.Query kindClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(kind);
        q.querySpec.query.addBooleanClause(kindClause);

        QueryTask.Query emailClause = new QueryTask.Query()
                .setTermPropertyName(UserState.FIELD_NAME_EMAIL)
                .setTermMatchValue(userName);
        emailClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;

        q.querySpec.query.addBooleanClause(emailClause);
        q.taskInfo.isDirect = true;

        Operation.CompletionHandler userServiceCompletion = (o, ex) -> {
            if (ex != null) {
                logWarning("Exception validating user: %s", Utils.toString(ex));
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
            queryAuthStore(parentOp, userLink, userName, password);
        };

        Operation queryOp = Operation
                .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(q)
                .setCompletion(userServiceCompletion);
        setAuthorizationContext(queryOp, getSystemAuthorizationContext());
        sendRequest(queryOp);
    }

    private void queryAuthStore(Operation parentOp, String userLink, String userName,
            String password) {
        // query against the auth credentials store
        QueryTask authQuery = new QueryTask();
        authQuery.querySpec = new QueryTask.QuerySpecification();

        String authKind = Utils.buildKind(AuthCredentialsServiceState.class);
        QueryTask.Query authKindClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(authKind);
        authQuery.querySpec.query.addBooleanClause(authKindClause);

        QueryTask.Query authEmailClause = new QueryTask.Query()
                .setTermPropertyName(AuthCredentialsServiceState.FIELD_NAME_EMAIL)
                .setTermMatchValue(userName);
        authEmailClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
        authQuery.querySpec.query.addBooleanClause(authEmailClause);

        QueryTask.Query authCredentialsClause = new QueryTask.Query()
                .setTermPropertyName(AuthCredentialsServiceState.FIELD_NAME_PRIVATE_KEY)
                .setTermMatchValue(password);
        authCredentialsClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
        authQuery.querySpec.query.addBooleanClause(authCredentialsClause);
        authQuery.taskInfo.isDirect = true;
        Operation.CompletionHandler authCompletionHandler = (authOp, authEx) -> {
            if (authEx != null) {
                logWarning("Exception validating user credentials: %s",
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
            // set token validity to one hour - this is arbitrary at this point and will
            // need to be parameterized
            if (!associateAuthorizationContext(parentOp, userLink,
                    (Utils.getNowMicrosUtc() + TimeUnit.HOURS.toMicros(1)))) {
                parentOp.fail(Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD);
                return;
            }

            parentOp.complete();
        };

        Operation queryAuth = Operation
                .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(authQuery)
                .setCompletion(authCompletionHandler);
        setAuthorizationContext(queryAuth, getSystemAuthorizationContext());
        sendRequest(queryAuth);
    }

    private boolean associateAuthorizationContext(Operation op, String userLink, long expirationTime) {
        Claims.Builder builder = new Claims.Builder();
        builder.setIssuer(AuthenticationConstants.JWT_ISSUER);
        builder.setSubject(userLink);
        builder.setExpirationTime(expirationTime);

        // Generate token for set of claims
        Claims claims = builder.getResult();
        String token;

        try {
            token = getTokenSigner().sign(claims);
        } catch (Exception e) {
            logSevere(e);
            return false;
        }

        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(token);
        ab.setPropagateToClient(true);

        // Associate resulting authorization context with operation.
        setAuthorizationContext(op, ab.getResult());
        return true;
    }
}
