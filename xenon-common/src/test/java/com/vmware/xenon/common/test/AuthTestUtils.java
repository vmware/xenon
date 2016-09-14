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

package com.vmware.xenon.common.test;

import static java.util.stream.Collectors.toSet;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.authn.AuthenticationRequest;
import com.vmware.xenon.services.common.authn.AuthenticationRequest.AuthenticationRequestType;

/**
 * Provides auth related methods for test.
 */
public class AuthTestUtils {

    private static final Method GET_SYSTEM_AUTH_CONTEXT_METHOD;
    private static final Method SET_SYSTEM_AUTH_CONTEXT_METHOD;

    static {
        try {
            GET_SYSTEM_AUTH_CONTEXT_METHOD = ServiceHost.class.getDeclaredMethod("getSystemAuthorizationContext");
            SET_SYSTEM_AUTH_CONTEXT_METHOD = ServiceHost.class.getDeclaredMethod("setAuthorizationContext", AuthorizationContext.class);
            GET_SYSTEM_AUTH_CONTEXT_METHOD.setAccessible(true);
            SET_SYSTEM_AUTH_CONTEXT_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to initialize system auth set/get methods reflectively.");
        }
    }

    private AuthTestUtils() {
    }

    /**
     * Login to one of the node in the node group.
     *
     * The nodes in the node group should have same auth policy in order for the login to work
     * seamlessly on all nodes.
     *
     * @return auth token
     */
    public static String login(TestNodeGroupManager nodeGroup, String username, String password) {
        ServiceHost peer = nodeGroup.getHost();
        return login(peer, username, password, true);
    }

    public static String login(ServiceHost host, String username, String password) {
        return login(host, username, password, true);
    }

    /**
     * Attempt to login one of the host in the node group, and expect it to fail.
     */
    public static void loginExpectFailure(TestNodeGroupManager nodeGroup, String username, String password) {
        ServiceHost peer = nodeGroup.getHost();
        loginExpectFailure(peer, username, password);
    }

    public static void loginExpectFailure(ServiceHost host, String username, String password) {
        login(host, username, password, false);
    }

    private static String login(ServiceHost host, String username, String password, boolean expectSuccess) {
        String userInfo = username + ":" + password;
        URI uri = UriUtils.buildUri(host, ServiceUriPaths.CORE_AUTHN_BASIC, "", userInfo);

        AuthenticationRequest body = new AuthenticationRequest();
        body.requestType = AuthenticationRequestType.LOGIN;

        // perform login request. it has to be remote request in order to receive token from remote context
        Operation post = Operation.createPost(uri).setBody(body).forceRemote();

        TestRequestSender sender = new TestRequestSender(host);
        if (expectSuccess) {
            Operation response = sender.sendAndWait(post);
            return response.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER);
        } else {
            sender.sendAndWaitFailure(post);
            return null;
        }
    }

    /**
     * Login and set the auth token to be used by {@link TestRequestSender}.
     *
     * NOTE:
     * Auth token is only used by {@link TestRequestSender}.
     *
     * @return auth token
     */
    public static String loginAndSetToken(TestNodeGroupManager nodeGroup, String username, String password) {
        ServiceHost peer = nodeGroup.getHost();
        String authToken = login(peer, username, password);
        TestRequestSender.setAuthToken(authToken);
        return authToken;
    }

    public static String loginAndSetToken(ServiceHost host, String username, String password) {
        String authToken = login(host, username, password);
        TestRequestSender.setAuthToken(authToken);
        return authToken;
    }

    /**
     * Logout from nodes in the node group.
     */
    public static void logout(TestNodeGroupManager nodeGroup) {
        // to clear the cookie stored in service client in host, needs perform logout on all hosts
        nodeGroup.getAllHosts().forEach(AuthTestUtils::logout);
    }

    public static void logout(ServiceHost host) {

        // make logout request
        URI uri = UriUtils.buildUri(host, ServiceUriPaths.CORE_AUTHN_BASIC);
        AuthenticationRequest body = new AuthenticationRequest();
        body.requestType = AuthenticationRequestType.LOGOUT;

        Operation post = Operation.createPost(uri).setBody(body);

        TestRequestSender sender = new TestRequestSender(host);
        sender.sendAndWait(post);

        // clear auth token set in request sender
        TestRequestSender.clearAuthToken();

        // clear cookie
        ServiceClient client = host.getClient();
        if (client instanceof NettyHttpServiceClient) {
            // since login response contains xenon-auth-cookie, client cookieJar keeps that entry.
            // clear all cookies for now.
            ((NettyHttpServiceClient) client).clearCookieJar();
        }
    }


    /**
     * Set system auth context to the host.
     *
     * This method is used descriptively paired with {@link #resetAuthorizationContext}.
     * Please see {@link #executeWithSystemAuthContext} for functional way using lambda.
     *
     * @param host a host to set system auth context
     * @return system auth context
     * @see #resetAuthorizationContext
     * @see #executeWithSystemAuthContext
     */
    public static AuthorizationContext setSystemAuthorizationContext(ServiceHost host) {
        // use reflection to set auth context
        AuthorizationContext[] result = new AuthorizationContext[]{null};
        ExceptionTestUtils.executeSafely(() -> {
            AuthorizationContext systemAuthContext = (AuthorizationContext) GET_SYSTEM_AUTH_CONTEXT_METHOD.invoke(host);
            SET_SYSTEM_AUTH_CONTEXT_METHOD.invoke(host, systemAuthContext);
            result[0] = systemAuthContext;
        });
        return result[0];
    }

    /**
     * Reset(nullify) the authorization context on the host.
     *
     * Mainly used paired with {@link #setSystemAuthorizationContext}.
     * For setting system auth context for lambda code block, see {@link #executeWithSystemAuthContext}.
     *
     * @param host a host to reset auth context
     * @see #setSystemAuthorizationContext
     * @see #executeWithSystemAuthContext
     */
    public static void resetAuthorizationContext(ServiceHost host) {
        ExceptionTestUtils.executeSafely(() -> {
            SET_SYSTEM_AUTH_CONTEXT_METHOD.invoke(host, new Object[]{null});
        });
    }

    /**
     * Perform give lambda code block under system auth context.
     *
     * see {@link #setSystemAuthorizationContext} and {@link #resetAuthorizationContext} for descriptively
     * perform logic under system auth context.
     *
     * @see #setSystemAuthorizationContext
     * @see #resetAuthorizationContext
     */
    public static void executeWithSystemAuthContext(ServiceHost host, ExecutableBlock block) {
        executeWithSystemAuthContext(Arrays.asList(host), block);
    }

    public static void executeWithSystemAuthContext(TestNodeGroupManager nodeGroup, ExecutableBlock block) {
        executeWithSystemAuthContext(nodeGroup.getAllHosts(), block);
    }

    public static void executeWithSystemAuthContext(Collection<ServiceHost> hosts, ExecutableBlock block) {

        // currently this method does not keep track of existing auth context.
        // If we need to recover existing auth context, retrieve and set them back in this method.

        Set<ServiceHost> authEnabledHosts = hosts.stream()
                .filter(ServiceHost::isAuthorizationEnabled)
                .collect(toSet());

        authEnabledHosts.forEach(AuthTestUtils::setSystemAuthorizationContext);
        ExceptionTestUtils.executeSafely(block);
        authEnabledHosts.forEach(AuthTestUtils::resetAuthorizationContext);
    }
}
