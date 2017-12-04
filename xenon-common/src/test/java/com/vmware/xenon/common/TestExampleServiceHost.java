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

package com.vmware.xenon.common;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static com.vmware.xenon.services.common.authn.BasicAuthenticationUtils.constructBasicAuth;

import java.net.URI;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.services.common.ExampleServiceHost;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserService;
import com.vmware.xenon.services.common.authn.AuthenticationRequest;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;

public class TestExampleServiceHost extends BasicReusableHostTestCase {

    private static final String adminUser = "admin@localhost";
    private static final String exampleUser = "example@localhost";

    /**
     * Verify that the example service host creates users as expected.
     *
     * In theory we could test that authentication and authorization works correctly
     * for these users. It's not critical to do here since we already test it in
     * TestAuthSetupHelper.
     */
    @Test
    public void createUsers() throws Throwable {
        ExampleServiceHost h = new ExampleServiceHost();
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        try {
            String bindAddress = "127.0.0.1";

            String[] args = {
                    "--sandbox="
                            + tmpFolder.getRoot().getAbsolutePath(),
                    "--port=0",
                    "--bindAddress=" + bindAddress,
                    "--isAuthorizationEnabled=" + Boolean.TRUE.toString(),
                    "--adminUser=" + adminUser,
                    "--adminUserPassword=" + adminUser,
                    "--exampleUser=" + exampleUser,
                    "--exampleUserPassword=" + exampleUser,
            };

            h.initialize(args);
            h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
            h.start();

            URI hostUri = h.getUri();
            String authToken = loginUser(hostUri);
            waitForUsers(hostUri, authToken);

        } finally {
            h.stop();
            tmpFolder.delete();
        }
    }

    /**
     * Supports createUsers() by logging in as the admin. The admin user
     * isn't created immediately, so this polls.
     */
    private String loginUser(URI hostUri) throws Throwable {
        URI usersLink = UriUtils.buildUri(hostUri, UserService.FACTORY_LINK);
        // wait for factory availability
        this.host.setSystemAuthorizationContext();
        this.host.waitForReplicatedFactoryServiceAvailable(usersLink);
        this.host.resetAuthorizationContext();

        String basicAuth = constructBasicAuth(adminUser, adminUser);
        URI loginUri = UriUtils.buildUri(hostUri, ServiceUriPaths.CORE_AUTHN_BASIC);
        AuthenticationRequest login = new AuthenticationRequest();
        login.requestType = AuthenticationRequest.AuthenticationRequestType.LOGIN;

        String[] authToken = new String[1];
        authToken[0] = null;

        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            Operation loginPost = Operation.createPost(loginUri)
                    .setBody(login)
                    .addRequestHeader(BasicAuthenticationService.AUTHORIZATION_HEADER_NAME,
                            basicAuth)
                    .forceRemote()
                    .setCompletion((op, ex) -> {
                        if (ex != null) {
                            this.host.completeIteration();
                            return;
                        }
                        authToken[0] = op.getResponseHeader(Operation.REQUEST_AUTH_TOKEN_HEADER);
                        this.host.completeIteration();
                    });

            this.host.testStart(1);
            this.host.send(loginPost);
            this.host.testWait();

            if (authToken[0] != null) {
                break;
            }
            Thread.sleep(250);
        }

        if (new Date().after(exp)) {
            throw new TimeoutException();
        }

        assertNotNull(authToken[0]);

        return authToken[0];
    }

    /**
     * Supports createUsers() by waiting for two users to be created. They aren't created immediately,
     * so this polls.
     */
    private void waitForUsers(URI hostUri, String authToken) throws Throwable {
        URI usersLink = UriUtils.buildUri(hostUri, UserService.FACTORY_LINK);
        Integer[] numberUsers = new Integer[1];
        for (int i = 0; i < 20; i++) {
            Operation get = Operation.createGet(usersLink)
                    .forceRemote()
                    .addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, authToken)
                    .setCompletion((op, ex) -> {
                        if (ex != null) {
                            if (op.getStatusCode() != Operation.STATUS_CODE_FORBIDDEN) {
                                this.host.failIteration(ex);
                                return;
                            } else {
                                numberUsers[0] = 0;
                                this.host.completeIteration();
                                return;
                            }
                        }
                        ServiceDocumentQueryResult response = op
                                .getBody(ServiceDocumentQueryResult.class);
                        assertTrue(response != null && response.documentLinks != null);
                        numberUsers[0] = response.documentLinks.size();
                        this.host.completeIteration();
                    });

            this.host.testStart(1);
            this.host.send(get);
            this.host.testWait();

            if (numberUsers[0] == 2) {
                break;
            }
            Thread.sleep(250);
        }
        assertTrue(numberUsers[0] == 2);
    }

}
