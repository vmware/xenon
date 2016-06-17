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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.authn.AuthenticationRequest;
import com.vmware.xenon.services.common.authn.BasicAuthenticationService;

public class ExampleServiceHost extends ServiceHost {

    public static class ExampleHostArguments extends Arguments {
        /**
         * The email address of a user that should be granted "admin" privileges to all services
         */
        public String adminUser;

        /**
         * The password of the adminUser
         */
        public String adminUserPassword;

        /**
         * The email address of a user that should be granted privileges just to example services
         * that they own
         */
        public String exampleUser;

        /**
         * The password of the exampleUser
         */
        public String exampleUserPassword;
    }

    private ExampleHostArguments args;

    public static void main(String[] args) throws Throwable {
        ExampleServiceHost h = new ExampleServiceHost();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    @Override
    public ServiceHost initialize(String[] args) throws Throwable {
        this.args = new ExampleHostArguments();
        super.initialize(args, this.args);
        if (this.args.adminUser != null && this.args.adminUserPassword == null) {
            throw new IllegalStateException("adminUser specified, but not adminUserPassword");
        }
        if (this.args.exampleUser != null && this.args.exampleUserPassword == null) {
            throw new IllegalStateException("exampleUser specified, but not exampleUserPassword");
        }
        return this;
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        setAuthorizationContext(this.getSystemAuthorizationContext());

        // Start the example service factory
        super.startFactory(ExampleService.class, ExampleService::createFactory);

        // Start the example task service factory: when it receives a task, it will delete
        // all example services
        super.startFactory(ExampleTaskService.class, ExampleTaskService::createFactory);

        // Start the root namespace factory: this will respond to the root URI (/) and list all
        // the factory services.
        super.startService(new RootNamespaceService());

        //Start the TaskMigrationService factory
        super.startFactory(new MigrationTaskService());

        // The args are null because many of the tests use this class (via VerificationHost)
        // without providing arguments.
        if (this.args != null) {
            if (this.args.adminUser != null) {
                AuthorizationSetupHelper.create()
                        .setHost(this)
                        .setUserEmail(this.args.adminUser)
                        .setUserPassword(this.args.adminUserPassword)
                        .setIsAdmin(true)
                        .start();

                String sourceHost = System.getProperty("sourceHostUri");

                // If a source node group supplied, create a MigrationTaskService to migrate users
                // If not specified, create test users that should get migrated by another host
                if (sourceHost != null && !sourceHost.trim().equals("")) {
                    log(Level.INFO, "Starting up in upgrade mode! Migrate state from [sourceHost=%s]", sourceHost);

                    // get AuthorityContext for our AdminUser (which we will use to create MigrationTaskService)
                    AuthenticationRequest authRequest = new AuthenticationRequest();
                    authRequest.requestType = AuthenticationRequest.AuthenticationRequestType.LOGIN;
                    String authValue = constructBasicAuth(this.args.adminUser, this.args.adminUserPassword);
                    log(Level.INFO, "Sending Authorization Request Header Value: %s", authValue);

                    URI sourceHostUri = UriUtils.buildUri(sourceHost);
                    URI sourceAuthUri = UriUtils.buildUri(sourceHostUri, BasicAuthenticationService.SELF_LINK);
                    log(Level.INFO, "Authenticating against: %s", sourceAuthUri);

                    Operation postForAuth = Operation.createPost(sourceAuthUri)
                            .setBody(authRequest)
                            .addRequestHeader(BasicAuthenticationService.AUTHORIZATION_HEADER_NAME, authValue)
                            .setReferer(getUri())
                            .setCompletion((op, err) -> {
                                if (err != null) {
                                    log(Level.SEVERE, "Error getting auth token: %s", Utils.toString(err));
                                    throw new RuntimeException(err);
                                }
                                log(Level.INFO, "Success! Got response auth token: %s", op.getAuthorizationContext().getToken());

                                setAuthorizationContext(op.getAuthorizationContext());
                                log(Level.INFO, "Creating MigrationTaskService under %s user", this.args.adminUser);
                                MigrationTaskService.State state = new MigrationTaskService.State();
                                state.sourceFactoryLink = ServiceUriPaths.CORE_AUTHZ_USERS;
                                state.destinationFactoryLink = ServiceUriPaths.CORE_AUTHZ_USERS;
                                state.sourceNodeGroupReference = UriUtils.buildUri(sourceHostUri, ServiceUriPaths.DEFAULT_NODE_GROUP);
                                state.destinationNodeGroupReference = UriUtils.buildUri(this, ServiceUriPaths.DEFAULT_NODE_GROUP);

                                Operation registerOp = Operation
                                        .createPost(this, MigrationTaskService.FACTORY_LINK)
                                        .setBody(state)
                                        .setCompletion((op1, err1) -> {
                                            if (err1 != null) {
                                                log(Level.SEVERE, "Failed to create MigrationTaskService: %s", Utils.toString(err1));
                                            }

                                            MigrationTaskService.State response = op1.getBody(MigrationTaskService.State.class);
                                            log(Level.INFO, "Successfully created MigrationTaskService: %s", response.documentSelfLink);
                                        })
                                        .setReferer(getUri());
                                sendRequest(registerOp);
                            });

                    // Just simple proof of concept; need to wait until everything is ready first
                    schedule(() -> {
                        log(Level.INFO, "Sending request to get auth token for [email=%s]", this.args.adminUser);
                        sendRequest(postForAuth);
                    }, 5L, TimeUnit.SECONDS);

                } else {
                    log(Level.INFO, "No source node group specified. Creating test users on this node...");

                    List<String> emails = Arrays.asList("user1@migration.test", "user2@migration.test");
                    for (String email : emails) {
                        UserService.UserState user = new UserService.UserState();
                        user.email = email;
                        Operation createUser = Operation.createPost(this, UserService.FACTORY_LINK)
                                .setBody(user)
                                .setReferer(getUri())
                                .setCompletion((op, err) -> {
                                    if (err != null) {
                                        log(Level.SEVERE, "Error creating user: %s", email);
                                        throw new RuntimeException(err);
                                    }
                                    UserService.UserState response = op.getBody(UserService.UserState.class);
                                    log(Level.INFO, "Created user [email=%s]: %s", email, response.documentSelfLink);
                                });

                        // Just simple proof of concept; need to wait until everything is ready first
                        schedule(() -> {
                            log(Level.INFO, "Sending request to create user [email=%s]", email);
                            sendRequest(createUser);
                        }, 5L, TimeUnit.SECONDS);
                    }
                }
            }
            if (this.args.exampleUser != null) {
                AuthorizationSetupHelper.create()
                        .setHost(this)
                        .setUserEmail(this.args.exampleUser)
                        .setUserPassword(this.args.exampleUserPassword)
                        .setIsAdmin(false)
                        .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                        .start();
            }
        }

        setAuthorizationContext(null);

        return this;
    }

    /**
     * Supports loginUser() by creating a Basic Auth header
     */
    private static String constructBasicAuth(String name, String password) {
        String userPass = String.format("%s:%s", name, password);
        byte[] bytes = Base64.getEncoder().encode(userPass.getBytes(StandardCharsets.UTF_8));
        String encodedUserPass = new String(bytes, StandardCharsets.UTF_8);
        String basicAuth = "Basic " + encodedUserPass;
        return basicAuth;
    }
}