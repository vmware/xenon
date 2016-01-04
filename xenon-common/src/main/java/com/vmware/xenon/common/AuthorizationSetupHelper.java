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

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.Policy;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;
import com.vmware.xenon.services.common.UserService.UserState;

/**
 * This class assists clients (generally the ServiceHost or its subclasses) in creating users.
 * It creates a user with credentials, adds the user to a usergroup that contains only that
 * user, creates a resource group, and a role that ties them all together.
 *
 * If authorization is enabled, there must be at least one privileged user created by the host,
 * otherwise no clients can connect.
 *
 * Creating an administrative user looks like this:
 *
 *   AuthorizationSetupHelper.create()
 *         .setHost(this)
 *         .setUserEmail(this.args.adminUser)
 *         .setUserPassword(this.args.adminUserPassword)
 *         .setIsAdmin(true)
 *         .start();
 *
 * Creating a non-administrative user will grant the user access to all documents of a
 * given kind that are owned by that user. This is not very generic: the functionality
 * in this class will be extended as necessary. Creating a non-administrativer user that
 * can access ExampleService documents they own looks like this:
 *
 *   AuthorizationSetupHelper.create()
 *         .setHost(this)
 *         .setUserEmail(this.args.exampleUser)
 *         .setUserPassword(this.args.exampleUserPassword)
 *         .setIsAdmin(false)
 *         .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
 *         .start();
 *
 * As a note on the limitations: this user doesn't have access to modify their
 * credentials, only documents of type ExampleServiceState
 *
 */
public class AuthorizationSetupHelper {

    @FunctionalInterface
    public static interface AuthSetupCompletion {
        void handle(Exception ex);
    }

    /**
     * The steps we follow in order to fully create a user. See {@link setupUser} for details
     */
    private enum UserCreationStep {
        QUERY_USER, MAKE_USER, MAKE_CREDENTIALS, MAKE_USER_GROUP, MAKE_RESOURCE_GROUP, MAKE_ROLE, SUCCESS, FAILURE
    }

    private String userEmail;
    private String userPassword;
    private boolean isAdmin;
    private String documentKind;
    private ServiceHost host;
    private AuthSetupCompletion completion;

    private UserCreationStep currentStep;
    private URI referer;
    private String userSelfLink;
    private String userGroupSelfLink;
    private String resourceGroupSelfLink;
    private String roleSelfLink;

    private String failureMessage;

    public static AuthorizationSetupHelper create() {
        return new AuthorizationSetupHelper();
    }

    public AuthorizationSetupHelper setUserEmail(String userEmail) {
        this.userEmail = userEmail;
        return this;
    }

    public AuthorizationSetupHelper setUserPassword(String userPassword) {
        this.userPassword = userPassword;
        return this;
    }

    public AuthorizationSetupHelper setIsAdmin(boolean isAdmin) {
        this.isAdmin = isAdmin;
        return this;
    }

    public AuthorizationSetupHelper setDocumentKind(String documentKind) {
        this.documentKind = documentKind;
        return this;
    }

    public AuthorizationSetupHelper setHost(ServiceHost host) {
        this.host = host;
        this.referer = UriUtils.buildUri(host, "");
        return this;
    }

    public AuthorizationSetupHelper setCompletion(AuthSetupCompletion completion) {
        this.completion = completion;
        return this;
    }

    public AuthorizationSetupHelper start() {
        validate();
        this.currentStep = UserCreationStep.QUERY_USER;
        this.setupUser();
        return this;
    }

    private void validate() {
        if (this.userEmail == null) {
            throw new IllegalStateException("Missing user email");
        }
        if (this.userPassword == null) {
            throw new IllegalStateException("Missing user password");
        }
        if (this.host == null) {
            throw new IllegalStateException("Missing host");
        }
        if (!this.isAdmin && this.documentKind == null) {
            throw new IllegalStateException("User has access to nothing");
        }
    }

    /**
     * The state machine for creating a user.
     *
     * Based on the current step in creating a user, this dispatches to the relevant
     * method. This isn't needed: it's syntactic sugar to make it easier to follow
     * a set of chained completion handlers.
     */
    private void setupUser() {
        switch (this.currentStep) {
        case QUERY_USER:
            queryUser();
            break;
        case MAKE_USER:
            makeUser();
            break;
        case MAKE_CREDENTIALS:
            makeCredentials();
            break;
        case MAKE_USER_GROUP:
            makeUserGroup();
            break;
        case MAKE_RESOURCE_GROUP:
            makeResourceGroup();
            break;
        case MAKE_ROLE:
            makeRole();
            break;
        case SUCCESS:
            printUserDetails();
            break;
        case FAILURE:
            handleFailure();
            break;
        default:
            throw new IllegalStateException(
                    String.format("Unhandled user setup step: %s", this.currentStep));
        }
    }

    /**
     * Figure out if the user exists. We'll only make the user and the associated services
     * (like user group) if the user doesn't exist. This means we assume a cooperative world:
     * for example, if the user was previously created, but the user group doesn't exist, we won't
     * create it.
     */
    private void queryUser() {
        Query userQuery = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND,
                        Utils.buildKind(UserState.class))
                .addFieldClause(UserState.FIELD_NAME_EMAIL, this.userEmail)
                .build();

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(userQuery)
                .build();

        URI queryTaskUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_QUERY_TASKS);
        Operation postQuery = Operation.createPost(queryTaskUri)
                .setBody(queryTask)
                .setReferer(this.referer)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.failureMessage = String.format("Could not query user %s: %s",
                                this.userEmail, ex);
                        this.currentStep = UserCreationStep.FAILURE;
                        setupUser();
                        return;
                    }
                    QueryTask queryResponse = op.getBody(QueryTask.class);
                    if (queryResponse.results.documentLinks != null
                            && queryResponse.results.documentLinks.isEmpty()) {
                        this.currentStep = UserCreationStep.MAKE_USER;
                        setupUser();
                        return;
                    }
                    this.host.log(Level.INFO, "User %s already exists, skipping setup of user",
                            this.userEmail);
                });
        this.host.sendRequest(postQuery);
    }

    /**
     * Make the user service. Once this has been created, documents can be owned
     * by the user because they will provide a documentAuthPrincipalLink that is
     * the selfLink of the user service.
     */
    private void makeUser() {
        UserState user = new UserState();
        user.email = this.userEmail;

        URI userFactoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        Operation postUser = Operation.createPost(userFactoryUri)
                .setBody(user)
                .setReferer(this.referer)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.failureMessage = String.format("Could not make user %s: %s",
                                this.userEmail, ex);
                        this.currentStep = UserCreationStep.FAILURE;
                        setupUser();
                        return;
                    }
                    UserState userResponse = op.getBody(UserState.class);
                    this.userSelfLink = userResponse.documentSelfLink;
                    this.currentStep = UserCreationStep.MAKE_CREDENTIALS;
                    setupUser();
                });
        this.host.sendRequest(postUser);
    }

    /**
     * Make the credentials for the user.
     */
    private void makeCredentials() {
        AuthCredentialsServiceState auth = new AuthCredentialsServiceState();
        auth.userEmail = this.userEmail;
        auth.privateKey = this.userPassword;

        URI credentialFactoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_CREDENTIALS);
        Operation postCreds = Operation.createPost(credentialFactoryUri)
                .setBody(auth)
                .setReferer(this.referer)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.failureMessage = String.format(
                                "Could not make credentials for user %s: %s",
                                this.userEmail, ex);
                        this.currentStep = UserCreationStep.FAILURE;
                        setupUser();
                        return;
                    }
                    this.currentStep = UserCreationStep.MAKE_USER_GROUP;
                    setupUser();
                });
        this.host.sendRequest(postCreds);
    }

    /**
     * Make a user group that contains just this user
     */
    private void makeUserGroup() {
        Query userQuery = Query.Builder.create()
                .setTerm(ServiceDocument.FIELD_NAME_SELF_LINK, this.userSelfLink)
                .build();
        UserGroupState group = new UserGroupState();
        group.query = userQuery;

        URI userGroupFactoryUri = UriUtils.buildUri(this.host,
                ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        Operation postGroup = Operation.createPost(userGroupFactoryUri)
                .setBody(group)
                .setReferer(this.referer)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.failureMessage = String.format(
                                "Could not make user group for user %s: %s",
                                this.userEmail, ex);
                        this.currentStep = UserCreationStep.FAILURE;
                        setupUser();
                        return;
                    }
                    UserGroupState groupResponse = op.getBody(UserGroupState.class);
                    this.userGroupSelfLink = groupResponse.documentSelfLink;
                    this.currentStep = UserCreationStep.MAKE_RESOURCE_GROUP;
                    setupUser();
                });
        this.host.sendRequest(postGroup);
    }

    /**
     * Make a resource group. Depending on the type of the user, we'll make one of two
     * kinds of resource groups.
     *
     * For administrative users, we'll make a resource group that allows access to all
     * services.
     *
     * For non-administrative users, we'll make a resource group that allows access
     * to documents of a single type that are owned by the user
     */
    private void makeResourceGroup() {
        /* A resource group is a query that will return the set of resources in the group
         * We have a simple query that will return all documents, but but we could choose
         * a more selective query if we desired.
         */
        Query resourceQuery;

        if (this.isAdmin) {
            resourceQuery = Query.Builder.create()
                    .setTerm(ServiceDocument.FIELD_NAME_SELF_LINK, UriUtils.URI_WILDCARD_CHAR,
                            QueryTerm.MatchType.WILDCARD)
                    .build();
        } else {
            resourceQuery = Query.Builder.create()
                    .addFieldClause(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK,
                            this.userSelfLink)
                    .addFieldClause(ServiceDocument.FIELD_NAME_KIND, this.documentKind)
                    .build();
        }
        ResourceGroupState group = new ResourceGroupState();
        group.query = resourceQuery;

        URI resourceGroupFactoryUri = UriUtils.buildUri(this.host,
                ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
        Operation postGroup = Operation.createPost(resourceGroupFactoryUri)
                .setBody(group)
                .setReferer(this.referer)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.failureMessage = String.format(
                                "Could not make resource group for user %s: %s",
                                this.userEmail, ex);
                        this.currentStep = UserCreationStep.FAILURE;
                        setupUser();
                        return;
                    }
                    ResourceGroupState groupResponse = op.getBody(ResourceGroupState.class);
                    this.resourceGroupSelfLink = groupResponse.documentSelfLink;
                    this.currentStep = UserCreationStep.MAKE_ROLE;
                    setupUser();
                });
        this.host.sendRequest(postGroup);
    }

    /**
     * Make the role that ties together the user group and resource group. Our policy is to
     * allow all verbose (PUT, POST, etc)
     */
    private void makeRole() {
        Set<Action> verbs = new HashSet<>();
        for (Action action : Action.values()) {
            verbs.add(action);
        }
        RoleState role = new RoleState();
        role.userGroupLink = this.userGroupSelfLink;
        role.resourceGroupLink = this.resourceGroupSelfLink;
        role.verbs = verbs;
        role.policy = Policy.ALLOW;

        URI resourceGroupFactoryUri = UriUtils.buildUri(this.host,
                ServiceUriPaths.CORE_AUTHZ_ROLES);
        Operation postGroup = Operation.createPost(resourceGroupFactoryUri)
                .setBody(role)
                .setReferer(this.referer)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.failureMessage = String.format("Could not make role for user %s: %s",
                                this.userEmail, ex);
                        this.currentStep = UserCreationStep.FAILURE;
                        setupUser();
                        return;
                    }
                    RoleState roleResponse = op.getBody(RoleState.class);
                    this.roleSelfLink = roleResponse.documentSelfLink;
                    this.currentStep = UserCreationStep.SUCCESS;
                    setupUser();
                });
        this.host.sendRequest(postGroup);
    }

    /**
     * When we complete the process, log the full details of the user
     */
    private void printUserDetails() {
        this.host.log(Level.WARNING,
                "Created user %s (%s) with credentials, user group (%s) "
                        + "resource group (%s) and role(%s)",
                this.userEmail,
                this.userSelfLink,
                this.userGroupSelfLink,
                this.resourceGroupSelfLink,
                this.roleSelfLink);
        if (this.completion != null) {
            this.completion.handle(null);
        }
        return;
    }

    private void handleFailure() {
        this.host.log(Level.WARNING, this.failureMessage);
        if (this.completion != null) {
            this.completion.handle(new IllegalStateException(this.failureMessage));
        }
        return;
    }

}