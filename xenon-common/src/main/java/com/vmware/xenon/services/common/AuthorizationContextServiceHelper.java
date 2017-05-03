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

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import com.vmware.xenon.common.AuthUtils;
import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.QueryFilterUtils;
import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryFilter.QueryFilterException;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.ResourceGroupService.ResourceGroupState;
import com.vmware.xenon.services.common.RoleService.Policy;
import com.vmware.xenon.services.common.RoleService.RoleState;
import com.vmware.xenon.services.common.UserGroupService.UserGroupState;
import com.vmware.xenon.services.common.UserService.UserState;

/**
 * Helper class that services responsible for populating and clearing
 * the authorization context for an operation can call to perform some
 * common operations
 *
 */
public class AuthorizationContextServiceHelper {

    private AuthorizationContextServiceHelper() {
    }

    public static class AuthServiceContext {
        // map of service factories to document description.
        public final ConcurrentHashMap<String, ServiceDocumentDescription> factoryDescriptionMap =
                                            new ConcurrentHashMap<>();
        // Auth context service instance
        public final Service authContextService;
        // Function used to filter userGroups the user belongs to.
        public final BiFunction<Operation, Collection<String>, Collection<String>> userGroupsFilter;

        // Function to be invoked on cache clear requests
        public final Consumer<AuthorizationCacheClearRequest> cacheClearRequestHandler;

        public AuthServiceContext(Service authContextService,
                BiFunction<Operation, Collection<String>, Collection<String>> userGroupsFilter,
                Consumer<AuthorizationCacheClearRequest> cacheClearRequestHandler) {
            this.authContextService = authContextService;
            this.userGroupsFilter = userGroupsFilter;
            this.cacheClearRequestHandler = cacheClearRequestHandler;
        }
    }

    /**
     * This private {@code Role} class is an encapsulation of the state associated
     * with a single role. The native role service points to a user group and resource
     * group service by reference. As these states are retrieved one after the other,
     * it is convenient to have a wrapper to encapsulate them.
     */
    private static class Role {
        protected RoleState roleState;
        @SuppressWarnings("unused")
        protected UserGroupState userGroupState;
        protected ResourceGroupState resourceGroupState;

        public void setRoleState(RoleState roleState) {
            this.roleState = roleState;
        }

        public void setUserGroupState(UserGroupState userGroupState) {
            this.userGroupState = userGroupState;
        }

        public void setResourceGroupState(ResourceGroupState resourceGroupState) {
            this.resourceGroupState = resourceGroupState;
        }
    }

    /**
     * Method to check if the input request needs to be processed in-line or scheduled by the service host
     * @param op Input operation
     * @param context auth service context
     * @return True if the request has been completed in-line.
     *         False if the request should be scheduled for execution by the service host.
     */
    public static boolean queueRequest(Operation op, AuthServiceContext context) {
        if (op.getAction() == Action.DELETE && op.getUri().getPath().equals(context.authContextService.getSelfLink())) {
            return false;
        }
        AuthorizationContext ctx = op.getAuthorizationContext();
        if (ctx == null) {
            op.fail(new IllegalArgumentException("no authorization context"));
            return true;
        }

        Claims claims = ctx.getClaims();
        if (claims == null) {
            op.fail(new IllegalArgumentException("no claims"));
            return true;
        }

        String subject = claims.getSubject();
        if (subject == null) {
            op.fail(new IllegalArgumentException("no subject"));
            return true;
        }

        // handle a cache clear request
        if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CLEAR_AUTH_CACHE)) {
            return handleCacheClearRequest(op, subject, context);
        }

        // Allow unconditionally if this is the system user
        if (subject.equals(SystemUserService.SELF_LINK)) {
            op.complete();
            return true;
        }

        // Check whether or not the operation already has a processed context.
        if (ctx.getResourceQueryFilter(op.getAction()) != null) {
            op.complete();
            return true;
        }

        // Needs to be scheduled by the service host, as we'll have to retrieve the
        // user, find out which roles apply, and verify whether the authorization
        // context allows access to the service targeted by the operation.
        return false;
    }

    private static boolean handleCacheClearRequest(Operation op, String subject,
            AuthServiceContext context) {
        AuthorizationCacheClearRequest requestBody = op.getBody(AuthorizationCacheClearRequest.class);
        if (!AuthorizationCacheClearRequest.KIND.equals(requestBody.kind)) {
            op.fail(new IllegalArgumentException("invalid request body type"));
            return true;
        }
        if (requestBody.subjectLink == null) {
            op.fail(new IllegalArgumentException("no subjectLink"));
            return true;
        }
        if (!subject.equals(SystemUserService.SELF_LINK)) {
            op.fail(Operation.STATUS_CODE_FORBIDDEN);
            return true;
        }
        if (context.cacheClearRequestHandler != null) {
            context.cacheClearRequestHandler.accept(requestBody);
        }
        context.authContextService.getHost().clearAuthorizationContext(context.authContextService, requestBody.subjectLink);
        op.complete();
        return true;
    }

    /**
     * Method to populate an input Opearation's auth context. This method invokes queries
     * against the various authz services and populates the auth context on the operation.
     * The method invocation does not have any other side effect outside of the scope of
     * the Operation
     * @param op Input operation
     * @param context auth service context
     */
    public static void populateAuthContext(Operation op, AuthServiceContext context) {
        AuthorizationContext ctx = op.getAuthorizationContext();
        if (ctx == null) {
            op.fail(new IllegalArgumentException("no authorization context"));
            return;
        }

        Claims claims = ctx.getClaims();
        if (claims == null) {
            op.fail(new IllegalArgumentException("no claims"));
            return;
        }
        URI getSubjectUri = AuthUtils.buildUserUriFromClaims(context.authContextService.getHost(), claims);
        Operation get = Operation.createGet(getSubjectUri)
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    ServiceDocument userState = extractServiceState(o, context.authContextService.getHost());
                    // If the native user state could not be extracted, we are sure no roles
                    // will apply and we can populate the authorization context.
                    if (userState == null) {
                        populateAuthorizationContext(op, ctx, claims, null, context);
                        return;
                    }

                    loadUserGroups(op, ctx, claims, userState, context);
                });

        context.authContextService.setAuthorizationContext(get, context.authContextService.getSystemAuthorizationContext());
        context.authContextService.sendRequest(get);
    }

    private static ServiceDocument extractServiceState(Operation getOp, ServiceHost serviceHost) {
        ServiceDocument userState = QueryFilterUtils.getServiceState(getOp, serviceHost);
        if (userState == null) {
            Object rawBody = getOp.getBodyRaw();
            Class<?> serviceTypeClass = null;
            if (rawBody instanceof String) {
                String kind = Utils.getJsonMapValue(rawBody, ServiceDocument.FIELD_NAME_KIND,
                        String.class);
                serviceTypeClass = Utils.getTypeFromKind(kind);
            } else {
                serviceTypeClass = rawBody.getClass();
            }
            if (serviceTypeClass != null) {
                userState = (ServiceDocument)Utils.fromJson(rawBody, serviceTypeClass);
            }
        }
        return userState;
    }

    private static boolean loadUserGroupsFromUserState(Operation op, AuthorizationContext ctx, Claims claims,
            ServiceDocument userServiceDocument, AuthServiceContext context) {
        Field groupLinksField = ReflectionUtils.getFieldIfExists(userServiceDocument.getClass(),
                UserState.FIELD_NAME_USER_GROUP_LINKS);

        if (groupLinksField == null) {
            return false;
        }

        Object fieldValue;
        try {
            fieldValue = groupLinksField.get(userServiceDocument);
        } catch (IllegalArgumentException | IllegalAccessException e1) {
            return false;
        }
        if (!(fieldValue instanceof Collection<?>)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Collection<String> userGroupLinks = (Collection<String>) fieldValue;
        if (userGroupLinks.isEmpty()) {
            return false;
        }
        JoinedCompletionHandler handler = (ops, failures) -> {
            Collection<Operation> userGroupOps = null;
            if (failures != null && !failures.isEmpty()) {
                userGroupOps = new HashSet<>(ops.values());
                for (Operation groupOp : ops.values()) {
                    if (groupOp.getStatusCode() == Operation.STATUS_CODE_OK) {
                        continue;
                    } else if (groupOp.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                        userGroupOps.remove(groupOp);
                        continue;
                    }
                    op.fail(failures.values().iterator().next());
                    return;
                }
            } else {
                userGroupOps = ops.values();
            }

            // If no user groups apply to this user, we are sure no roles
            // will apply and we can populate the authorization context with a null context.
            if (userGroupOps.isEmpty()) {
                populateAuthorizationContext(op, ctx, claims, null, context);
                return;
            }
            try {
                Collection<UserGroupState> userGroupStates = new HashSet<>();
                for (Operation userGroupOp : userGroupOps) {
                    UserGroupState userGroupState = userGroupOp.getBody(UserGroupState.class);
                    userGroupStates.add(userGroupState);
                }
                loadRoles(op, ctx, claims, userGroupStates, context);
            } catch (Throwable e) {
                op.fail(e);
                return;
            }
        };
        Collection<String> filteredUserGroupLinks = null;
        // if there is a userGroupFilter specified, invoke it to filter out the list
        // of user groups that do not apply
        if (context.userGroupsFilter != null) {
            filteredUserGroupLinks = context.userGroupsFilter.apply(op, userGroupLinks);
            if (filteredUserGroupLinks == null || filteredUserGroupLinks.isEmpty()) {
                populateAuthorizationContext(op, ctx, claims, null, context);
                return true;
            }
        } else {
            filteredUserGroupLinks = userGroupLinks;
        }
        Collection<Operation> gets = new HashSet<>();
        for (String userGroupLink : filteredUserGroupLinks) {
            Operation get = Operation.createGet(AuthUtils
                    .buildAuthProviderHostUri(context.authContextService.getHost(), userGroupLink)).setReferer(context.authContextService.getUri());
            context.authContextService.setAuthorizationContext(get, context.authContextService.getSystemAuthorizationContext());
            gets.add(get);
        }
        OperationJoin join = OperationJoin.create(gets);
        join.setCompletion(handler);
        join.sendWith(context.authContextService.getHost());
        return true;
    }

    private static void loadUserGroups(Operation op, AuthorizationContext ctx, Claims claims, ServiceDocument userServiceDocument,
            AuthServiceContext context) {
        // if the user service derived from UserService and it has the userGroupLinks
        // field populated, use that to compute the list of groups that apply to the user
        if (loadUserGroupsFromUserState(op, ctx, claims, userServiceDocument, context)) {
            return;
        }

        URI getUserGroupsUri = AuthUtils.buildAuthProviderHostUri(context.authContextService.getHost(),
                ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        getUserGroupsUri = UriUtils.buildExpandLinksQueryUri(getUserGroupsUri);
        Operation get = Operation.createGet(getUserGroupsUri)
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    ServiceDocumentQueryResult result = o
                            .getBody(ServiceDocumentQueryResult.class);
                    Collection<UserGroupState> userGroupStates = new ArrayList<>();
                    for (Object doc : result.documents.values()) {
                        UserGroupState userGroupState = Utils.fromJson(doc,
                                UserGroupState.class);
                        try {
                            QueryFilter f = QueryFilter.create(userGroupState.query);
                            ServiceDocumentDescription sdd = getServiceDesc(userServiceDocument, context);
                            if (QueryFilterUtils.evaluate(f, userServiceDocument, sdd)) {
                                userGroupStates.add(userGroupState);
                            }
                        } catch (QueryFilterException qfe) {
                            //service.logWarning("Error creating query filter: %s", qfe.toString());
                            op.fail(qfe);
                            return;
                        }
                    }

                    // If no user groups apply to this user, we are sure no roles
                    // will apply and we can populate the authorization context.
                    if (userGroupStates.isEmpty()) {
                        // TODO(DCP-782): Add negative cache
                        populateAuthorizationContext(op, ctx, claims, null, context);
                        return;
                    }
                    if (context.userGroupsFilter != null) {
                        Collection<String> userGroupLinks = new ArrayList<>();
                        for (UserGroupState groupState : userGroupStates) {
                            userGroupLinks.add(groupState.documentSelfLink);
                        }
                        Collection<String> filteredUserGroupLinks = context.userGroupsFilter.apply(op, userGroupLinks);
                        if (filteredUserGroupLinks == null || filteredUserGroupLinks.isEmpty()) {
                            populateAuthorizationContext(op, ctx, claims, null, context);
                            return;
                        }
                        Collection<UserGroupState> filteredUserGroupStates = new ArrayList<>();
                        if (filteredUserGroupLinks != null) {
                            for (String filterUserGroupLink : filteredUserGroupLinks) {
                                filteredUserGroupStates.add(Utils.fromJson(result.documents.get(filterUserGroupLink),
                                        UserGroupState.class));
                            }
                        }
                        userGroupStates = filteredUserGroupStates;
                    }
                    loadRoles(op, ctx, claims, userGroupStates, context);
                });

        context.authContextService.setAuthorizationContext(get, context.authContextService.getSystemAuthorizationContext());
        context.authContextService.sendRequest(get);
    }

    private static ServiceDocumentDescription getServiceDesc(ServiceDocument userServiceDocument,
            AuthServiceContext context) {
        String parentLink = UriUtils.getParentPath(userServiceDocument.documentSelfLink);
        if (parentLink == null) {
            return null;
        }
        ServiceDocumentDescription sdd =
                context.factoryDescriptionMap.computeIfAbsent(parentLink, (val) -> {
                    return ServiceDocumentDescription.Builder.create()
                            .buildDescription(userServiceDocument.getClass());
                });
        return sdd;
    }

    private static void loadRoles(Operation op, AuthorizationContext ctx, Claims claims,
            Collection<UserGroupState> userGroupStates,
            AuthServiceContext context) {
        // Map user group self-link to user group state so we can lookup in O(1) later
        Map<String, UserGroupState> userGroupStateMap = new HashMap<>();
        for (UserGroupState userGroupState : userGroupStates) {
            userGroupStateMap.put(userGroupState.documentSelfLink, userGroupState);
        }

        // Create query for roles referring any of the specified user groups
        Query kindClause = new Query();
        kindClause.occurance = Occurance.MUST_OCCUR;
        kindClause.setTermPropertyName(ServiceDocument.FIELD_NAME_KIND);
        kindClause.setTermMatchType(MatchType.TERM);
        kindClause.setTermMatchValue(RoleState.KIND);

        Query selfLinkClause = new Query();
        selfLinkClause.occurance = Occurance.MUST_OCCUR;
        if (userGroupStates.size() == 1) {
            selfLinkClause.setTermPropertyName(RoleState.FIELD_NAME_USER_GROUP_LINK);
            selfLinkClause.setTermMatchType(MatchType.TERM);
            selfLinkClause.setTermMatchValue(userGroupStates.iterator().next().documentSelfLink);
        } else {
            for (UserGroupState userGroupState : userGroupStates) {
                Query clause = new Query();
                clause.occurance = Occurance.SHOULD_OCCUR;
                clause.setTermPropertyName(RoleState.FIELD_NAME_USER_GROUP_LINK);
                clause.setTermMatchType(MatchType.TERM);
                clause.setTermMatchValue(userGroupState.documentSelfLink);
                selfLinkClause.addBooleanClause(clause);
            }
        }

        Query query = new Query();
        query.addBooleanClause(kindClause);
        query.addBooleanClause(selfLinkClause);

        QueryTask queryTask = new QueryTask();
        queryTask.querySpec = new QuerySpecification();
        queryTask.querySpec.query = query;
        queryTask.querySpec.options =
                EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
        queryTask.setDirect(true);

        URI postQueryUri = AuthUtils.buildAuthProviderHostUri(context.authContextService.getHost(),
                ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        Operation post = Operation.createPost(postQueryUri)
                .setBody(queryTask)
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }

                    QueryTask queryTaskResult = o.getBody(QueryTask.class);
                    ServiceDocumentQueryResult result = queryTaskResult.results;
                    if (result.documents == null || result.documents.isEmpty()) {
                        populateAuthorizationContext(op, ctx, claims, null, context);
                        return;
                    }

                    Collection<Role> roles = new LinkedList<>();
                    for (Object doc : result.documents.values()) {
                        RoleState roleState = Utils.fromJson(doc, RoleState.class);
                        Role role = new Role();
                        role.setRoleState(roleState);
                        role.setUserGroupState(userGroupStateMap.get(roleState.userGroupLink));
                        roles.add(role);
                    }

                    loadResourceGroups(op, ctx, claims, roles, context);
                });

        context.authContextService.setAuthorizationContext(post, context.authContextService.getSystemAuthorizationContext());
        context.authContextService.sendRequest(post);
    }

    private static void loadResourceGroups(Operation op, AuthorizationContext ctx, Claims claims, Collection<Role> roles,
            AuthServiceContext context) {
        // Map resource group self-link to role so we can lookup in O(1) later
        Map<String, Collection<Role>> rolesByResourceGroup = new HashMap<>();
        for (Role role : roles) {
            String resourceGroupLink = role.roleState.resourceGroupLink;
            Collection<Role> byResourceGroup = rolesByResourceGroup.get(resourceGroupLink);
            if (byResourceGroup == null) {
                byResourceGroup = new LinkedList<>();
                rolesByResourceGroup.put(resourceGroupLink, byResourceGroup);
            }

            byResourceGroup.add(role);
        }

        JoinedCompletionHandler handler = (ops, failures) -> {
            Collection<Operation> resourceGroupOps = null;
            if (failures != null && !failures.isEmpty()) {
                resourceGroupOps = new HashSet<>(ops.values());
                for (Operation getOp : ops.values()) {
                    if (getOp.getStatusCode() == Operation.STATUS_CODE_OK) {
                        continue;
                    } else if (getOp.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                        // ignore ops resulting in a 404 response code
                        resourceGroupOps.remove(getOp);
                        continue;
                    }
                    op.fail(failures.values().iterator().next());
                    return;
                }
            } else {
                resourceGroupOps = ops.values();
            }

            try {
                // Add every resource group state to every role that references it
                for (Operation resourceOp : resourceGroupOps) {
                    ResourceGroupState resourceGroupState = resourceOp.getBody(ResourceGroupState.class);
                    Collection<Role> rolesForResourceGroup = rolesByResourceGroup
                            .get(resourceGroupState.documentSelfLink);
                    if (rolesForResourceGroup != null) {
                        for (Role role : rolesForResourceGroup) {
                            role.setResourceGroupState(resourceGroupState);
                        }
                    }
                }
                populateAuthorizationContext(op, ctx, claims, roles, context);
            } catch (Throwable e) {
                op.fail(e);
                return;
            }
        };

        // Fire off GET for every resource group
        Collection<Operation> gets = new LinkedList<>();
        for (String resourceGroupLink : rolesByResourceGroup.keySet()) {
            Operation get = Operation.createGet(AuthUtils.buildAuthProviderHostUri(context.authContextService.getHost(),
                                resourceGroupLink)).setReferer(context.authContextService.getUri());
            context.authContextService.setAuthorizationContext(get, context.authContextService.getSystemAuthorizationContext());
            gets.add(get);
        }

        OperationJoin join = OperationJoin.create(gets);
        join.setCompletion(handler);
        join.sendWith(context.authContextService.getHost());
    }

    private static void populateAuthorizationContext(Operation op, AuthorizationContext ctx, Claims claims,
            Collection<Role> roles, AuthServiceContext context) {
        if (roles == null) {
            roles = Collections.emptyList();
        }
        try {

            AuthorizationContext.Builder builder = AuthorizationContext.Builder.create();
            builder.setClaims(ctx.getClaims());
            builder.setToken(ctx.getToken());

            if (!roles.isEmpty()) {
                Map<Action, Collection<Role>> roleListByAction = new HashMap<>(
                        Action.values().length);
                for (Role role : roles) {
                    for (Action action : role.roleState.verbs) {
                        Collection<Role> roleList = roleListByAction.get(action);
                        if (roleList == null) {
                            roleList = new LinkedList<>();
                            roleListByAction.put(action, roleList);
                        }

                        roleList.add(role);
                    }
                }

                Map<Action, QueryFilter> queryFilterByAction = new HashMap<>(
                        Action.values().length);
                Map<Action, Query> queryByAction = new HashMap<>(Action.values().length);
                for (Map.Entry<Action, Collection<Role>> entry : roleListByAction.entrySet()) {
                    Query q = new Query();
                    q.occurance = Occurance.MUST_OCCUR;
                    for (Role role : entry.getValue()) {
                        if (role.resourceGroupState == null) {
                            continue;
                        }
                        Query resourceGroupQuery = role.resourceGroupState.query;
                        if (role.roleState.policy == Policy.ALLOW) {
                            resourceGroupQuery.occurance = Occurance.SHOULD_OCCUR;
                        } else {
                            resourceGroupQuery.occurance = Occurance.MUST_NOT_OCCUR;
                        }
                        q.addBooleanClause(resourceGroupQuery);
                    }

                    if (q.booleanClauses != null) {
                        try {
                            queryFilterByAction.put(entry.getKey(), QueryFilter.create(q));
                            queryByAction.put(entry.getKey(), q);
                        } catch (QueryFilterException qfe) {
                            op.fail(qfe);
                            return;
                        }
                    }
                }

                builder.setResourceQueryMap(queryByAction);
                builder.setResourceQueryFilterMap(queryFilterByAction);
            }

            AuthorizationContext newContext = builder.getResult();
            // set the populated auth context on the operation
            context.authContextService.setAuthorizationContext(op, newContext);
            op.complete();
        } catch (Throwable e) {
            op.fail(e);
        }
    }
}
