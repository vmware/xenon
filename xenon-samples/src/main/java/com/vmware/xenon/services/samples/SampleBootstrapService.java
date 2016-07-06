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

import java.net.URI;
import java.util.logging.Level;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UserService.UserState;

/**
 * Demonstrate one-time node group setup(bootstrap).
 *
 * This service is guaranteed to be performed only once within entire node group, in a consistent
 * safe way. Durable for restarting the owner node or even complete shutdown and restarting of all
 * nodes.
 */
public class SampleBootstrapService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/bootstrap";
    private static final String ADMIN_EMAIL = "admin@vmware.com";

    public static FactoryService createFactory() {
        return FactoryService.create(SampleBootstrapService.class);
    }

    /**
     * POST to a fixed self link that starts an initialization task.
     *
     * If the task is already in progress, or it has completed, the POST will be ignored.
     *
     * This callback can be registered to
     *   {@link ServiceHost#registerForServiceAvailability(CompletionHandler, boolean, String...)}.
     *
     *
     * The call will happen in every node start, but service options and implementation guarantees
     * actual logic will be performed only once within entire node group.
     *
     * Example how to register this callback in ServiceHost:
     * <pre>
     * {@code
     *   @Override
     *   public ServiceHost start() throws Throwable {
     *     super.start();
     *     // starts other services
     *
     *     registerForServiceAvailability(SampleBootstrapService.startTask(h), true,
     *                                      SampleBootstrapService.FACTORY_LINK);
     *     // ...
     *   }
     * }
     * </pre>
     *
     * @param host a service host
     */
    public static CompletionHandler startTask(ServiceHost host) {
        return (o, e) -> {
            if (e != null) {
                host.log(Level.SEVERE, Utils.toString(e));
                return;
            }

            // create service with fixed link
            // POST will be issued multiple times but will be converted to PUT after the first one.
            ServiceDocument doc = new ServiceDocument();
            doc.documentSelfLink = "preparation-task";
            Operation.createPost(host, SampleBootstrapService.FACTORY_LINK)
                    .setBody(doc)
                    .setReferer(host.getUri())
                    .setCompletion((oo, ee) -> {
                        if (ee != null) {
                            host.log(Level.SEVERE, Utils.toString(ee));
                            return;
                        }
                        host.log(Level.INFO, "preparation-task triggered");
                    })
                    .sendWith(host);

        };
    }

    public SampleBootstrapService() {
        super(ServiceDocument.class);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation post) {

        if (!ServiceHost.isServiceCreate(post)) {
            // do not perform bootstrap logic when the post is NOT from direct client, eg: node restart
            post.complete();
            return;
        }

        createAdminIfNotExist(getHost(), post);
    }

    @Override
    public void handlePut(Operation put) {

        if (put.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
            // converted PUT due to IDEMPOTENT_POST option
            logInfo("Task has already started. Ignoring converted PUT.");
            put.complete();
            return;
        }

        // normal PUT is not supported
        put.fail(Operation.STATUS_CODE_BAD_METHOD);
    }

    private void createAdminIfNotExist(ServiceHost host, Operation post) {

        // Simple version of AuthorizationSetupHelper.
        // Prefer using TaskService for complex bootstrap logic.

        Query userQuery = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND, Utils.buildKind(UserState.class))
                .addFieldClause(UserState.FIELD_NAME_EMAIL, ADMIN_EMAIL)
                .build();

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(userQuery)
                .build();

        URI queryTaskUri = UriUtils.buildUri(host, ServiceUriPaths.CORE_QUERY_TASKS);

        CompletionHandler userCreationCallback = (op, ex) -> {
            if (ex != null) {
                String msg = String.format("Could not make user %s: %s", ADMIN_EMAIL, ex);
                post.fail(new IllegalStateException(msg, ex));
                return;
            }

            host.log(Level.INFO, "User %s has been created", ADMIN_EMAIL);
            post.complete();
        };

        CompletionHandler queryUserCallback = (op, ex) -> {
            if (ex != null) {
                String msg = String.format("Could not query user %s: %s", ADMIN_EMAIL, ex);
                post.fail(new IllegalStateException(msg, ex));
                return;
            }

            QueryTask queryResponse = op.getBody(QueryTask.class);
            boolean userExists = queryResponse.results.documentLinks != null
                    && !queryResponse.results.documentLinks.isEmpty();

            if (userExists) {
                host.log(Level.INFO, "User %s already exists, skipping setup of user", ADMIN_EMAIL);
                post.complete();
                return;
            }

            // create user
            UserState user = new UserState();
            user.email = ADMIN_EMAIL;
            user.documentSelfLink = ADMIN_EMAIL;

            URI userFactoryUri = UriUtils.buildUri(host, ServiceUriPaths.CORE_AUTHZ_USERS);
            Operation.createPost(userFactoryUri)
                    .setBody(user)
                    .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                            Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL)
                    .setCompletion(userCreationCallback)
                    .sendWith(this);
        };

        Operation.createPost(queryTaskUri)
                .setBody(queryTask)
                .setCompletion(queryUserCallback)
                .sendWith(this);

    }

}
