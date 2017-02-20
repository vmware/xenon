/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

import java.util.function.Consumer;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;


/**
 * Provides a partial implementation of service that watches for any notifications
 * from continuous query service.
 *
 * Extended class would provide the continuous query
 * to watch, and processResult method to process the results.
 */

public abstract class ContinuousQueryWatchService extends StatefulService implements ContinuousQueryWatchInterface {

    // FACTORY_LINK is a special variable that Xenon looks for to know where to host your REST API
    public static final String FACTORY_LINK = "/watches";

    public ContinuousQueryWatchService() {
        super(State.class);

        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    public String getQueryTaskLink() {
        return UriUtils.buildUriPath(ServiceUriPaths.CORE_LOCAL_QUERY_TASKS, "query-" + getId());
    }

    /**
     * Call ServiceHost.registerForServiceAvailability with the result of this
     * function in order to start the singleton instance of this service.
     */
    public static Operation.CompletionHandler startSingletonService(ServiceHost host, String id) {
        return (o, e) -> {
            if (e != null) {
                host.log(Level.SEVERE, Utils.toString(e));
                return;
            }

            Operation postOperation = createPostToStartSingleton(host, id);
            host.sendRequest(postOperation);
        };
    }

    /**
     * Creates a POST request that creates a singleton instance of this service.
     * Does not send the request.
     */
    public static Operation createPostToStartSingleton(ServiceHost host, String id) {
        ContinuousQueryWatchService.State newState = new ContinuousQueryWatchService.State();
        newState.documentSelfLink = UriUtils.buildUriPath(FACTORY_LINK, id);

        Operation postOperation = Operation
                .createPost(host, ContinuousQueryWatchService.FACTORY_LINK)
                .setBody(newState)
                .setReferer(host.getUri());

        return postOperation;
    }

    @Override
    public void handlePatch(Operation patch) {
        State state = getState(patch);
        State patchBody = getBody(patch);

        if (patchBody.subscriptionLink != null) {
            state.subscriptionLink = patchBody.subscriptionLink;
        }

        state.notificationsCounter += patchBody.notificationsCounter;

        patch.setBody(state);
        patch.complete();
    }

    public void processAllResults(Operation op) {
        processBaseResults(op);
        processResults(op);
    }

    public void processBaseResults(Operation op) {
        QueryTask body = op.getBody(QueryTask.class);

        if (body.results == null || body.results.documentLinks.isEmpty()) {
            return;
        }

        State newState = new State();
        newState.notificationsCounter = body.results.documents.size();
        // patch the state with the number of new notifications received
        Operation.createPatch(getUri())
                .setBody(newState)
                .sendWith(this);
    }

    @Override
    public void handleNodeGroupMaintenance(Operation op) {
        // Create continuous queries and subscriptions in case of change in node group topology.
        if (hasOption(ServiceOption.DOCUMENT_OWNER)) {
            createAndSubscribeToContinuousQuery(op);
        } else {
            deleteSubscriptionAndContinuousQuery(op);
        }
    }

    public void createAndSubscribeToContinuousQuery(Operation op) {
        QueryTask queryTask = createContinuousQuery();

        if (!queryTask.querySpec.options.contains(QueryTask.QuerySpecification.QueryOption.CONTINUOUS)) {
            throw new IllegalArgumentException("QueryTask should have QueryOption.CONTINUOUS option");
        }

        queryTask.documentSelfLink = getQueryTaskLink();
        Operation post = Operation.createPost(getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                .setBody(queryTask)
                .setReferer(getHost().getUri());

        getHost().sendWithDeferredResult(post)
                .thenAccept((state) -> subscribeToContinuousQuery())
                .whenCompleteNotify(op);
    }

    public void subscribeToContinuousQuery() {
        Operation post = Operation
                .createPost(UriUtils.buildUri(getHost(), getQueryTaskLink()))
                .setReferer(getHost().getUri());

        URI subscriptionUri = getHost().startSubscriptionService(post, this::processAllResults);
        updateSubscriptionLink(subscriptionUri);
    }

    private void updateSubscriptionLink(URI subscriptionLink) {
        State state = new State();
        state.subscriptionLink = subscriptionLink == null ? "" : subscriptionLink.toString();
        Operation patch = Operation.createPatch(getUri())
                .setBody(state)
                .setReferer(getUri());
        this.sendRequest(patch);
    }

    private void deleteSubscriptionAndContinuousQuery(Operation op) {
        Operation unsubscribeOperation = Operation.createPost(UriUtils.buildUri(getHost(), getQueryTaskLink()))
                .setReferer(getUri())
                .setCompletion((o, e) -> {
                    updateSubscriptionLink(null);
                    deleteContinuousQuery();
                });

        getStateAndApply(state -> getHost().stopSubscriptionService(unsubscribeOperation,
                UriUtils.buildUri(state.subscriptionLink)));
    }

    private void deleteContinuousQuery() {
        getHost().sendRequest(Operation
                .createDelete(UriUtils.buildUri(getHost(), getQueryTaskLink()))
                .setReferer(getUri()));
    }

    private void getStateAndApply(Consumer<? super State> action) {
        Operation get = Operation
                .createGet(this, this.getSelfLink())
                .setReferer(getUri());

        getHost().sendWithDeferredResult(get, State.class)
                .thenAccept(action)
                .whenCompleteNotify(get);
    }

    public static class State extends ServiceDocument {

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String subscriptionLink;

        public int notificationsCounter;
    }
}