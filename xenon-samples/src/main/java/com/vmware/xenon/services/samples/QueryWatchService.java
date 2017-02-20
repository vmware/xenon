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

import com.vmware.xenon.common.*;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

import java.net.URI;
import java.util.function.Consumer;
import java.util.logging.Level;

import static com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;

/**
 * Provides a partial implementation of service that watches for any notifications
 * from continuous query service.
 *
 * Extended class (see SampleQueryWatchService) would provide the continuous query
 * to watch, and processResult method to process the results.
 */

public abstract class QueryWatchService extends StatefulService implements QueryWatchInterface {

    // FACTORY_LINK is a special variable that Xenon looks for to know where to host your REST API
    public static final String FACTORY_LINK = "/watches";

    public QueryWatchService() {
        super(State.class);

        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    public String getQueryTaskLink() {
        return UriUtils.buildUriPath(ServiceUriPaths.CORE_LOCAL_QUERY_TASKS, "query-" + getId());
    }

    /**
     * Creates a CompletionHandler that starts a single QueryWatchService.
     * <p/>
     * Call ServiceHost.registerForServiceAvailability with the result of this
     * function in order to start the singleton instance of QueryWatchService.
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
     * Creates a POST request that creates a singleton QueryWatchService.
     * <p/>
     * Does not send the request.
     */
    public static Operation createPostToStartSingleton(ServiceHost host, String id) {
        QueryWatchService.State newState = new QueryWatchService.State();
        newState.documentSelfLink = UriUtils.buildUriPath(FACTORY_LINK, id);

        Operation postOperation = Operation
                .createPost(host, QueryWatchService.FACTORY_LINK)
                .setBody(newState)
                .setReferer(host.getUri());

        return postOperation;
    }

    @Override
    public void handlePatch(Operation patch) {
        State state = getState(patch);
        State patchBody = getBody(patch);

        /*
          Looks for differences between the 'body' object (the new version) and the 'state' object (old version).
          the object already stored in Xenon (the state). If a field differs and is non-null in the body,
          and if the AUTO_MERGE_IF_NOT_NULL property is set on that field, then the field is updated with the new
          value found in the body. The merged version is returned.
         */

        Utils.mergeWithState(getStateDescription(), state, patchBody);
        state.notificationsCounter += patchBody.notificationsCounter;
        /*
         * For a PATCH, the default body returned to caller will just have the properties that actually changed plus all the
         * system properties. This may not be intuitive. By calling setBody with the new state, we'll return all
         * the properties that have been set (in this patch or previously).
         * setBody(null) may be the most optimized solution which will return an empty document to the user,
         * although it can be confusing when first exploring Xenon.
         */
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