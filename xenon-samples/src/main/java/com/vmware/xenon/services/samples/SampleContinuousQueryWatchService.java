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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.ServiceUriPaths;


/**
 * Load balanced continuous query sample
 *
 * A StatefulService that is responsible for creating the continuous query and a subscription to
 * that continuous query. The Continuous Query service will be created on the local index.
 * The StatefulService overrides handleNodeGroupMaintenance() to stop the Continuous Query & subscription
 * when the node is no longer the owner.
 */
public class SampleContinuousQueryWatchService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES + "/watches";

    /**
     * A known self-link for query task service, so that we can refer it easily for start and stop.
     */
    public static final String QUERY_SELF_LINK = ServiceUriPaths.CORE_LOCAL_QUERY_TASKS + "/sample-continuous-query";

    public SampleContinuousQueryWatchService() {
        super(State.class);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    /**
     * Create the service and validate the state.
     */
    @Override
    public void handleStart(Operation start) {
        try {
            validateState(start);
            start.complete();
        } catch (Exception e) {
            start.fail(e);
        }
    }

    /**
     * Updates the service state with patched state. Service keeps track of notification counter in its state which
     * is incremented by the number of new notifications.
     */
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

    /**
     * This is overridable method in stateful services that is invoked by the Xenon runtime when there is change
     * in the ownership of the state of the service. We override this method to check if we are owner or not
     * and based on that create continuous query task if we are owner, and delete running continuous query task
     * service and subscription if we are not the owner anymore.
     */
    @Override
    public void handleNodeGroupMaintenance(Operation op) {
        // Create continuous queries and subscriptions in case of change in node group topology.
        if (hasOption(ServiceOption.DOCUMENT_OWNER)) {
            createAndSubscribeToContinuousQuery(op);
        } else {
            deleteSubscriptionAndContinuousQuery(op);
        }
    }

    /**
     * On each notification, this method is called for processing the updates from the notification.
     */
    private void processResults(Operation op) {
        QueryTask body = op.getBody(QueryTask.class);

        if (body.results == null || body.results.documentLinks.isEmpty()) {
            return;
        }

        State newState = new State();
        newState.notificationsCounter = body.results.documents.size();
        // patch the state with the number of new notifications received
        Operation.createPatch(this, getSelfLink())
                .setBody(newState)
                .sendWith(this);
    }

    /**
     * Called by handleNodeGroupMaintenance() to create the continuous query and subscription when
     * current node is owner of the service state.
     */
    private void createAndSubscribeToContinuousQuery(Operation op) {
        getStateAndApply((s) -> {
            QueryTask queryTask = QueryTask.create(s.querySpec);
            queryTask.documentExpirationTimeMicros = Long.MAX_VALUE;

            // Creating continuous query task service with known self-link. We will use this same
            // self-link when deleting this query service when current node is not the ownership anymore.
            queryTask.documentSelfLink = QUERY_SELF_LINK;
            Operation post = Operation.createPost(getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                    .setBody(queryTask)
                    .setReferer(getHost().getUri());

            // On successful creation of continuous query task service, create subscription to that query service.
            getHost().sendWithDeferredResult(post)
                    .thenAccept((state) -> subscribeToContinuousQuery())
                    .whenCompleteNotify(op);
        });
    }

    /**
     * Create subscription service.
     *
     * It has two parts, subscription service on local host to listen for the notifications,
     * and subscribing to query task service with our subscription service link which will be used
     * by query task service to call our subscription service. Both of these tasks are done by
     * startSubscriptionService() method of ServiceHost.
     */
    private void subscribeToContinuousQuery() {
        Operation post = Operation
                .createPost(getHost(), QUERY_SELF_LINK)
                .setReferer(getHost().getUri());

        // Create subscription service with processResults as callback to process the results.
        URI subscriptionUri = getHost().startSubscriptionService(post, this::processResults);
        updateSubscriptionLink(subscriptionUri);
    }

    /**
     * Update subscription link in service state using send PATCH request to self.
     */
    private void updateSubscriptionLink(URI subscriptionLink) {
        State state = new State();
        state.subscriptionLink = subscriptionLink == null ? "" : subscriptionLink.toString();
        Operation.createPatch(getUri()).setBody(state).sendWith(this);
    }

    /**
     * Called by handleNodeGroupMaintenance() to delete the continuous query and subscription when
     * current node is not the owner of the service state.
     */
    private void deleteSubscriptionAndContinuousQuery(Operation op) {
        Operation unsubscribeOperation = Operation.createPost(getHost(), QUERY_SELF_LINK)
                .setReferer(getHost().getUri())
                .setCompletion((o, e) -> {
                    updateSubscriptionLink(null);
                    deleteContinuousQuery();
                });

        getStateAndApply(state -> getHost().stopSubscriptionService(unsubscribeOperation,
                UriUtils.buildUri(state.subscriptionLink)));
    }

    /**
     * Stop the continuous query task service using its known self-link.
     */
    private void deleteContinuousQuery() {
        Operation.createDelete(getHost(), QUERY_SELF_LINK).sendWith(this);
    }

    /**
     * Helper method to get current state of the service and apply action on that state.
     */
    private void getStateAndApply(Consumer<? super State> action) {
        Operation get = Operation.createGet(this, getSelfLink());

        getHost().sendWithDeferredResult(get, State.class)
                .thenAccept(action)
                .whenCompleteNotify(get);
    }

    private void validateState(Operation op) {
        if (!op.hasBody()) {
            throw new IllegalArgumentException("attempt to initialize service with an empty state");
        }

        State state = op.getBody(State.class);
        if (!state.querySpec.options.contains(QuerySpecification.QueryOption.CONTINUOUS)) {
            throw new IllegalArgumentException("QueryTask should have QueryOption.CONTINUOUS option");
        }
    }

    public static class State extends ServiceDocument {

        // must call Utils.mergeWithState to leverage this
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String subscriptionLink;

        public QuerySpecification querySpec;

        public int notificationsCounter;
    }
}