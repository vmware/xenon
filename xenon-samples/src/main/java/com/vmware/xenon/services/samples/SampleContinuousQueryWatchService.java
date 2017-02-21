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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;


/**
 * Provides an implementation of service that watches for any notifications
 * from continuous query service.
 */

public class SampleContinuousQueryWatchService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES + "/watches";

    public static final String QUERY_SELF_LINK = ServiceUriPaths.CORE_LOCAL_QUERY_TASKS + "/sample-continuous-query";

    public static final String SUBSCRIPTION_SELF_LINK = ServiceUriPaths.CORE_CALLBACKS + "/sample-cq-subscription";

    public SampleContinuousQueryWatchService() {
        super(State.class);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    public QueryTask createContinuousQuery() {
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(SamplePreviousEchoService.EchoServiceState.class)
                .build();

        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT)
                .addOption(QueryTask.QuerySpecification.QueryOption.CONTINUOUS)
                .setQuery(query)
                .build();

        queryTask.documentExpirationTimeMicros = Long.MAX_VALUE;

        return queryTask;
    }

    @Override
    public void handlePatch(Operation patch) {
        State state = getState(patch);
        State patchBody = getBody(patch);
        state.notificationsCounter += patchBody.notificationsCounter;
        patch.setBody(state);
        patch.complete();
    }

    public void processResults(Operation op) {
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

        queryTask.documentSelfLink = QUERY_SELF_LINK;
        Operation post = Operation.createPost(getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                .setBody(queryTask)
                .setReferer(getHost().getUri());

        getHost().sendWithDeferredResult(post)
                .thenAccept((state) -> subscribeToContinuousQuery())
                .whenCompleteNotify(op);
    }

    public void subscribeToContinuousQuery() {
        Operation post = Operation
                .createPost(getHost(), QUERY_SELF_LINK)
                .setReferer(getHost().getUri());

        URI subscriptionUri = UriUtils.buildPublicUri(getHost(), SUBSCRIPTION_SELF_LINK);

        ServiceSubscriber sr = ServiceSubscriber
                .create(false)
                .setUsePublicUri(true)
                .setSubscriberReference(subscriptionUri);

        getHost().startSubscriptionService(post, this::processResults, sr);
    }

    private void deleteSubscriptionAndContinuousQuery(Operation op) {
        Operation unsubscribeOperation = Operation.createPost(getHost(), QUERY_SELF_LINK)
                .setReferer(getUri())
                .setCompletion((o, e) -> deleteContinuousQuery());

        URI notificationTarget = UriUtils.buildPublicUri(getHost(), SUBSCRIPTION_SELF_LINK);
        getHost().stopSubscriptionService(unsubscribeOperation, notificationTarget);
    }

    private void deleteContinuousQuery() {
        Operation.createDelete(getHost(), QUERY_SELF_LINK)
                .setReferer(getUri())
                .sendWith(getHost());
    }

    public static class State extends ServiceDocument {
        public int notificationsCounter;
    }
}