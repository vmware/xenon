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

import java.util.EnumSet;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TestSimpleTransactionService.BankAccountService.BankAccountServiceState;
import com.vmware.xenon.services.samples.FsmTaskService.FsmTaskServiceState;
import com.vmware.xenon.services.samples.SampleServiceWithCustomUi.SampleServiceWithCustomUiState;

/**
 * Simple service that creates a continuous query task to monitor
 * changes to other service documents in the xenon-samples project
 * by creating a continuous query task and subscribing to it.
 * This service keeps track of the number of query results it has received.
 */
public class SampleContinuousQueryObserverService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/continuousQueryObserverExample";

    // very simple state to track the number of notifications
    public static class QueryObserverState extends ServiceDocument {
        public int notificationsCounter;
    }

    public SampleContinuousQueryObserverService() {
        super(QueryObserverState.class);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.PERSISTENCE, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        // create the query
        Query query = Query.Builder.create()
                .addKindFieldClause(SamplePreviousEchoService.EchoServiceState.class, Occurance.SHOULD_OCCUR)
                .addKindFieldClause(BankAccountServiceState.class, Occurance.SHOULD_OCCUR)
                .addKindFieldClause(FsmTaskServiceState.class, Occurance.SHOULD_OCCUR)
                .addKindFieldClause(SampleServiceWithCustomUiState.class, Occurance.SHOULD_OCCUR)
                .build();

        // build the continuous query task
        QueryTask continuousQuerytask = QueryTask.Builder.create().addOptions(
                EnumSet.of(QueryTask.QuerySpecification.QueryOption.CONTINUOUS,
                        QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT))
                .setQuery(query).build();

        // If the expiration time is not set, then the query will receive the default
        // query expiration time of 1 minute.
        continuousQuerytask.documentExpirationTimeMicros = Long.MAX_VALUE;

        // subscribe to the continuous query task
        Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
        .setBody(continuousQuerytask)
        .setReferer(getUri())
        .setCompletion((completedOp, failure) -> {
            QueryTask rsp = completedOp.getBody(QueryTask.class);
            // Now that we have selfLink of this query,
            // subscribe to it to get updates to factory and child services both with replay enabled.
            Operation subscribe = Operation.createPost(this, rsp.documentSelfLink)
                    .setReferer(getUri());

            getHost().startSubscriptionService(subscribe,
                    (notifyOp) -> {
                        notifyOp.complete();
                        if (!notifyOp.hasBody()) {
                            return;
                        }
                        QueryTask taskState = notifyOp.getBody(QueryTask.class);
                        if (null != taskState.results && null != taskState.results.documents) {
                            QueryObserverState newState = new QueryObserverState();
                            newState.notificationsCounter = taskState.results.documents.size();
                            // patch the state with the number of new notifications received
                            Operation.createPatch(getUri())
                            .setBody(newState)
                            .sendWith(this);
                        }
                    }, ServiceSubscriptionState.ServiceSubscriber.create(true));
        }).sendWith(this);

        super.handleStart(startPost);
    }

    @Override
    public void handlePatch(Operation patch) {
        if (!patch.hasBody()) {
            patch.complete();
            return;
        }
        QueryObserverState currentState = getState(patch);
        QueryObserverState newState = patch.getBody(QueryObserverState.class);
        // update the state by updating the number of received notifications
        currentState.notificationsCounter += newState.notificationsCounter;
        setState(patch, currentState);
        patch.complete();
    }

}
