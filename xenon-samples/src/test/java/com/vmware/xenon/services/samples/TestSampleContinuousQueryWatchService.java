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


import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.LuceneDocumentIndexService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestSampleContinuousQueryWatchService {

    public int serviceCount = 100;
    private List<VerificationHost> hostsToCleanup = new ArrayList<>();

    private VerificationHost createAndStartHost(boolean enableAuth) throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.setAuthorizationEnabled(enableAuth);
        // to speed up tests, set short maintenance interval.
        // it needs to "explicitly" set for VerificationHost instance
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));

        host.start();
        host.toggleServiceOptions(host.getDocumentIndexServiceUri(),EnumSet.of(ServiceOption.INSTRUMENTATION), null);

        host.startFactory(new SamplePreviousEchoService());
        host.startFactory(new SampleContinuousQueryWatchService());
        host.waitForServiceAvailable(
                SamplePreviousEchoService.FACTORY_LINK,
                SampleContinuousQueryWatchService.FACTORY_LINK);

        // add to the list for cleanup after each test run
        this.hostsToCleanup.add(host);
        return host;
    }

    @Before
    public void setup() {
        this.hostsToCleanup.forEach(VerificationHost::tearDown);
        this.hostsToCleanup.clear();
    }

    @After
    public void tearDown() {
        this.hostsToCleanup.forEach(VerificationHost::tearDown);
        this.hostsToCleanup.clear();
    }

    @Test
    public void testSampleWatchService() throws Throwable {
        VerificationHost host = createAndStartHost(false);
        TestRequestSender sender = new TestRequestSender(host);
        // create a new continuous query watch service with
        // notification counter set to 0
        SampleContinuousQueryWatchService.State sampleQueryWatchState = new SampleContinuousQueryWatchService.State();
        sampleQueryWatchState.notificationsCounter = 0;

        QueryTask.QuerySpecification spec = new QueryTask.QuerySpecification();
        spec.query.addBooleanClause(
                Query.Builder.create().addKindFieldClause(ExampleService.ExampleServiceState.class).build());
        spec.options.add(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);

        sampleQueryWatchState.querySpec = spec;
        Operation post = Operation.createPost(host, SampleContinuousQueryWatchService.FACTORY_LINK)
                .setBody(sampleQueryWatchState);

        // Verify that creating service fails if CONTINUOUS option is not specified.
        sender.sendAndWaitFailure(post);

        // Create child services before creating the Continuous query task and verify later that we get notified.
        List<ExampleService.ExampleServiceState> exampleStates =
                host.createExampleServices(host, this.serviceCount, null, ExampleService.FACTORY_LINK);
        List<String> exampleLinks =
                exampleStates.stream().map(state -> state.documentSelfLink).collect(Collectors.toList());

        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(ExampleService.ExampleServiceState.class)
                .build();

        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .setQuery(query)
                .build();

        // Simple query to with expectedResultCount in the query specification to keep query
        // from completing until result count is satisfied.
        queryTask.querySpec.expectedResultCount = (long)this.serviceCount;
        Operation verifyPost = Operation.createPost(host, ServiceUriPaths.CORE_QUERY_TASKS).setBody(queryTask);
        sender.sendAndWait(verifyPost, QueryTask.class);

        // Now create the service with CONTINUOUS option.
        spec.options.add(QueryTask.QuerySpecification.QueryOption.CONTINUOUS);
        post = Operation.createPost(host, SampleContinuousQueryWatchService.FACTORY_LINK)
                .setBody(sampleQueryWatchState);
        sampleQueryWatchState = sender.sendAndWait(post, SampleContinuousQueryWatchService.State.class);

        // remember the link to the continuous query watch service we just created
        final String queryWatchSelfLink = sampleQueryWatchState.documentSelfLink;

        // wait for filter to be active in the index service, which happens asynchronously
        // in relation to query task creation, before issuing updates.
        host.waitFor("Filter never got activated", () -> {
            Map<String, ServiceStat> indexStats = host.getServiceStats(host.getDocumentIndexServiceUri());

            ServiceStat activeQueryStat = indexStats.get(
                    LuceneDocumentIndexService.STAT_NAME_ACTIVE_QUERY_FILTERS + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            return !(activeQueryStat == null || activeQueryStat.latestValue < 1.0);
        });

        // get the continuous query watch service state and make sure that it was notified
        // for the new SamplePreviousEchoService that we created
        // the notification count should equal to the serviceCount
        host.waitFor("Sample continuous query watch service did not received all the notifications", () -> {
            Operation getQueryWatchState = Operation.createGet(host, queryWatchSelfLink);
            SampleContinuousQueryWatchService.State updatedWatchState =
                    sender.sendAndWait(getQueryWatchState, SampleContinuousQueryWatchService.State.class);
            host.log("notification count: %d", updatedWatchState.notificationsCounter);
            return (this.serviceCount <= updatedWatchState.notificationsCounter);
        });

        // Second set of child services
        host.createExampleServices(host, this.serviceCount, null);

        // get the continuous query watch service state and make sure that it was notified
        // for the new SamplePreviousEchoService that we created
        // the notification count should equal to the serviceCount
        host.waitFor("Sample continuous query watch service did not received all the notifications", () -> {
            Operation getQueryWatchState = Operation.createGet(host, queryWatchSelfLink);
            SampleContinuousQueryWatchService.State updatedWatchState =
                    sender.sendAndWait(getQueryWatchState, SampleContinuousQueryWatchService.State.class);
            host.log("notification count: %d", updatedWatchState.notificationsCounter);
            return (this.serviceCount * 2 <= updatedWatchState.notificationsCounter);
        });

        // update the state of all the SamplePreviousEchoService we created
        List<Operation> puts = new ArrayList<>();
        for (String selfLink: exampleLinks) {
            final ExampleService.ExampleServiceState updatedState = new ExampleService.ExampleServiceState();
            updatedState.name = "hello world";
            puts.add(Operation.createPut(host, selfLink)
                    .setBody(updatedState)
                    .setReferer(host.getUri()));
        }
        sender.sendAndWait(puts);

        // get the continuous query watch service state and make sure that it was notified
        // for the updates to the SamplePreviousEchoService services
        // the notification count should equal to serviceCount*2
        // (serviceCount create operations + serviceCount put operations)
        host.waitFor("Sample continuous query watch service did not received all the notifications", () -> {
            Operation getQueryWatchState = Operation.createGet(host, queryWatchSelfLink);
            SampleContinuousQueryWatchService.State updatedQueryWatchState =
                    sender.sendAndWait(getQueryWatchState, SampleContinuousQueryWatchService.State.class);
            host.log("notification count: %d", updatedQueryWatchState.notificationsCounter);
            return (this.serviceCount * 3 <= updatedQueryWatchState.notificationsCounter);
        });

        // delete all the services
        List<Operation> deletes = new ArrayList<>();
        for (String selfLink: exampleLinks) {
            deletes.add(Operation.createDelete(host, selfLink));
        }
        sender.sendAndWait(deletes);

        /// get the continuous query watch service state and make sure that it was notified
        // for the deletes to the SamplePreviousEchoService services
        // the notification count should equal to serviceCount*3
        // (serviceCount create operations + serviceCount put operations +
        //  serviceCount delete operations)
        host.waitFor("Sample continuous query watch service did not received all the notifications", () -> {
            Operation getQueryWatchState = Operation.createGet(host, queryWatchSelfLink);
            SampleContinuousQueryWatchService.State updatedQueryWatchState =
                    sender.sendAndWait(getQueryWatchState, SampleContinuousQueryWatchService.State.class);
            host.log("notification count: %d", updatedQueryWatchState.notificationsCounter);
            return (this.serviceCount * 4 <= updatedQueryWatchState.notificationsCounter);
        });
    }
}