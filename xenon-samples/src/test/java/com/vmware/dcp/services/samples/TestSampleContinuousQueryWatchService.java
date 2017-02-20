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

package com.vmware.dcp.services.samples;

import static com.vmware.xenon.common.test.TestContext.waitFor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.LuceneDocumentIndexService;
import com.vmware.xenon.services.samples.SampleContinuousQueryWatchService;
import com.vmware.xenon.services.samples.SamplePreviousEchoService;
import com.vmware.xenon.services.samples.SamplePreviousEchoService.EchoServiceState;


public class TestSampleContinuousQueryWatchService {

    private VerificationHost host;
    private TestRequestSender sender;
    public int serviceCount = 10;
    private Duration timeout = Duration.ofSeconds(5);

    @Before
    public void setupHost() throws Exception, Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = VerificationHost.create(0);
        this.host.start();
        this.host.toggleServiceOptions(this.host.getDocumentIndexServiceUri(),
                EnumSet.of(ServiceOption.INSTRUMENTATION),
                null);
        this.sender = new TestRequestSender(this.host);
        this.host.startFactory(new SamplePreviousEchoService());
        this.host.startFactory(new SampleContinuousQueryWatchService());
        this.host.waitForReplicatedFactoryServiceAvailable(
                UriUtils.buildUri(this.host, SamplePreviousEchoService.FACTORY_LINK));
        this.host.waitForReplicatedFactoryServiceAvailable(
                UriUtils.buildUri(this.host, SampleContinuousQueryWatchService.FACTORY_LINK));
    }

    @After
    public void cleanup() {
        if (this.host == null) {
            return;
        }
        try {
            this.host.logServiceStats(this.host.getDocumentIndexServiceUri());
        } catch (Throwable e) {
            this.host.log("Error logging stats: %s", e.toString());
        }
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }

    @Test
    public void testSampleWatchService() throws Throwable {
        // create a new continuous query watch service with
        // notification counter set to 0
        SampleContinuousQueryWatchService.State sampleQueryWatchState = new SampleContinuousQueryWatchService.State();
        sampleQueryWatchState.notificationsCounter = 0;
        Operation post = Operation.createPost(UriUtils.buildUri(this.host.getUri(),
                SampleContinuousQueryWatchService.FACTORY_LINK))
                .setBody(sampleQueryWatchState)
                .setReferer(this.host.getUri());
        sampleQueryWatchState = this.sender.sendAndWait(post, SampleContinuousQueryWatchService.State.class);
        // remember the link to the continuous query watch service we just created
        final String queryWatchSelfLink = sampleQueryWatchState.documentSelfLink;

        // wait for filter to be active in the index service, which happens asynchronously
        // in relation to query task creation, before issuing updates.
        waitFor(this.timeout, () -> {
            ServiceStats indexStats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(this.host.getDocumentIndexServiceUri()));
            ServiceStat activeQueryStat = indexStats.entries.get(
                    LuceneDocumentIndexService.STAT_NAME_ACTIVE_QUERY_FILTERS
                            + ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
            if (activeQueryStat == null || activeQueryStat.latestValue < 1.0) {
                return false;
            }
            return true;
        });

        // create serviceCount instances of SamplePreviousEchoService
        // and save the links to these services
        List<String> samplePreviousEchoServicesLinks = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            EchoServiceState state = new EchoServiceState();
            state.message = "hello";
            post = Operation.createPost(UriUtils.buildUri(this.host.getUri(),
                    SamplePreviousEchoService.FACTORY_LINK))
                    .setBody(state)
                    .setReferer(this.host.getUri());
            state = this.sender.sendAndWait(post, EchoServiceState.class);
            samplePreviousEchoServicesLinks.add(state.documentSelfLink);
        }

        // get the continuous query watch service state and make sure that it was notified
        // for the new SamplePreviousEchoService that we created
        // the notification count should equal to the serviceCount
        waitFor(this.timeout, () -> {
            Operation getQueryWatchState = Operation.createGet(
                    UriUtils.buildUri(this.host.getUri(), queryWatchSelfLink))
                    .setReferer(this.host.getUri());
            SampleContinuousQueryWatchService.State updatedWatchState =
                    this.sender.sendAndWait(getQueryWatchState, SampleContinuousQueryWatchService.State.class);
            this.host.log("notification count: %d",
                    updatedWatchState.notificationsCounter);
            return (this.serviceCount <= updatedWatchState.notificationsCounter);
        });

        // update the state of all the SamplePreviousEchoService we created
        final EchoServiceState updatedState = new EchoServiceState();
        updatedState.message = "hello world";
        TestContext ctx1 = new TestContext(samplePreviousEchoServicesLinks.size(), this.timeout);
        samplePreviousEchoServicesLinks.forEach(selfLink -> {
            Operation putOp = Operation.createPut(UriUtils.buildUri(this.host.getUri(), selfLink))
                    .setBody(updatedState)
                    .setReferer(this.host.getUri())
                    .setCompletion(ctx1.getCompletion());
            this.sender.sendRequest(putOp);
        });
        ctx1.await();

        // get the continuous query watch service state and make sure that it was notified
        // for the updates to the SamplePreviousEchoService services
        // the notification count should equal to serviceCount*2
        // (serviceCount create operations + serviceCount put operations)
        waitFor(this.timeout, () -> {
            Operation getQueryWatchState = Operation.createGet(
                    UriUtils.buildUri(this.host.getUri(), queryWatchSelfLink))
                    .setReferer(this.host.getUri());
            SampleContinuousQueryWatchService.State updatedQueryWatchState =
                    this.sender.sendAndWait(getQueryWatchState, SampleContinuousQueryWatchService.State.class);
            this.host.log("notification count: %d",
                    updatedQueryWatchState.notificationsCounter);
            return (this.serviceCount * 2 <= updatedQueryWatchState.notificationsCounter);
        });

        // delete all the services
        TestContext ctx = new TestContext(samplePreviousEchoServicesLinks.size(), this.timeout);
        samplePreviousEchoServicesLinks.forEach(selfLink -> {
            Operation delete = Operation
                    .createDelete(UriUtils.buildUri(this.host.getUri(), selfLink))
                    .setReferer(this.host.getUri()).setCompletion(ctx.getCompletion());
            this.sender.sendRequest(delete);
        });
        ctx.await();

        /// get the continuous query watch service state and make sure that it was notified
        // for the deletes to the SamplePreviousEchoService services
        // the notification count should equal to serviceCount*3
        // (serviceCount create operations + serviceCount put operations +
        //  serviceCount delete operations)
        waitFor(this.timeout, () -> {
            Operation getQueryWatchState = Operation.createGet(
                    UriUtils.buildUri(this.host.getUri(), queryWatchSelfLink))
                    .setReferer(this.host.getUri());
            SampleContinuousQueryWatchService.State updatedQueryWatchState =
                    this.sender.sendAndWait(getQueryWatchState, SampleContinuousQueryWatchService.State.class);
            this.host.log("notification count: %d",
                    updatedQueryWatchState.notificationsCounter);
            return (this.serviceCount * 3 <= updatedQueryWatchState.notificationsCounter);
        });
    }
}
