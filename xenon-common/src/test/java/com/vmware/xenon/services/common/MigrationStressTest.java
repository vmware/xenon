/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import static java.util.stream.Collectors.joining;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.ODataFactoryQueryResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.ExceptionTestUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MigrationTaskService.State;

/**
 * TODO: document
 */
public class MigrationStressTest {


    public long totalDataSize = 1_000;
    public int dataBatchSize = 100;
    public int sourceNodeCount = 3;
    public int destNodeCount = 3;
    public long taskWaitSeconds = Duration.ofMinutes(1).getSeconds();

    private Set<VerificationHost> hostToClean = new HashSet<>();

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);

        String message = String.format(
                "totalDataSize=%,d, dataBatchSize=%,d, sourceNodeCount=%d, destNodeCount=%d",
                this.totalDataSize, this.dataBatchSize, this.sourceNodeCount, this.destNodeCount);
        Logger.getAnonymousLogger().log(Level.INFO, message);
    }

    @After
    public void tearDown() {
        for (VerificationHost host : this.hostToClean) {
            for (VerificationHost h : host.getInProcessHostMap().values()) {
                h.tearDown();
            }
            host.tearDown();
        }
    }

    public VerificationHost prepareHost(int nodeCount) throws Throwable {
        VerificationHost parentHost = VerificationHost.create(0);
        parentHost.start();
        parentHost.setPeerSynchronizationEnabled(true);
        parentHost.setUpPeerHosts(nodeCount);

        parentHost.setNodeGroupQuorum(nodeCount);
        parentHost.joinNodesAndVerifyConvergence(nodeCount, true);

        parentHost.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        for (VerificationHost h : parentHost.getInProcessHostMap().values()) {

            h.waitForServiceAvailable(ExampleService.FACTORY_LINK);
            h.toggleServiceOptions(UriUtils.buildUri(h, ExampleService.FACTORY_LINK),
                    null, EnumSet.of(ServiceOption.INSTRUMENTATION));

            // TODO:
            h.startFactory(new MigrationTaskService());
            h.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);
        }

        this.hostToClean.add(parentHost);
        return parentHost;
    }

    private State triggerMigrationTask(VerificationHost sourceHost, VerificationHost destHost) {
        State state = new State();
        state.destinationFactoryLink = ExampleService.FACTORY_LINK;
        state.destinationNodeGroupReference = UriUtils.buildUri(destHost.getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.sourceFactoryLink = ExampleService.FACTORY_LINK;
        state.sourceNodeGroupReference = UriUtils.buildUri(sourceHost.getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);

        // specify expiration time which transcends to query pages
        state.documentExpirationTimeMicros = Utils.getSystemNowMicrosUtc() + TimeUnit.SECONDS.toMicros(this.taskWaitSeconds);

        Operation op = Operation.createPost(destHost, MigrationTaskService.FACTORY_LINK).setBody(state);
        state = destHost.getTestRequestSender().sendAndWait(op, State.class);
        return state;
    }

    private State waitForServiceCompletion(TestRequestSender sender, URI taskUri) {
        Set<TaskStage> finalStages = EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED, TaskStage.FINISHED);

        Duration waitDuration = Duration.ofSeconds(this.taskWaitSeconds);
        AtomicReference<State> stateHolder = new AtomicReference<>();
        TestContext.waitFor(waitDuration, () -> {
            State state = sender.sendAndWait(Operation.createGet(taskUri), State.class);
            stateHolder.set(state);
            return finalStages.contains(state.taskInfo.stage);
        }, () -> "Timeout while waiting migration task to finish");
        return stateHolder.get();
    }


    @Test
    public void simpleMigration() throws Throwable {
        VerificationHost sourceParentHost = prepareHost(this.sourceNodeCount);
        VerificationHost destParentHost = prepareHost(this.destNodeCount);

        String sourcePeers = sourceParentHost.getInProcessHostMap().values().stream()
                .map(VerificationHost::getUri)
                .map(URI::toString)
                .collect(joining(","));
        String destPeers = destParentHost.getInProcessHostMap().values().stream()
                .map(VerificationHost::getUri)
                .map(URI::toString)
                .collect(joining(","));
        destParentHost.log("source hosts: %s", sourcePeers);
        destParentHost.log("dest hosts: %s", destPeers);

        VerificationHost sourcePeer = sourceParentHost.getPeerHost();
        VerificationHost destPeer = destParentHost.getPeerHost();


        populateMiniData(sourcePeer);

        State migrationState = triggerMigrationTask(sourcePeer, destPeer);
        URI taskUri = UriUtils.buildUri(destPeer, migrationState.documentSelfLink);

        destPeer.log("Migration task URI=%s", taskUri);

        waitForServiceCompletion(destPeer.getTestRequestSender(), taskUri);

        Operation get = Operation.createGet(destPeer, ExampleService.FACTORY_LINK + "?$count=true");

        ODataFactoryQueryResult result = destPeer.getTestRequestSender().sendAndWait(get, ODataFactoryQueryResult.class);
        // TODO: if failed, capture task stats
        assertEquals(Long.valueOf(this.totalDataSize), result.totalCount);
    }

    private void populateMiniData(VerificationHost host) {
        // TODO: log
        int batch = this.dataBatchSize;
        long total = this.totalDataSize;


        List<Operation> ops = new ArrayList<>();
        for (long i = 0; i < total; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            ops.add(Operation.createPost(host, ExampleService.FACTORY_LINK).setBody(state));

            if (i != 0 && i % batch == 0) {
                long batchStart = System.currentTimeMillis();
                host.getTestRequestSender().sendAndWait(ops);
                long batchEnd = System.currentTimeMillis();
                host.log("populating data: i=%,d, took=%d", i, batchEnd - batchStart);
                ops.clear();
            }
        }

        // send remaining
        long lastStart = System.currentTimeMillis();
        host.getTestRequestSender().sendAndWait(ops);
        long lastEnd = System.currentTimeMillis();
        host.log("populating remaining: took=%d", lastEnd - lastStart);
    }

    //    @Test
    public void main() throws Throwable {
        VerificationHost host = VerificationHost.create(8005);
        host.start();

//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            host.log(Level.WARNING, "Host stopping ...");
//            for (VerificationHost h : host.getInProcessHostMap().values()) {
//                h.tearDown();
//            }
//            host.tearDown();
//            host.log(Level.WARNING, "Host is stopped");
//        }));

        host.setPeerSynchronizationEnabled(true);
        host.setUpPeerHosts(1);

//        int nodeCount = 3;
//        host.setPeerSynchronizationEnabled(true);
//        host.setUpPeerHosts(nodeCount);
//        host.setNodeGroupQuorum(nodeCount);
//        host.joinNodesAndVerifyConvergence(nodeCount, true);

        VerificationHost source = host.getPeerHost();
        source.testDurationSeconds = TimeUnit.HOURS.toSeconds(1);

        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        for (VerificationHost h : host.getInProcessHostMap().values()) {
            h.waitForServiceAvailable(ExampleService.FACTORY_LINK);

            host.toggleServiceOptions(UriUtils.buildUri(host, ExampleService.FACTORY_LINK),
                    null, EnumSet.of(ServiceOption.INSTRUMENTATION));
        }

//        host.getDocumentIndexServiceUri();


        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {
            ExceptionTestUtils.executeSafely(() -> {
//                populateData(host);
                long beforePopulate = System.currentTimeMillis();
                populateData(source);

                ////
//                host.setPeerSynchronizationEnabled(true);
                host.setUpPeerHosts(2);

                for (VerificationHost h : host.getInProcessHostMap().values()) {
                    h.testDurationSeconds = TimeUnit.HOURS.toSeconds(1);
                    h.waitForServiceAvailable(ExampleService.FACTORY_LINK);
                    h.toggleServiceOptions(UriUtils.buildUri(h, ExampleService.FACTORY_LINK),
                            null, EnumSet.of(ServiceOption.INSTRUMENTATION));
                }

                host.setLoggingLevel(Level.OFF);

                host.setNodeGroupQuorum(3);
                host.joinNodesAndVerifyConvergence(3, true);
                //// TODO: wait for for nodegroup
//                TimeUnit.SECONDS.sleep(5);
                for (VerificationHost h : host.getInProcessHostMap().values()) {
                    h.waitForNodeGroupIsAvailableConvergence();
                    h.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(h, ExampleService.FACTORY_LINK));
                }


                long time = System.currentTimeMillis() - beforePopulate;
                System.out.println("========");
                System.out.println("========");
                System.out.println("========");
                System.out.println("========");
                System.out.println("========");
                System.out.println("========");
                System.out.println("========");
                System.out.println("========");
                for (VerificationHost h : host.getInProcessHostMap().values()) {
                    System.out.println(h.getUri());
                }
                System.out.println("ready=" + time);
                System.out.println("========");

                host.setLoggingLevel(Level.INFO);

                while (true) {
                    TimeUnit.SECONDS.sleep(1);
                }
            });
        });

        future.get();
//        populateData(host);
    }

    private static void populateData(VerificationHost host) throws Throwable {
        System.out.println("total memory=" + Runtime.getRuntime().totalMemory());
        System.out.println("free memory=" + Runtime.getRuntime().freeMemory());
        // turn off instrumentation to save memory from stats

        long begin = System.currentTimeMillis();
//        int size = 2_000_000;
//        int size = 1_000_000;
//        int size = 500_000;
        int size = 100_000;
//        int size = 100_000;  // 200m
//        int size = 120_000;
        int batch = 10_000;
        List<Operation> ops = new ArrayList<>(batch);
        long start = System.currentTimeMillis();
        for (int i = 0; i <= size; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            ops.add(Operation.createPost(host, ExampleService.FACTORY_LINK).setBody(state));
            if (i != 0 && i % batch == 0) {
                host.getTestRequestSender().sendAndWait(ops);
//                ops.clear();
                ops = new ArrayList<>();
                long took = System.currentTimeMillis() - start;
                System.out.println(String.format("batched: %d took= %d", i, took));
//                System.out.println("batched: " + i + " took=" + took);
                start = System.currentTimeMillis();
//                TimeUnit.MILLISECONDS.sleep(100);
            }

            // without this, OOO
//            if (i != 0 && i % 100_000 == 0) {
//                performMaintenanceSynchronously(host);
//                System.out.println("=== sleep start ==");
//                TimeUnit.SECONDS.sleep(10);
//                System.out.println("=== sleep end ==");
//            }
        }
        System.out.println(System.currentTimeMillis() - begin);
        System.out.println("total memory=" + Runtime.getRuntime().totalMemory());
        System.out.println("free memory=" + Runtime.getRuntime().freeMemory());

        System.out.println("Performing GC");
        System.gc();
    }
}
