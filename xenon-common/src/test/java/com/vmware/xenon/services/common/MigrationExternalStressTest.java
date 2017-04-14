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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
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
import com.vmware.xenon.common.FactoryService;
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
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

/**
 * TODO: document
 */
public class MigrationExternalStressTest {

    private static final URI SOURCE_PEER_URI = UriUtils.buildUri("http://localhost:9000");
    private static final URI DEST_PEER_URI = UriUtils.buildUri("http://localhost:9001");

    public static class NonInstrumentedExampleService extends ExampleService {
        public NonInstrumentedExampleService() {
            super();
            this.toggleOption(ServiceOption.INSTRUMENTATION, false);
        }

        public static FactoryService createFactory() {
            return FactoryService.create(NonInstrumentedExampleService.class);
        }
    }

    public static class SourceServer {
        public static void main(String[] args) throws Throwable {
            startServerFromCommandLine(9000);
        }
    }

    public static class DestServer {
        public static void main(String[] args) throws Throwable {
            startServerFromCommandLine(9001);
        }
    }

    private static void startServerFromCommandLine(int port) throws Throwable {
        Properties properties = System.getProperties();
        String nodeCountProperty = properties.getProperty(Utils.PROPERTY_NAME_PREFIX + "nodeCount");
        int nodeCount = Integer.valueOf(nodeCountProperty);

        Logger.getAnonymousLogger().log(Level.INFO, String.format("port=%d, nodeCount=%d", port, nodeCount));
        // TODO: get port from property
        VerificationHost parentHost = prepareHost(nodeCount, port);
        parentHost.setProcessOwner(true);
    }

    public long totalDataSize = 1_000;
    public int dataBatchSize = 100;
    //    public int sourceNodeCount = 3;
//    public int destNodeCount = 3;
    public long taskWaitSeconds = Duration.ofMinutes(1).getSeconds();
    public int sleepAfterDataPopulationMinutes = 0;
    public boolean runThisTest;

//    private Set<VerificationHost> hostToClean = new HashSet<>();

    // used for sending requests, writing logs, etc.
    private VerificationHost host;


    @Before
    public void setUp() throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.start();
        // increase timeout
        this.host.setStressTest(true);
        this.host.getTestRequestSender().setTimeout(Duration.ofMinutes(10));

        CommandLineArgumentParser.parseFromProperties(this);
//
//        String message = String.format(
//                "totalDataSize=%,d, dataBatchSize=%,d, sourceNodeCount=%d, destNodeCount=%d, sleepAfterDataPopulationMinutes=%d",
//                this.totalDataSize, this.dataBatchSize, this.sourceNodeCount, this.destNodeCount, this.sleepAfterDataPopulationMinutes);
//        Logger.getAnonymousLogger().log(Level.INFO, message);
    }

    @After
    public void tearDown() {

//        for (VerificationHost host : this.hostToClean) {
//            for (VerificationHost h : host.getInProcessHostMap().values()) {
//                h.tearDown();
//            }
//            host.tearDown();
//        }

        if (!this.runThisTest) {
            return;
        }

        Operation sourceOp = Operation.createDelete(UriUtils.buildUri(SOURCE_PEER_URI, ServiceUriPaths.CORE_MANAGEMENT));
        Operation destOp = Operation.createDelete(UriUtils.buildUri(DEST_PEER_URI, ServiceUriPaths.CORE_MANAGEMENT));

        List<Operation> deletes = new ArrayList<>();
        deletes.add(sourceOp);
        deletes.add(destOp);
        this.host.getTestRequestSender().sendAndWait(deletes);

        this.host.tearDown();
    }

    private static VerificationHost prepareHost(int nodeCount, int port) throws Throwable {
        VerificationHost parentHost = VerificationHost.create(port);
        parentHost.start();
        parentHost.setPeerSynchronizationEnabled(true);
        parentHost.setUpPeerHosts(nodeCount);

        parentHost.setNodeGroupQuorum(nodeCount);
        parentHost.joinNodesAndVerifyConvergence(nodeCount, true);

        // increase timeout(test wait and sending op)
        parentHost.setStressTest(true);
        for (VerificationHost h : parentHost.getInProcessHostMap().values()) {
            h.setStressTest(true);
        }

        // stop example service
        parentHost.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        parentHost.getTestRequestSender().sendAndWait(Operation.createDelete(parentHost, ExampleService.FACTORY_LINK));

        // start NonInstrumentedExampleService instead
        parentHost.startFactory(NonInstrumentedExampleService.class, NonInstrumentedExampleService::createFactory);

        // TODO:
        parentHost.startFactory(new MigrationTaskService());
        parentHost.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);


        parentHost.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        for (VerificationHost h : parentHost.getInProcessHostMap().values()) {
            // stop example service
            h.waitForServiceAvailable(ExampleService.FACTORY_LINK);
            h.getTestRequestSender().sendAndWait(Operation.createDelete(h, ExampleService.FACTORY_LINK));

            // start NonInstrumentedExampleService instead
            h.startFactory(NonInstrumentedExampleService.class, NonInstrumentedExampleService::createFactory);

            // TODO:
            h.startFactory(new MigrationTaskService());
            h.waitForServiceAvailable(MigrationTaskService.FACTORY_LINK);
        }

//        this.hostToClean.add(parentHost);
        return parentHost;
    }

    private State triggerMigrationTask(URI sourceHostUri, URI destHostUri) {
        State state = new State();
        state.destinationFactoryLink = ExampleService.FACTORY_LINK;
        state.destinationNodeGroupReference = UriUtils.buildUri(destHostUri, ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.sourceFactoryLink = ExampleService.FACTORY_LINK;
        state.sourceNodeGroupReference = UriUtils.buildUri(sourceHostUri, ServiceUriPaths.DEFAULT_NODE_GROUP);

        // just to check count query performance
        state.querySpec = new QuerySpecification();
        state.querySpec.options.add(QueryOption.INCLUDE_ALL_VERSIONS);

        // specify expiration time which transcends to query pages
        state.documentExpirationTimeMicros = Utils.getSystemNowMicrosUtc() + TimeUnit.SECONDS.toMicros(this.taskWaitSeconds);

        Operation op = Operation.createPost(UriUtils.buildUri(destHostUri, MigrationTaskService.FACTORY_LINK)).setBody(state);
        state = this.host.getTestRequestSender().sendAndWait(op, State.class);
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

        assumeTrue("Need to set runThisTest=true", this.runThisTest);

//        VerificationHost sourceParentHost = prepareHost(this.sourceNodeCount);
//        VerificationHost destParentHost = prepareHost(this.destNodeCount);
//
//        String sourcePeers = sourceParentHost.getInProcessHostMap().values().stream()
//                .map(VerificationHost::getUri)
//                .map(URI::toString)
//                .collect(joining(","));
//        String destPeers = destParentHost.getInProcessHostMap().values().stream()
//                .map(VerificationHost::getUri)
//                .map(URI::toString)
//                .collect(joining(","));
//        destParentHost.log("source hosts: %s", sourcePeers);
//        destParentHost.log("dest hosts: %s", destPeers);
//
//        VerificationHost sourcePeer = sourceParentHost.getPeerHost();
//        VerificationHost destPeer = destParentHost.getPeerHost();

//        this.host = VerificationHost.create(0);
        TestRequestSender sender = this.host.getTestRequestSender();


        long beforeDataPopulation = System.currentTimeMillis();
        populateMiniData(SOURCE_PEER_URI);
        long afterDataPopulation = System.currentTimeMillis();
        log("Populating %,d data took %,d msec", this.totalDataSize, afterDataPopulation - beforeDataPopulation);

        log("Sleep start %d minutes", this.sleepAfterDataPopulationMinutes);
        TimeUnit.MINUTES.sleep(this.sleepAfterDataPopulationMinutes);
        log("Sleep end");

        State migrationState = triggerMigrationTask(SOURCE_PEER_URI, DEST_PEER_URI);
        URI taskUri = UriUtils.buildUri(DEST_PEER_URI, migrationState.documentSelfLink);

        log("Migration task URI=%s", taskUri);

        State finalState = waitForServiceCompletion(sender, taskUri);
        if (TaskStage.FAILED == finalState.taskInfo.stage) {
            log(Level.WARNING, "Migration failed: %s", Utils.toJson(finalState.taskInfo.failure));
        }

        // quick check count with /core/examples?$count=true
        URI factoryUri = UriUtils.buildUri(DEST_PEER_URI, ExampleService.FACTORY_LINK);
        URI countUri = UriUtils.appendQueryParam(factoryUri, UriUtils.URI_PARAM_ODATA_COUNT, "true");
        Operation get = Operation.createGet(countUri);

        sender.setTimeout(Duration.ofMinutes(10));
        ODataFactoryQueryResult result = sender.sendAndWait(get, ODataFactoryQueryResult.class);

        // TODO: if failed, capture task stats
        assertEquals(Long.valueOf(this.totalDataSize), result.totalCount);
    }

    private void populateMiniData(URI sourceHostUri) {
        // TODO: log
        int batch = this.dataBatchSize;
        long total = this.totalDataSize;


        List<Operation> ops = new ArrayList<>();
        for (long i = 0; i < total; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            ops.add(Operation.createPost(UriUtils.buildUri(sourceHostUri, ExampleService.FACTORY_LINK)).setBody(state));

            if (i != 0 && i % batch == 0) {
                long batchStart = System.currentTimeMillis();
                this.host.getTestRequestSender().sendAndWait(ops);
                long batchEnd = System.currentTimeMillis();
                log("populating data: i=%,d, took=%d", i, batchEnd - batchStart);
                ops.clear();
            }
        }

        // send remaining
        long lastStart = System.currentTimeMillis();
        this.host.getTestRequestSender().sendAndWait(ops);
        long lastEnd = System.currentTimeMillis();
        log("populating remaining: took=%d", lastEnd - lastStart);
    }

    private void log(Level level, String fmt, Object... args) {
        this.host.log(level, fmt, args);
    }

    private void log(String fmt, Object... args) {
        this.host.log(fmt, args);
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
