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

package com.vmware.xenon.services.common.stress;

import static org.junit.Assert.fail;

import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost.Arguments;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleODLService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MigrationTaskService;
import com.vmware.xenon.services.common.MigrationTaskService.State;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Migration stress test
 *
 * When this test is run with normal profile(e.g: "mvn test", from IDE), it uses in-process hosts with random ports.
 * When run with "ci" profile, it is expecting hosts are running outside of the test process.
 * See more details on CI job configuration.
 *
 */
public class MigrationStressTest {

    private static final String DEFAULT_SOURCE_HOST = "http://localhost:9000";
    private static final String DEFAULT_DEST_HOST = "http://localhost:9001";

    private static final String SYSTEM_PROP_HOST_PORT = Utils.PROPERTY_NAME_PREFIX + "port";
    private static final String SYSTEM_PROP_HOST_SANDBOX = Utils.PROPERTY_NAME_PREFIX + "sandbox";
    private static final String SYSTEM_PROP_HOST_NODE_COUNT = Utils.PROPERTY_NAME_PREFIX + "nodeCount";

    /**
     * ServiceHost that is started by CI
     *
     * command example:
     * <pre>
     * ./mvnw -B -e exec:java  \
     *   -pl xenon-stress-test  \
     *   -Dexec.mainClass="com.vmware.xenon.services.common.stress.MigrationStressTest\$MigrationHost"  \
     *   -Dxenon.port=9000  \
     *   -Dxenon.sandbox=...
     *   -P performance &
     * </pre>
     */
    public static class MigrationHost {

        public static void main(String[] args) throws Throwable {
            Properties properties = System.getProperties();

            String sandboxDir = properties.getProperty(SYSTEM_PROP_HOST_SANDBOX);
            if (sandboxDir != null) {
                sandboxDir = sandboxDir.trim();
            }

            int port = Integer.getInteger(SYSTEM_PROP_HOST_PORT, 0);
            int nodeCount = Integer.getInteger(SYSTEM_PROP_HOST_NODE_COUNT, 0);

            Logger.getAnonymousLogger().log(Level.INFO, String.format("port=%d, nodeCount=%d, sandbox=%s", port, nodeCount, sandboxDir));
            VerificationHost parentHost = prepareHost(0, port, sandboxDir, args);
            parentHost.setProcessOwner(true);
        }
    }


    public String sourceHost = DEFAULT_SOURCE_HOST;
    public String destHost = DEFAULT_DEST_HOST;

    public long totalDataSize = 1_000;
    public int dataBatchSize = 100;
    public long taskWaitSeconds = Duration.ofMinutes(1).getSeconds();
    public long requestSenderTimeOutMin = 10;
    public int sleepAfterDataPopulationMinutes = 0;
    public int sleepAfterMigrationMinutes = 0;
    public int resultLimit = 10_000;

    public boolean startHost = !StressTestUtils.isRunningOnCI();

    // used for sending requests, writing logs, etc.
    private VerificationHost host;
    private Set<VerificationHost> hostToClean = new HashSet<>();

    private Logger logger = Logger.getLogger(MigrationStressTest.class.getName());


    private static VerificationHost prepareHost(int nodeCount, int port, String sandboxDir, String[] commandLineArgs) throws Throwable {
        Utils.setTimeDriftThreshold(TimeUnit.SECONDS.toMicros(120));

        Arguments args = new Arguments();
        args.port = port;
        if (sandboxDir == null) {
            // nullify dir to use temporal location since instantiating Arguments automatically assign default location
            args.sandbox = null;
        } else {
            args.sandbox = Paths.get(sandboxDir);
        }

        // bind command line args if provided
        if (commandLineArgs != null) {
            CommandLineArgumentParser.parse(args, commandLineArgs);
        }

        VerificationHost parentHost = VerificationHost.create(args);
        parentHost.start();

        parentHost.setUpPeerHosts(nodeCount);
        parentHost.setNodeGroupQuorum(nodeCount);
        parentHost.joinNodesAndVerifyConvergence(nodeCount, true);

        // increase timeout(test wait and sending op)
        parentHost.setStressTest(true);
        for (VerificationHost peer : parentHost.getInProcessHostMap().values()) {
            peer.setStressTest(true);
        }

        // start services and wait
        parentHost.startFactory(ExampleODLService.class, ExampleODLService::createFactory);
        parentHost.startFactory(new MigrationTaskService());
        parentHost.waitForServiceAvailable(ExampleODLService.FACTORY_LINK, MigrationTaskService.FACTORY_LINK);

        for (VerificationHost peer : parentHost.getInProcessHostMap().values()) {
            peer.startFactory(ExampleODLService.class, ExampleODLService::createFactory);
            peer.startFactory(new MigrationTaskService());
            peer.waitForServiceAvailable(ExampleODLService.FACTORY_LINK, MigrationTaskService.FACTORY_LINK);
        }

        return parentHost;
    }

    @Before
    public void setUp() throws Throwable {

        CommandLineArgumentParser.parseFromProperties(this);

        // if not started with ci profile, start source/dest hosts
        if (this.startHost) {
            this.logger.log(Level.INFO, "Starting source server");
            VerificationHost sourceHost = prepareHost(0, 0, null, null);

            this.logger.log(Level.INFO, "Starting dest server");
            VerificationHost destHost = prepareHost(0, 0, null, null);

            this.hostToClean.addAll(Arrays.asList(sourceHost, destHost));

            this.sourceHost = sourceHost.getUri().toString();
            this.destHost = destHost.getUri().toString();
        }


        this.host = VerificationHost.create(0);
        this.host.start();
        // increase timeout
        this.host.setStressTest(true);
        this.host.getTestRequestSender().setTimeout(Duration.ofMinutes(this.requestSenderTimeOutMin));
        this.hostToClean.add(this.host);

        String message =
                String.format("" +
                                " sourceHost=%s, " +
                                " destHost=%s, " +
                                " totalDataSize=%,d, " +
                                " dataBatchSize=%,d, " +
                                " taskWaitSeconds=%,d, " +
                                " requestSenderTimeOutMin=%,d, " +
                                " sleepAfterDataPopulationMinutes=%d" +
                                " sleepAfterMigrationMinutes=%d" +
                                " resultLimit=%,d, ",
                        this.sourceHost,
                        this.destHost,
                        this.totalDataSize,
                        this.dataBatchSize,
                        this.taskWaitSeconds,
                        this.requestSenderTimeOutMin,
                        this.sleepAfterDataPopulationMinutes,
                        this.sleepAfterMigrationMinutes,
                        this.resultLimit
                );
        this.logger.log(Level.INFO, message);
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


    /**
     * Test migration with ODL services
     */
    @Test
    public void odlServiceMigration() throws Throwable {

        TestRequestSender sender = this.host.getTestRequestSender();

        URI sourceUri = URI.create(this.sourceHost);
        URI destUri = URI.create(this.destHost);


        long beforeDataPopulation = System.currentTimeMillis();
        populateData(sourceUri);
        long afterDataPopulation = System.currentTimeMillis();
        this.host.log("Populating %,d data took %,d msec", this.totalDataSize, afterDataPopulation - beforeDataPopulation);
        logMemoryUsage();

        this.host.log("Sleep start %d minutes", this.sleepAfterDataPopulationMinutes);
        TimeUnit.MINUTES.sleep(this.sleepAfterDataPopulationMinutes);
        this.host.log("Sleep end");

        long beforeMigration = System.currentTimeMillis();
        State migrationState = triggerMigrationTask(sourceUri, destUri);
        URI taskUri = UriUtils.buildUri(destUri, migrationState.documentSelfLink);

        this.host.log("Migration task URI=%s", taskUri);

        State finalState = waitForServiceCompletion(sender, taskUri);
        if (TaskStage.FAILED == finalState.taskInfo.stage) {
            String failure = Utils.toJson(finalState.taskInfo.failure);
            this.host.log(Level.WARNING, "Migration failed: %s", failure);
            fail("Migration task failed. failure=" + failure);
        }
        long afterMigration = System.currentTimeMillis();
        this.host.log("Migration took %,d msec", afterMigration - beforeMigration);
        logMemoryUsage();

        this.host.log("Sleep start %d minutes", this.sleepAfterMigrationMinutes);
        TimeUnit.MINUTES.sleep(this.sleepAfterMigrationMinutes);
        this.host.log("Sleep end");


        // verify data:
        long beforeVerify = System.currentTimeMillis();
        verifyData(destUri);
        long afterVerify = System.currentTimeMillis();
        this.host.log("Verifying %,d data took %,d msec", this.totalDataSize, afterVerify - beforeVerify);
        logMemoryUsage();
    }

    private void populateData(URI sourceHostUri) {
        int batch = this.dataBatchSize;
        long total = this.totalDataSize;

        List<Operation> ops = new ArrayList<>();
        for (long i = 0; i < total; i++) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "foo-" + i;
            state.documentSelfLink = state.name;
            ops.add(Operation.createPost(UriUtils.buildUri(sourceHostUri, ExampleODLService.FACTORY_LINK)).setBody(state));

            if (ops.size() % batch == 0) {
                long batchStart = System.currentTimeMillis();
                this.host.getTestRequestSender().sendAndWait(ops);
                long batchEnd = System.currentTimeMillis();
                this.host.log("populating data: i=%,d, took=%d msec", i + 1, batchEnd - batchStart);
                ops.clear();
            }
        }

        // send remaining
        long lastStart = System.currentTimeMillis();
        this.host.getTestRequestSender().sendAndWait(ops);
        long lastEnd = System.currentTimeMillis();
        this.host.log("populating remaining: count=%,d, took=%d msec", ops.size(), lastEnd - lastStart);
    }


    private State triggerMigrationTask(URI sourceHostUri, URI destHostUri) {
        State state = new State();
        state.destinationFactoryLink = ExampleODLService.FACTORY_LINK;
        state.destinationNodeGroupReference = UriUtils.buildUri(destHostUri, ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.sourceFactoryLink = ExampleODLService.FACTORY_LINK;
        state.sourceNodeGroupReference = UriUtils.buildUri(sourceHostUri, ServiceUriPaths.DEFAULT_NODE_GROUP);
        state.maintenanceIntervalMicros = TimeUnit.SECONDS.toMicros(1);
        state.querySpec = new QuerySpecification();
        state.querySpec.resultLimit = this.resultLimit;

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

    private void verifyData(URI destHostUri) {
        int batch = this.dataBatchSize;
        long total = this.totalDataSize;

        List<Operation> ops = new ArrayList<>();
        for (long i = 0; i < total; i++) {
            String selflink = "foo-" + i;
            String path = UriUtils.buildUriPath(ExampleODLService.FACTORY_LINK, selflink);
            ops.add(Operation.createGet(UriUtils.buildUri(destHostUri, path)));

            if (i != 0 && i % batch == 0) {
                long batchStart = System.currentTimeMillis();
                this.host.getTestRequestSender().sendAndWait(ops);
                long batchEnd = System.currentTimeMillis();
                this.host.log("verifying data: host=%s, i=%,d, took=%d msec", destHostUri, i, batchEnd - batchStart);
                ops.clear();
            }
        }

        // send remaining
        long lastStart = System.currentTimeMillis();
        this.host.getTestRequestSender().sendAndWait(ops);
        long lastEnd = System.currentTimeMillis();
        this.host.log("verifying remaining: host=%s, took=%d msec", destHostUri, lastEnd - lastStart);
    }

    private void logMemoryUsage() {
        this.host.log("Memory free:%,d, used:%,d total:%,d, max:%,d",
                Runtime.getRuntime().freeMemory(),
                Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(),
                Runtime.getRuntime().totalMemory(),
                Runtime.getRuntime().maxMemory());
    }
}
