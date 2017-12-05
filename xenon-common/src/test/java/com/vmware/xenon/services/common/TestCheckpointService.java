/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.logging.Level;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.SynchronizationTaskService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.config.TestXenonConfiguration;
import com.vmware.xenon.common.test.VerificationHost;

public class TestCheckpointService extends BasicTestCase {

    private long checkpoint = 0;
    private long checkpointVersion = 0;
    private static final String TEST_FACTORY_LINK = "test";
    private static final String TEST_CHECKPOINT_SELF_LINK = UriUtils.buildUriPath(
            CheckpointService.FACTORY_LINK,
            TEST_FACTORY_LINK);

    private static final long CHECKPOINT_PERIOD = TimeUnit.MILLISECONDS.toMicros(1000);
    private static final long CHECKPOINT_LAG = TimeUnit.MILLISECONDS.toMicros(1000);
    private static final int CHECKPOINT_WINDOW_SIZE = 1000;

    private static final String CHECKPOINT_EXAMPLE_SELF_LINK = UriUtils.buildUriPath(
            ServiceUriPaths.CHECKPOINTS,
            UriUtils.convertPathCharsFromLink(CheckpointExampleService.FACTORY_LINK));

    private Comparator<ExampleService.ExampleServiceState> documentComparator = (d0, d1) -> {
        if (d0.documentUpdateTimeMicros > d1.documentUpdateTimeMicros) {
            return 1;
        }
        return -1;
    };

    private BiPredicate<ExampleService.ExampleServiceState, ExampleService.ExampleServiceState> exampleStateConvergenceChecker = (
            initial, current) -> {
        if (current.name == null) {
            return false;
        }

        return current.name.equals(initial.name);
    };

    public static class CheckpointExampleService extends ExampleService {
        public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/checkpoint-examples";

        /**
         * Create a default factory service that starts instances of this service on POST.
         * This method is optional, {@code FactoryService.create} can be used directly
         */
        public static FactoryService createFactory() {
            FactoryService fs = FactoryService.create(CheckpointExampleService.class);
            fs.setCheckpointPeriod(CHECKPOINT_PERIOD);
            fs.setCheckpointLag(CHECKPOINT_LAG);
            fs.setCheckpointScanWindowSize(CHECKPOINT_WINDOW_SIZE);
            return fs;
        }

        public CheckpointExampleService() {
            super();
        }
    }

    public long serviceCount = 10;
    public int updateCount = 1;
    public int nodeCount = 3;
    public int iterationCount = 1;
    public String exampleFactoryLink = CheckpointExampleService.FACTORY_LINK;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestXenonConfiguration.override(
                SynchronizationTaskService.class,
                "isCheckpointEnabled",
                "true"
        );
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        TestXenonConfiguration.restore();
    }

    public void setUp(int nodeCount) throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(nodeCount);
            this.host.joinNodesAndVerifyConvergence(nodeCount, true);
            this.host.setNodeGroupQuorum(nodeCount);

            NodeGroupService.NodeGroupConfig cfg = new NodeGroupService.NodeGroupConfig();
            cfg.nodeRemovalDelayMicros = TimeUnit.SECONDS.toMicros(1);
            this.host.setNodeGroupConfig(cfg);

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
                s.timestamp = 0L;
                s.factoryLink = TEST_FACTORY_LINK;
                Operation op = Operation.createPost(host, CheckpointService.FACTORY_LINK)
                        .setBody(s);
                this.host.getTestRequestSender().sendAndWait(op);

                host.startFactory(CheckpointExampleService.class, CheckpointExampleService::createFactory);

                host.waitForServiceAvailable(CheckpointExampleService.FACTORY_LINK);
            }
        }
    }

    @Test
    public void testCheckpointIdempotentPost() throws Throwable {
        setUp(1);
        this.host.waitForNodeGroupIsAvailableConvergence();
        VerificationHost h0 = this.host.getPeerHost();
        CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
        s.timestamp = 0L;
        s.factoryLink = "foo/bar";
        String expectedSelfLink = UriUtils.buildUriPath(ServiceUriPaths.CHECKPOINTS, UriUtils.convertPathCharsFromLink(s.factoryLink));
        // start post
        this.host.getTestRequestSender().sendAndWait(Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS)).setBody(s));
        CheckpointService.CheckpointState r = this.host.getTestRequestSender().sendAndWait(Operation.createGet(UriUtils.buildUri(h0, expectedSelfLink)),
                CheckpointService.CheckpointState.class);
        Assert.assertEquals(s.timestamp, r.timestamp);
        Assert.assertEquals(s.factoryLink, r.factoryLink);
        Assert.assertEquals(expectedSelfLink, r.documentSelfLink);
        // idempotent post
        s.timestamp = 1L;
        this.host.getTestRequestSender().sendAndWait(Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS)).setBody(s));
        r = this.host.getTestRequestSender().sendAndWait(Operation.createGet(UriUtils.buildUri(h0, expectedSelfLink)),
                CheckpointService.CheckpointState.class);
        Assert.assertEquals(s.timestamp, r.timestamp);
        Assert.assertEquals(s.factoryLink, r.factoryLink);
        Assert.assertEquals(expectedSelfLink, r.documentSelfLink);
    }

    @Test
    public void testCheckpointIdempotentPostAfterGet() throws Throwable {
        setUp(1);
        this.host.waitForNodeGroupIsAvailableConvergence();
        VerificationHost h0 = this.host.getPeerHost();
        CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
        s.timestamp = 0L;
        s.factoryLink = "foo/bar";
        // since not created get expect fail
        String expectedSelfLink = UriUtils.buildUriPath(ServiceUriPaths.CHECKPOINTS, UriUtils.convertPathCharsFromLink(s.factoryLink));
        this.host.getTestRequestSender().sendAndWaitFailure(Operation.createGet(UriUtils.buildUri(h0, expectedSelfLink)));

        // post one
        this.host.getTestRequestSender().sendAndWait(Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS)).setBody(s));
        CheckpointService.CheckpointState r = this.host.getTestRequestSender().sendAndWait(Operation.createGet(UriUtils.buildUri(h0, expectedSelfLink)),
                CheckpointService.CheckpointState.class);
        Assert.assertEquals(s.timestamp, r.timestamp);
        Assert.assertEquals(s.factoryLink, r.factoryLink);
        Assert.assertEquals(expectedSelfLink, r.documentSelfLink);
    }

    @Test
    public void testCheckpointServiceRestart() throws Throwable {
        setUp(1);
        this.host.waitForNodeGroupIsAvailableConvergence();
        VerificationHost h0 = this.host.getPeerHost();

        Operation get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        CheckpointService.CheckpointState s0 =
                this.host.getTestRequestSender().sendAndWait(get, CheckpointService.CheckpointState.class);
        Assert.assertEquals(TEST_FACTORY_LINK, s0.factoryLink);
        Assert.assertEquals(this.checkpointVersion, s0.documentVersion);
        Assert.assertEquals(this.checkpoint, s0.timestamp.longValue());
        // stop then restart
        this.host.stopHostAndPreserveState(h0);
        h0.setPort(0);
        VerificationHost.restartStatefulHost(h0, false);
        this.host.addPeerNode(h0);
        this.host.waitForNodeGroupIsAvailableConvergence();

        // patch, start service on demand
        CheckpointService.CheckpointState s1 = new CheckpointService.CheckpointState();
        s1.factoryLink = TEST_FACTORY_LINK;
        s1.timestamp = ++this.checkpoint;
        ++this.checkpointVersion;
        Operation post = Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS))
                .setBody(s1);
        this.host.getTestRequestSender().sendAndWait(post);

        get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        s0 = this.host.getTestRequestSender().sendAndWait(get, CheckpointService.CheckpointState.class);
        Assert.assertEquals(TEST_FACTORY_LINK, s0.factoryLink);
        Assert.assertEquals(this.checkpointVersion, s0.documentVersion);
        Assert.assertEquals(this.checkpoint, s0.timestamp.longValue());
    }

    @Test
    public void testCheckpointServiceUpdate() throws Throwable {
        setUp(1);
        this.host.waitForNodeGroupIsAvailableConvergence();
        VerificationHost h0 = this.host.getPeerHost();

        CheckpointService.CheckpointState state = new CheckpointService.CheckpointState();
        long oldCheckpoint = this.checkpoint++;
        state.factoryLink = TEST_FACTORY_LINK;
        state.timestamp = oldCheckpoint;
        // update with oldCheckpoint, expect the same version and checkpoint
        Operation post = Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS))
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(post);
        Operation get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        CheckpointService.CheckpointState s0 =
                this.host.getTestRequestSender().sendAndWait(get, CheckpointService.CheckpointState.class);
        Assert.assertEquals(0L, s0.documentVersion);
        Assert.assertEquals(0L, s0.timestamp.longValue());

        // update with advanced checkpoint, expect updated version and checkpoint
        state.timestamp = this.checkpoint;
        post = Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS))
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(post);
        get = Operation.createGet(UriUtils.buildUri(h0, TEST_CHECKPOINT_SELF_LINK));
        s0 = this.host.getTestRequestSender().sendAndWait(get, CheckpointService.CheckpointState.class);
        Assert.assertEquals(1L, s0.documentVersion);
        Assert.assertEquals(this.checkpoint, s0.timestamp.longValue());
    }

    @Test
    public void testCheckpointServiceMultiNode() throws Throwable {
        int nodeCount = 3;
        setUp(nodeCount);
        this.host.setNodeGroupQuorum(nodeCount - 1);
        this.host.waitForNodeGroupConvergence();
        VerificationHost h0 = this.host.getPeerHost();
        CheckpointService.CheckpointState state = new CheckpointService.CheckpointState();
        long oldCheckpoint = this.checkpoint++;
        state.factoryLink = TEST_FACTORY_LINK;
        state.timestamp = this.checkpoint;
        // update local check point of h0, no propagation to {h1, h2}
        Operation post = Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS))
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(post);

        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            Operation op = Operation.createGet(UriUtils.buildUri(h, TEST_CHECKPOINT_SELF_LINK));
            CheckpointService.CheckpointState s = this.host.getTestRequestSender().sendAndWait(op, CheckpointService.CheckpointState.class);
            Assert.assertEquals(h.getId() == h0.getId() ? this.checkpoint : oldCheckpoint, s.timestamp.longValue());
        }

        // stop h0 and preserve checkpoint state
        this.host.stopHostAndPreserveState(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP,
                this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();

        // update checkpoint within {h1, h2}
        oldCheckpoint = this.checkpoint++;
        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
            s.timestamp = this.checkpoint;
            s.factoryLink = TEST_FACTORY_LINK;
            Operation op = Operation.createPost(UriUtils.buildUri(h, ServiceUriPaths.CHECKPOINTS))
                    .setBody(s);
            this.host.getTestRequestSender().sendAndWait(op);
        }
        // restart h0
        h0.start();
        h0.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        this.host.addPeerNode(h0);
        this.host.waitForNodeGroupConvergence();

        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            Operation op = Operation.createGet(UriUtils.buildUri(h, TEST_CHECKPOINT_SELF_LINK));
            CheckpointService.CheckpointState s =
                    this.host.getTestRequestSender().sendAndWait(op, CheckpointService.CheckpointState.class);
            Assert.assertEquals(h.getId() != h0.getId() ? this.checkpoint : oldCheckpoint, s.timestamp.longValue());
        }
    }

    /**
     * update check point by manually triggered check point task
     * @throws Throwable
     */
    @Test
    public void verifyCheckpointConvergence() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);
        ExampleService.ExampleServiceState lastUpdateExampleState;

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
            lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
            verifyCheckpoints(lastUpdateExampleState.documentUpdateTimeMicros);
        }

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        verifyCheckpoints(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    @Test
    public void checkpointBasedSynchronization() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount);
        this.host.waitForNodeGroupConvergence();
        long expectedCheckpoint;
        long expectedMaxHitTime;
        long expectedMinMissTime;
        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);
        Map<String, ExampleService.ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        ExampleService.ExampleServiceState lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        expectedMinMissTime = expectedMaxHitTime = expectedCheckpoint = lastUpdateExampleState.documentUpdateTimeMicros;
        // check point convergence
        verifyCheckpoints(expectedCheckpoint);

        // stop h0 with preserved index
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHostAndPreserveState(h0);
        h0.setPort(0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.host.getPeerCount());

        // no service should be synced check point remain the same
        verifySynchTask(expectedMinMissTime, expectedMaxHitTime, 0);

        // more update after h0 left
        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        expectedMinMissTime = getExampleServices(exampleStates).stream().min(this.documentComparator).get().documentUpdateTimeMicros;

        // restart h0 with preserved index
        restartStatefulHost(h0);

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.host.getPeerCount());

        // latest version should be synch since h0 is back, sync from preserved checkpoint
        verifySynchTask(expectedMinMissTime, expectedMaxHitTime, this.serviceCount);

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        // check point convergence
        verifyCheckpoints(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    /**
     * check point converge and match expected check point
     * @param expectedCheckpoint
     * @throws Throwable
     */
    private void verifyCheckpoints(Long expectedCheckpoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            Long actualCheckpoint = checkpointsConverged(CHECKPOINT_EXAMPLE_SELF_LINK);
            if (actualCheckpoint == null) {
                // non unique check point
                return false;
            }
            if (!actualCheckpoint.equals(expectedCheckpoint)) {
                this.host.log(Level.INFO,"Expected check point %d\nActual check point %d", expectedCheckpoint, actualCheckpoint);
                return false;
            }
            return true;
        });
    }

    private void verifySynchTask(long expectedMinMissTime, long expectedMaxHitTime, long expectedSynchCompletionCount) {
        VerificationHost factoryOwner = this.host.getOwnerPeer(this.exampleFactoryLink, ServiceUriPaths.DEFAULT_NODE_SELECTOR);
        String synchTaskSelflink =
                UriUtils.buildUriPath(SynchronizationTaskService.FACTORY_LINK, UriUtils.convertPathCharsFromLink(this.exampleFactoryLink));
        this.host.waitFor("synch task finish timeout", () -> {
            SynchronizationTaskService.State newState =
                    this.host.getTestRequestSender().sendAndWait(Operation.createGet(factoryOwner, synchTaskSelflink), SynchronizationTaskService.State.class);
            if (!newState.checkpoint.equals(expectedMinMissTime - 1) && !newState.checkpoint.equals(expectedMaxHitTime)) {
                this.host.log(Level.INFO, "actual checkpoint %d, expected checkpoints %d or %d",
                        newState.checkpoint, expectedMinMissTime - 1, expectedMaxHitTime);
                return false;
            }
            if (TaskState.isInProgress(newState.taskInfo)) {
                return false;
            }
            if (newState.synchCompletionCount != expectedSynchCompletionCount) {
                this.host.log(Level.INFO, "actual synchCount %d, expected synchCount %d", newState.synchCompletionCount, expectedSynchCompletionCount);
                return false;
            }
            return true;
        });
    }

    /**
     * check point convergence across peers
     * @param checkpointServiceLink
     * @return
     */
    private Long checkpointsConverged(String checkpointServiceLink) {
        Set<Long> checkpoints = new HashSet<>();
        List<CheckpointService.CheckpointState> states = queryCheckpoints(checkpointServiceLink);
        if (states.size() < this.host.getPeerCount()) {
            this.host.log(Level.INFO, "check point not initialed, expected %d actual %d",
                    this.host.getPeerCount(), states.size());
            return null;
        }
        states.stream().forEach(s -> checkpoints.add(s.timestamp));
        if (checkpoints.size() > 1) {
            this.host.log(Level.INFO, "check point not converged %s",
                    Utils.toJson(checkpoints));
            return null;
        }
        return checkpoints.iterator().next();
    }

    private void updateExampleServices(List<ExampleService.ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleService.ExampleServiceState st : exampleStates) {
            ExampleService.ExampleServiceState s = new ExampleService.ExampleServiceState();
            s.counter =  ++st.counter;
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation patch = Operation.createPatch(serviceUri).setBody(s);
            ops.add(patch);
        }
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    private List<ExampleService.ExampleServiceState> getExampleServices(List<ExampleService.ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleService.ExampleServiceState st : exampleStates) {
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation get = Operation.createGet(serviceUri);
            ops.add(get);
        }
        return this.host.getTestRequestSender().sendAndWait(ops, ExampleService.ExampleServiceState.class);
    }

    private void restartStatefulHost(VerificationHost h) throws Throwable {
        VerificationHost.restartStatefulHost(h, false);
        this.host.addPeerNode(h);
        h.startFactory(CheckpointExampleService.class, CheckpointExampleService::createFactory);
        h.waitForServiceAvailable(CheckpointExampleService.FACTORY_LINK);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
    }

    private List<CheckpointService.CheckpointState> queryCheckpoints(String checkpointServiceLink) {
        QueryTask.Query booleanClause = QueryTask.Query.Builder.create()
                .addKindFieldClause(CheckpointService.CheckpointState.class)
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, checkpointServiceLink)
                .build();
        QueryTask task = QueryTask.Builder.createDirectTask()
                .setQuery(booleanClause)
                .addOption(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT)
                .build();
        List<Operation> ops = new ArrayList<>();
        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            ops.add(Operation.createPost(h, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS).setBody(task).setReferer(this.host.getUri()));
        }
        List<QueryTask> tasks = this.host.getTestRequestSender().sendAndWait(ops, QueryTask.class);
        return tasks.stream().filter(t -> t.results.documentCount.equals(1L))
                .map(t -> t.results.documents.values().iterator().next())
                .map(d -> Utils.fromJson(d, CheckpointService.CheckpointState.class))
                .collect(Collectors.toList());
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }
}