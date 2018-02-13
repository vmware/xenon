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
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.SynchronizationTaskService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.config.TestXenonConfiguration;
import com.vmware.xenon.common.test.VerificationHost;

public class TestCheckpointService extends BasicTestCase {

    private static final String TEST_FACTORY_LINK = "/test";
    private static final String TEST_CHECKPOINT_SELF_LINK = UriUtils.buildUriPath(
            CheckpointService.FACTORY_LINK,
            TEST_FACTORY_LINK);

    private static final int SYNCH_SCHEDULE_PERIOD_SECONDS = 3;
    private static final String CHECKPOINT_EXAMPLE_SELF_LINK = UriUtils.buildUriPath(
            ServiceUriPaths.CHECKPOINTS,
            UriUtils.convertPathCharsFromLink(ExampleService.FACTORY_LINK));

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

    public long serviceCount = 10;
    public int updateCount = 1;
    public int nodeCount = 3;
    public int iterationCount = 1;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestXenonConfiguration.override(
                SynchronizationTaskService.class,
                "isCheckpointEnabled",
                "true"
        );
        TestXenonConfiguration.override(
                SynchronizationTaskService.class,
                "schedulePeriodSeconds",
                String.valueOf(SYNCH_SCHEDULE_PERIOD_SECONDS)
        );
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        TestXenonConfiguration.restore();
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }

    public void setUp(int nodeCount) throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(nodeCount);

            NodeGroupService.NodeGroupConfig cfg = new NodeGroupService.NodeGroupConfig();
            cfg.nodeRemovalDelayMicros = TimeUnit.SECONDS.toMicros(1);
            this.host.setNodeGroupConfig(cfg);

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                        VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
                CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
                s.timestamp = 0L;
                s.factoryLink = TEST_FACTORY_LINK;
                Operation op = Operation.createPost(host, CheckpointService.FACTORY_LINK)
                        .setBody(s);
                this.host.getTestRequestSender().sendAndWait(op);
            }
            this.host.joinNodesAndVerifyConvergence(nodeCount, true);
            this.host.waitForReplicatedFactoryServiceAvailable(
                    UriUtils.buildUri(this.host.getPeerHost(), ExampleService.FACTORY_LINK));
        }
    }

    private void setUpLocalPeerHost() throws Throwable {
        VerificationHost host = this.host.setUpLocalPeerHost(0, VerificationHost.FAST_MAINT_INTERVAL_MILLIS, null, null);
        host.setPeerSynchronizationEnabled(true);
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
    }

    @Test
    public void testCheckpointIdempotentPost() throws Throwable {
        setUp(1);
        this.host.waitForNodeGroupIsAvailableConvergence();
        VerificationHost h0 = this.host.getPeerHost();
        CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
        s.timestamp = 0L;
        s.factoryLink = "/foo/bar";
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
    public void testCheckpointServiceNotSynchronized() throws Throwable {
        int nodeCount = 3;
        long checkpoint = 0;
        setUp(nodeCount);
        this.host.setNodeGroupQuorum(nodeCount - 1);
        this.host.waitForNodeGroupConvergence();
        VerificationHost h0 = this.host.getPeerHost();
        CheckpointService.CheckpointState state = new CheckpointService.CheckpointState();
        long oldCheckpoint = checkpoint++;
        state.factoryLink = TEST_FACTORY_LINK;
        state.timestamp = checkpoint;
        // update local check point of h0, no propagation to {h1, h2}
        Operation post = Operation.createPost(UriUtils.buildUri(h0, ServiceUriPaths.CHECKPOINTS))
                .setBody(state);
        this.host.getTestRequestSender().sendAndWait(post);

        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            Operation op = Operation.createGet(UriUtils.buildUri(h, TEST_CHECKPOINT_SELF_LINK));
            CheckpointService.CheckpointState s = this.host.getTestRequestSender().sendAndWait(op, CheckpointService.CheckpointState.class);
            Assert.assertEquals(h.getId().equals(h0.getId()) == true ? checkpoint : oldCheckpoint, s.timestamp.longValue());
        }

        // stop h0 and preserve checkpoint state
        this.host.stopHostAndPreserveState(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP,
                this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();

        // update checkpoint within {h1, h2}
        oldCheckpoint = checkpoint++;
        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
            s.timestamp = checkpoint;
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
            Assert.assertEquals(!h.getId().equals(h0.getId()) ? checkpoint : oldCheckpoint, s.timestamp.longValue());
        }
    }

    /**
     * verify checkpoint converge when service update
     * @throws Throwable
     */
    @Test
    public void verifyCheckpointConvergence() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        ExampleService.ExampleServiceState lastUpdateExampleState;

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    @Test
    public void checkpointBasedSynchronizationServiceUpdate() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        Map<String, ExampleService.ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        ExampleService.ExampleServiceState lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        // check point convergence
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);

        // stop h0 with preserved index
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHostAndPreserveState(h0);
        h0.setPort(0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();

        // more update after h0 left
        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);

        // restart h0 with preserved index
        restartStatefulHost(h0);

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                this.updateCount, this.host.getPeerCount());

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        // check point convergence
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    @Test
    public void checkpointBasedSynchronizationServiceCreation() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        Map<String, ExampleService.ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));

        ExampleService.ExampleServiceState lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        // check point convergence
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);

        // stop h0 with preserved index
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHostAndPreserveState(h0);
        h0.setPort(0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();

        // more services after h0 left
        List<ExampleService.ExampleServiceState> additionalExampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        additionalExampleStates.stream().forEach(s -> exampleStatesMap.put(s.documentSelfLink, s));
        exampleStates.addAll(additionalExampleStates);

        additionalExampleStates = getExampleServices(additionalExampleStates);
        // h0 missed all additional service
        lastUpdateExampleState = additionalExampleStates.stream().max(this.documentComparator).get();
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);
        // restart h0 with preserved index
        restartStatefulHost(h0);

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.host.getPeerCount());

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        // check point convergence
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    @Test
    public void nodeJoinWithEmptyIndex() throws Throwable {
        setUp(this.nodeCount - 1);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services within two nodes
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        Map<String, ExampleService.ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));

        updateExampleServices(exampleStates);
        // one node join with empty index, synch all services, regardless of checkpoint
        setUpLocalPeerHost();
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.host.getPeerCount());

        // checkpoints converge after synchronization
        ExampleService.ExampleServiceState lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        verifyCheckpointsConvergence(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    /**
     * check point converge and match expected check point
     * @param expectedCheckpoint
     * @throws Throwable
     */
    private void verifyCheckpointsConvergence(Long expectedCheckpointLowerLimit) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            Long actualCheckpoint = checkpointsConverged(CHECKPOINT_EXAMPLE_SELF_LINK);
            if (actualCheckpoint == null) {
                // non unique check point
                return false;
            }
            if (actualCheckpoint < expectedCheckpointLowerLimit) {
                this.host.log(Level.INFO,"Expected checkpoint lower limit: %d\n. Actual checkpoint: %d",
                        expectedCheckpointLowerLimit, actualCheckpoint);
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
        h.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        h.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.host.addPeerNode(h);
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
}