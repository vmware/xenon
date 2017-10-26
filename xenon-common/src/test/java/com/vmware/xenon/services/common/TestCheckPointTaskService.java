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
import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestCheckPointTaskService extends BasicTestCase {

    private static final String CUSTOM_CHECKPOINT_SELF_LINK = "/check-point/custom";
    private static final QueryTask.Query exampleQuery = QueryTask.Query.Builder.create()
            .addKindFieldClause(ExampleServiceState.class)
            .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                    ExampleService.FACTORY_LINK + UriUtils.URI_PATH_CHAR + UriUtils.URI_WILDCARD_CHAR, QueryTask.QueryTerm.MatchType.WILDCARD)
            .build();

    private static final QueryTask.Query customExampleQuery = QueryTask.Query.Builder.create()
            .addKindFieldClause(ExampleServiceState.class)
            .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                    CustomExampleFactoryService.SELF_LINK + UriUtils.URI_PATH_CHAR + UriUtils.URI_WILDCARD_CHAR, QueryTask.QueryTerm.MatchType.WILDCARD)
            .build();
    /**
     * test with small scale data, set short life time for task
     */
    public static long initialTaskLifeTime = TimeUnit.SECONDS.toMicros(10);
    public static long checkPointLag = TimeUnit.MILLISECONDS.toMicros(10);
    public static long schedulePeriod = TimeUnit.MILLISECONDS.toMicros(100);
    public static int checkPointScanWindowSize = 1000;
    public long serviceCount = 10;
    public int updateCount = 3;
    public int nodeCount = 3;
    public int iterationCount = 1;
    public String exampleFactoryLink = CustomExampleFactoryService.SELF_LINK;

    private Comparator<ExampleServiceState> documentComparator = (d0, d1) -> {
        if (d0.documentUpdateTimeMicros > d1.documentUpdateTimeMicros) {
            return 1;
        }
        return -1;
    };

    private BiPredicate<ExampleServiceState, ExampleServiceState> exampleStateConvergenceChecker = (
            initial, current) -> {
        if (current.name == null) {
            return false;
        }

        return current.name.equals(initial.name);
    };

    /**
     * A factory schedule check point task for its child services
     */
    public static class CustomExampleFactoryService extends FactoryService {
        public static final String SELF_LINK = "/test/custom-examples";

        private String inProgressTaskLink;
        private Long taskExpireation = 0L;
        private final CheckPointTaskService.State checkPointTask = validateTaskState(customExampleQuery, true);

        public CustomExampleFactoryService() {
            super(ExampleServiceState.class);
            this.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
            this.setMaintenanceIntervalMicros(schedulePeriod);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            ExampleService s = new ExampleService();
            return s;
        }

        @Override
        public void handleRequest(Operation op) {
            if (!op.isNotification()) {
                super.handleRequest(op);
                return;
            }
            handleCheckPointTaskNotification(op);
        }

        private void handleCheckPointTaskNotification(Operation notifyOp) {
            notifyOp.complete();
            if (!notifyOp.hasBody()) {
                return;
            }
            CheckPointTaskService.State s = notifyOp.getBody(CheckPointTaskService.State.class);
            if (s == null) {
                return;
            }
            logInfo("%s %s %s", s.documentSelfLink, s.taskInfo.stage, s.subStage);
            if (!TaskState.isInProgress(s.taskInfo)) {
                if (TaskState.isFinished(s.taskInfo)) {
                    collectTaskStat(s);
                }
                synchronized (this) {
                    if (this.inProgressTaskLink == null) {
                        // task should have expired
                        return;
                    }
                    if (this.inProgressTaskLink.equals(s.documentSelfLink)) {
                        this.inProgressTaskLink = null;
                    }
                }
            }
        }

        private void collectTaskStat(CheckPointTaskService.State state) {
            if (state.start == null) {
                return;
            }
            long cost = state.documentUpdateTimeMicros - state.start;
            logInfo("%s time cost: %d millis", state.documentSelfLink, cost / 1000);
            if (state.totalDocsCount != null && state.totalDocsCount > 0) {
                int hitCount = state.totalDocsCount - state.missDocs.size();
                logInfo("document hit rate %d/%d", hitCount, state.totalDocsCount);
            }
            if (!state.missDocs.isEmpty()) {
                logInfo("on demand sync success rate %d/%d ", state.synchCompletionCount, state.missDocs.size());
                // worst case time cost
                this.checkPointTask.taskLifetime = cost;
            }
        }

        /**
         * factory launch check point task in period
         * @param maintOp
         */
        @Override
        public void handlePeriodicMaintenance(Operation maintOp) {
            // Factory is available if local node is owner
            if (!isAvailable()) {
                maintOp.complete();
                return;
            }

            if (this.taskExpireation < Utils.getNowMicrosUtc()) {
                // task expired, double the time to live
                this.checkPointTask.taskLifetime *= 2;
                synchronized (this) {
                    this.inProgressTaskLink = null;
                }
            }

            // whether a check point task in progress
            if (this.inProgressTaskLink != null) {
                maintOp.complete();
                return;
            }
            // create new task and subscribe
            scheduleTaskAsOwner(maintOp);
        }

        private void scheduleTaskAsOwner(Operation maintOp) {
            Operation.createPost(getHost(), CheckPointTaskService.FACTORY_LINK)
                    .setBody(this.checkPointTask)
                    .setReferer(getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            logInfo("Failed to create task");
                            maintOp.fail(e);
                            return;
                        }
                        CheckPointTaskService.State s = o.getBody(CheckPointTaskService.State.class);
                        synchronized (this) {
                            this.inProgressTaskLink = s.documentSelfLink;
                        }
                        this.taskExpireation = s.documentExpirationTimeMicros;
                        logInfo("task %s created", s.documentSelfLink);
                        maintOp.complete();
                        subscribeTask(s.documentSelfLink);
                    }).sendWith(this);
        }

        private void subscribeTask(String taskSelfLink) {
            Operation post = Operation
                    .createPost(getHost(), taskSelfLink)
                    .setReferer(getHost().getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            logInfo("subscribe to %s failed", taskSelfLink);
                        }
                    });

            ServiceSubscriptionState.ServiceSubscriber sr = ServiceSubscriptionState.ServiceSubscriber
                    .create(true)
                    .setUsePublicUri(true)
                    .setSubscriberReference(this.getUri());

            getHost().startSubscriptionService(post, this, sr);
        }

        // local node is the owner to schedule check point task
        public void synchronizeChildServicesAsOwner(Operation maintOp, long membershipUpdateTimeMicros) {
            maintOp.setCompletion((o, e) -> {
                if (e != null) {
                    logInfo(e.getMessage());
                }
                setAvailable(true);
                maintOp.complete();
            });
            scheduleTaskAsOwner(maintOp);
        }
    }

    public void setUp(int nodeCount) throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(nodeCount);
            this.host.joinNodesAndVerifyConvergence(nodeCount, true);
            this.host.setNodeGroupQuorum(nodeCount);

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                host.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS.toMicros(10));
                CheckPointService.CheckPointState s
                        = new CheckPointService.CheckPointState();
                s.checkPoint = 0L;
                Operation post = Operation.createPost(UriUtils.buildUri(host, CUSTOM_CHECKPOINT_SELF_LINK))
                        .setBody(s);
                CheckPointService cps = new CheckPointService();
                cps.setCacheClearDelayMicros(TimeUnit.DAYS.toMicros(1));
                host.startService(post, cps);
                host.startFactory(new CheckPointTaskService());
                host.waitForServiceAvailable(CUSTOM_CHECKPOINT_SELF_LINK);
                host.waitForServiceAvailable(CheckPointTaskService.FACTORY_LINK);
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
                if (this.exampleFactoryLink.equals(CustomExampleFactoryService.SELF_LINK)) {
                    host.startServiceAndWait(CustomExampleFactoryService.class,
                            CustomExampleFactoryService.SELF_LINK);
                }
            }
        }
    }

    @Test
    public void testTaskCreation() throws Throwable {
        setUp(this.nodeCount);
        CheckPointTaskService.State initialState = new CheckPointTaskService.State();
        initialState.checkPointLag = 0L;
        initialState.query = new QueryTask.Query();
        Operation post = Operation.createPost(this.host.getPeerHost(), CheckPointTaskService.FACTORY_LINK)
                .setBody(initialState);
        TestRequestSender.FailureResponse fr = this.host.getTestRequestSender().sendAndWaitFailure(post);
        Assert.assertEquals(Operation.STATUS_CODE_BAD_REQUEST, fr.op.getStatusCode());
        initialState = validateTaskState(exampleQuery, false);
        post = Operation.createPost(this.host.getPeerHost(), CheckPointTaskService.FACTORY_LINK)
                .setBody(initialState);
        CheckPointTaskService.State result = this.host.getTestRequestSender().sendAndWait(post, CheckPointTaskService.State.class);
        Assert.assertEquals(initialState.checkPointLag.longValue(), result.checkPointLag.longValue());
        Assert.assertEquals(initialState.checkPointScanWindowSize.longValue(), result.checkPointScanWindowSize.longValue());
        Assert.assertEquals(initialState.checkPointServiceLink, result.checkPointServiceLink);
    }

    /**
     * Manually start check point task, verify whether check point
     * catch up with service update
     * @throws Throwable
     */
    @Test
    public void verifyCheckPointWithDocumentUpdate() throws Throwable {
        String originalExampleFactoryLink = this.exampleFactoryLink;
        this.exampleFactoryLink = ExampleService.FACTORY_LINK;
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);
        ExampleServiceState lastUpdateExampleState;

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
            lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
            // check whether check point catch up the latest update
            updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);
        }

        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);
        this.exampleFactoryLink = originalExampleFactoryLink;
    }

    /**
     * Manually start check point tasks, verify whether check point
     * catch up with service update when node left (index removed)
     * and later a new node join (empty index)
     * @throws Throwable
     */
    @Test
    public void verifyCheckPointWithNodeLeftThenJoin() throws Throwable {
        String originalExampleFactoryLink = this.exampleFactoryLink;
        this.exampleFactoryLink = ExampleService.FACTORY_LINK;
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);
        Map<String, ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));
        // update after node left

        for (int i = 0; i < this.updateCount; i++) {
            updateExampleServices(exampleStates);
        }

        exampleStates = getExampleServices(exampleStates);
        ExampleServiceState lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);
        // node left
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHost(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount - 1);

        // find the latest update time after synchronization
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);

        // node join
        setUpLocalPeerHost();
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount);

        // find the latest update time after synchronization
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);
        this.exampleFactoryLink = originalExampleFactoryLink;
    }
    /**
     * Manually start check point tasks, verify whether check point
     * catch up with service update when node left, preserve index
     * and rejoin.
     * @throws Throwable
     */
    @Test
    public void verifyCheckPointWithNodeRestart() throws Throwable {
        String originalExampleFactoryLink = this.exampleFactoryLink;
        this.exampleFactoryLink = ExampleService.FACTORY_LINK;
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);
        Map<String, ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));
        for (int i = 0; i < this.updateCount; i++) {
            // update
            updateExampleServices(exampleStates);
        }
        ExampleServiceState lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);
        // stop node, preserve index
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHostAndPreserveState(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount - 1);

        lastUpdateExampleState = getExampleServices(exampleStates).stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);
        // restart node with preserved index
        restartHost(h0);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount - 1);
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(exampleQuery, false, lastUpdateExampleState.documentUpdateTimeMicros);
        this.exampleFactoryLink = originalExampleFactoryLink;
    }

    /**
     * CustomExampleFactoryService schedule check point task
     * which post on demand sync request if any miss docs
     * @throws Throwable
     */
    @Test
    public void verifyNodeLeftThenJoinSynchOnDemand() throws Throwable {
        setUp(3);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        List<ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);

        // stop one node
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHost(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();

        // more update after node left
        for (int i = 0; i < this.updateCount; i++) {
            updateExampleServices(exampleStates);
        }

        // node join
        setUpLocalPeerHost();
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        // more services after new node join
        exampleStates.addAll(this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink));
        Map<String, ExampleServiceState> exampleStatesMap = exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount);
        if (this.exampleFactoryLink.equals(CustomExampleFactoryService.SELF_LINK)) {
            exampleStates = getExampleServices(exampleStates);
            ExampleService.ExampleServiceState lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
            verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
        }
    }

    /**
     * Node restart with preserved index
     * @throws Throwable
     */
    @Test
    public void NodeRestartSynchOnDemand() throws Throwable {
        setUp(3);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        List<ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink);

        for (int i = 0; i < this.iterationCount; i++) {
            // stop one node, preserve index - same as network partition
            VerificationHost h0 = this.host.getPeerHost();
            this.host.stopHostAndPreserveState(h0);
            this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
            this.host.waitForNodeGroupConvergence();

            // more update after node left
            for (int j = 0; j < this.updateCount; j++) {
                updateExampleServices(exampleStates);
            }

            // restart node with preserved index
            restartHost(h0);
            this.host.joinNodesAndVerifyConvergence(this.nodeCount - 1);
            // more services after new node join
            exampleStates.addAll(this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.exampleFactoryLink));
        }
        Map<String, ExampleServiceState> exampleStatesMap = exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount);
        if (this.exampleFactoryLink.equals(CustomExampleFactoryService.SELF_LINK)) {
            exampleStates = getExampleServices(exampleStates);
            ExampleService.ExampleServiceState lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
            verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
        }
    }

    /**
     * Node join group one by one. Ingest data to the first node
     * documents should propagate by synchronization
     * @throws Throwable
     */
    @Test
    public void NodeSequentialJoinSynchOnDemand() throws Throwable {
        // only one node started
        setUp(1);
        this.host.setNodeGroupQuorum(1);
        this.host.waitForNodeGroupConvergence();
        VerificationHost h0 = this.host.getPeerHost();
        // ingest documents to h0 only
        List<ExampleServiceState> exampleStates =
                this.host.createExampleServices(h0, this.serviceCount, null, this.exampleFactoryLink);
        Map<String, ExampleServiceState> exampleStatesMap = exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));

        // all late join nodes should get synced
        for (int i = 1; i < this.nodeCount; i++) {
            setUpLocalPeerHost();
            // update quorum when new node join
            this.host.setNodeGroupQuorum(i + 1);
            this.host.joinNodesAndVerifyConvergence(i + 1);
            // update after node join
            updateExampleServices(exampleStates);
        }
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(this.exampleFactoryLink),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount);
        if (this.exampleFactoryLink.equals(CustomExampleFactoryService.SELF_LINK)) {
            exampleStates = getExampleServices(exampleStates);
            ExampleService.ExampleServiceState lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
            verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
        }
    }

    private void updateAndVerifyCheckPoints(QueryTask.Query q, boolean onDemandSync, long expectedCheckPoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            startAndWaitCheckPointTaskService(q, onDemandSync);
            Long actualCheckPoint = checkPointsConverged(CUSTOM_CHECKPOINT_SELF_LINK);
            if (actualCheckPoint == null) {
                // non unique check point
                return false;
            }
            if (actualCheckPoint != expectedCheckPoint) {
                this.host.log(Level.INFO,"Expected check point %d\nActual check point %d", expectedCheckPoint, actualCheckPoint);
                return false;
            }
            return true;
        });
    }

    private void verifyCheckPoints(Long expectedCheckPoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            Long actualCheckPoint = checkPointsConverged(CUSTOM_CHECKPOINT_SELF_LINK);
            if (actualCheckPoint == null) {
                // non unique check point
                return false;
            }
            if (!actualCheckPoint.equals(expectedCheckPoint)) {
                this.host.log(Level.INFO,"Expected check point %d\nActual check point %d", expectedCheckPoint, actualCheckPoint);
                return false;
            }
            return true;
        });
    }

    private static CheckPointTaskService.State validateTaskState(QueryTask.Query query, boolean onDemandSync) {
        CheckPointTaskService.State s = new CheckPointTaskService.State();
        s.query = query;
        s.checkPointServiceLink = CUSTOM_CHECKPOINT_SELF_LINK;
        s.nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
        s.checkPointLag = checkPointLag;
        s.checkPointScanWindowSize = checkPointScanWindowSize;
        s.onDemandSync = onDemandSync;
        s.taskLifetime = initialTaskLifeTime;
        return s;
    }

    private void startAndWaitCheckPointTaskService(QueryTask.Query query, boolean onDemandSync) {
        CheckPointTaskService.State state = validateTaskState(query, onDemandSync);
        VerificationHost host =
                this.host.getInProcessHostMap().values().stream().filter(h -> !h.isStopping()).iterator().next();
        Operation post = Operation.createPost(host, CheckPointTaskService.FACTORY_LINK)
                .setBody(state);
        state = this.host.getTestRequestSender().sendAndWait(post, CheckPointTaskService.State.class);
        String link = state.documentSelfLink;
        this.host.waitFor("check point task timeout", () -> {

            Operation get = Operation.createGet(UriUtils.buildUri(this.host.getPeerHost(), link));
            CheckPointTaskService.State s =
                    this.host.getTestRequestSender().sendAndWait(get, CheckPointTaskService.State.class);
            if (s.taskInfo == null || s.taskInfo.stage == null) {
                return false;
            }
            if (!(s.taskInfo.stage.ordinal() > TaskState.TaskStage.STARTED.ordinal())) {
                return false;
            }
            return true;
        });
    }

    private Long checkPointsConverged(String checkPointServiceLink) {
        Set<Long> checkPoints = new HashSet<>();
        List<Operation> ops = new ArrayList<>();
        for (ServiceHost h : this.host.getInProcessHostMap().values()) {
            ops.add(Operation.createGet(UriUtils.buildUri(h, checkPointServiceLink)));
        }
        List<CheckPointService.CheckPointState> states =
                this.host.getTestRequestSender().sendAndWait(ops, CheckPointService.CheckPointState.class);
        states.stream().forEach(s -> checkPoints.add(s.checkPoint));
        if (checkPoints.size() > 1) {
            this.host.log(Level.INFO, "check point not converged %s",
                    Utils.toJson(checkPoints));
            return null;
        }
        return checkPoints.iterator().next();
    }

    private void updateExampleServices(List<ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleServiceState st : exampleStates) {
            ExampleServiceState s = new ExampleServiceState();
            s.counter =  ++st.counter;
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation patch = Operation.createPatch(serviceUri).setBody(s);
            ops.add(patch);
        }
        this.host.getTestRequestSender().sendAndWait(ops);
    }

    private List<ExampleServiceState> getExampleServices(List<ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleServiceState st : exampleStates) {
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation get = Operation.createGet(serviceUri);
            ops.add(get);
        }
        return this.host.getTestRequestSender().sendAndWait(ops, ExampleServiceState.class);
    }

    private VerificationHost setUpLocalPeerHost() throws Throwable {
        VerificationHost host = this.host.setUpLocalPeerHost(0, VerificationHost.FAST_MAINT_INTERVAL_MILLIS, null, null);
        host.setPeerSynchronizationEnabled(true);
        host.setServiceCacheClearDelayMicros(TimeUnit.SECONDS.toMicros(1));
        CheckPointService.CheckPointState s
                = new CheckPointService.CheckPointState();
        s.checkPoint = 0L;
        Operation post = Operation.createPost(UriUtils.buildUri(host, CUSTOM_CHECKPOINT_SELF_LINK))
                .setBody(s);
        CheckPointService cps = new CheckPointService();
        cps.setCacheClearDelayMicros(TimeUnit.DAYS.toMicros(1));
        host.startService(post, cps);
        host.startFactory(new CheckPointTaskService());
        host.waitForServiceAvailable(CUSTOM_CHECKPOINT_SELF_LINK);
        host.waitForServiceAvailable(CheckPointTaskService.FACTORY_LINK);
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        if (this.exampleFactoryLink.equals(CustomExampleFactoryService.SELF_LINK)) {
            host.startServiceAndWait(CustomExampleFactoryService.class,
                    CustomExampleFactoryService.SELF_LINK);
        }
        this.host.addPeerNode(host);
        return host;
    }

    private VerificationHost restartHost(VerificationHost host) throws Throwable {
        host.setPort(0);
        VerificationHost.restartStatefulHost(host, false);
        host.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS.toMicros(10));
        // restart with null body, reuse the preserved check point
        Operation post = Operation.createPost(UriUtils.buildUri(host, CUSTOM_CHECKPOINT_SELF_LINK));
        CheckPointService cps = new CheckPointService();
        cps.setCacheClearDelayMicros(TimeUnit.DAYS.toMicros(1));
        host.startService(post, cps);
        host.startFactory(new CheckPointTaskService());
        host.waitForServiceAvailable(CUSTOM_CHECKPOINT_SELF_LINK);
        host.waitForServiceAvailable(CheckPointTaskService.FACTORY_LINK);
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        if (this.exampleFactoryLink.equals(CustomExampleFactoryService.SELF_LINK)) {
            host.startServiceAndWait(CustomExampleFactoryService.class,
                    CustomExampleFactoryService.SELF_LINK);
        }
        this.host.addPeerNode(host);
        return host;
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }
}
