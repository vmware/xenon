package com.vmware.xenon.services.common;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

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

public class TestCheckPointTaskService extends BasicTestCase {

    private static final String CUSTOM_CHECKPOINT_SELF_LINK = "/check-point/custom";
    private static final String CUSTOM_TASK_SCHEDULER_SELF_LINK = "/task-scheduler/custom";
    private final QueryTask.Query exampleQuery = QueryTask.Query.Builder.create()
            .addKindFieldClause(ExampleService.ExampleServiceState.class)
            .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                    ExampleService.FACTORY_LINK + UriUtils.URI_PATH_CHAR + UriUtils.URI_WILDCARD_CHAR, QueryTask.QueryTerm.MatchType.WILDCARD)
            .build();

    public long checkPointLag = TimeUnit.MILLISECONDS.toMicros(10);
    public long schedulePeriod = TimeUnit.MILLISECONDS.toMicros(100);
    public long serviceCount = 10;
    public int checkPointScanWindowSize = 3;
    public int updateCount = 3;
    public int nodeCount = 3;
    public int iterationCount = 1;


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

    public void setUp(int nodeCount) throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(nodeCount);
            this.host.joinNodesAndVerifyConvergence(nodeCount, true);
            this.host.setNodeGroupQuorum(nodeCount);

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                CheckPointService.CheckPointState s
                        = new CheckPointService.CheckPointState();
                s.checkPoint = 0L;
                Operation post = Operation.createPost(UriUtils.buildUri(host, CUSTOM_CHECKPOINT_SELF_LINK))
                        .setBody(s);
                host.startService(post, new CheckPointService());
                host.startFactory(new CheckPointTaskService());
                host.waitForServiceAvailable(CUSTOM_CHECKPOINT_SELF_LINK);
                host.waitForServiceAvailable(CheckPointTaskService.FACTORY_LINK);
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
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
        initialState = validateTaskState(this.exampleQuery);
        post = Operation.createPost(this.host.getPeerHost(), CheckPointTaskService.FACTORY_LINK)
                .setBody(initialState);
        CheckPointTaskService.State result =
            this.host.getTestRequestSender().sendAndWait(post, CheckPointTaskService.State.class);
        Assert.assertEquals(initialState.checkPointLag.longValue(), result.checkPointLag.longValue());
        Assert.assertEquals(initialState.checkPointScanWindowSize.longValue(), result.checkPointScanWindowSize.longValue());
        Assert.assertEquals(initialState.checkPointServiceLink, result.checkPointServiceLink);
    }

    @Test
    public void verifyCheckPointWithDocumentUpdate() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        ExampleService.ExampleServiceState lastUpdateExampleState =
                exampleStates.stream().max(this.documentComparator).get();
        for (int i = 0; i < this.updateCount; i++) {
            // update
            lastUpdateExampleState =
                    updateExampleServices(exampleStates).stream().max(this.documentComparator).get();
            // check whether check point catch up the latest update
            updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);
        }

        for (int i = 0; i < this.updateCount; i++) {
            // update
            lastUpdateExampleState =
                    updateExampleServices(exampleStates).stream().max(this.documentComparator).get();
        }
        updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);
    }

    @Test
    public void verifyCheckPointWithNodeLeftAndJoin() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        Map<String, ExampleService.ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));
        ExampleService.ExampleServiceState lastUpdateExampleState =
                exampleStates.stream().max(this.documentComparator).get();
        // update
        for (int i = 0; i < this.updateCount; i++) {
            // update
            lastUpdateExampleState =
                    updateExampleServices(exampleStates).stream().max(this.documentComparator).get();
        }

        updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);
        // node left
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHost(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount - 1);

        // find the latest update time after synchronization
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);

        // node join
        h0 = setUpLocalPeerHost();
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount);

        // find the latest update time after synchronization
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);
    }

    @Test
    public void verifyCheckPointWithNodeRestart() throws Throwable {
        setUp(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // create example services
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        Map<String, ExampleService.ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));
        ExampleService.ExampleServiceState lastUpdateExampleState =
                exampleStates.stream().max(this.documentComparator).get();
        // update
        for (int i = 0; i < this.updateCount; i++) {
            // update
            lastUpdateExampleState =
                    updateExampleServices(exampleStates).stream().max(this.documentComparator).get();
        }
        updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);
        // stop node, preserve index
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHostAndPreserveState(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount - 1);
        // more update
        for (int i = 0; i < this.updateCount; i++) {
            // update
            lastUpdateExampleState =
                    updateExampleServices(exampleStates).stream().max(this.documentComparator).get();
        }
        updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);
        // restart node with preserved index
        restartHost(h0);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount - 1);
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        updateAndVerifyCheckPoints(this.exampleQuery, lastUpdateExampleState.documentUpdateTimeMicros);
    }

    /**
     * Scheduler start check point task in period
     * @throws Throwable
     */
    @Test
    public void verifyScheduleCheckPointTask() throws Throwable {
        setUp(3);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        this.host.waitForNodeGroupConvergence();

        // start task scheduler
        CheckPointTaskService.State taskTemplate = validateTaskState(this.exampleQuery);
        for (VerificationHost host : this.host.getInProcessHostMap().values()) {
            TaskScheduleService tss =
                    new TaskScheduleService(taskTemplate, CheckPointTaskService.FACTORY_LINK, this.schedulePeriod);
            Operation post = Operation.createPost(UriUtils.buildUri(host, CUSTOM_TASK_SCHEDULER_SELF_LINK));
            host.startService(post, tss);
        }
        List<ExampleService.ExampleServiceState> exampleStates =
                this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, ExampleService.FACTORY_LINK);
        Map<String, ExampleService.ExampleServiceState> exampleStatesMap =
                exampleStates.stream().collect(Collectors.toMap(s -> s.documentSelfLink, s -> s));
        ExampleService.ExampleServiceState lastUpdateExampleState =
                exampleStates.stream().max(this.documentComparator).get();
        verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);

        // stop one node
        VerificationHost h0 = this.host.getPeerHost();
        this.host.stopHost(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence();

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount - 1);
        // more update
        for (int i = 0; i < this.updateCount; i++) {
            // update
            lastUpdateExampleState =
                    updateExampleServices(exampleStates).stream().max(this.documentComparator).get();
        }

        verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);

        // node join
        h0 = setUpLocalPeerHost();
        TaskScheduleService tss =
                new TaskScheduleService(taskTemplate, CheckPointTaskService.FACTORY_LINK, this.schedulePeriod);
        Operation post = Operation.createPost(UriUtils.buildUri(h0, CUSTOM_TASK_SCHEDULER_SELF_LINK));
        h0.startService(post, tss);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        this.host.waitForReplicatedFactoryChildServiceConvergence(
                this.host.getNodeGroupToFactoryMap(ExampleService.FACTORY_LINK),
                exampleStatesMap,
                this.exampleStateConvergenceChecker,
                exampleStatesMap.size(),
                0, this.nodeCount);
        exampleStates = getExampleServices(exampleStates);
        lastUpdateExampleState = exampleStates.stream().max(this.documentComparator).get();
        verifyCheckPoints(lastUpdateExampleState.documentUpdateTimeMicros);
    }

    private void updateAndVerifyCheckPoints(QueryTask.Query q, long expectedCheckPoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            startAndWaitCheckPointTaskService(q);
            boolean converged = checkPointsConverged(expectedCheckPoint, CUSTOM_CHECKPOINT_SELF_LINK);
            if (converged) {
                return true;
            }
            return false;
        });
    }

    private void verifyCheckPoints(long expectedCheckPoint) throws Throwable {
        this.host.waitFor("check point convergence timeout", () -> {
            boolean converged = checkPointsConverged(expectedCheckPoint, CUSTOM_CHECKPOINT_SELF_LINK);
            if (converged) {
                return true;
            }
            return false;
        });
    }

    private CheckPointTaskService.State validateTaskState(QueryTask.Query query) {
        CheckPointTaskService.State s = new CheckPointTaskService.State();
        s.query = query;
        s.checkPointServiceLink = CUSTOM_CHECKPOINT_SELF_LINK;
        s.nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
        s.checkPointLag = this.checkPointLag;
        s.checkPointScanWindowSize = this.checkPointScanWindowSize;
        s.onDemandSync = true;
        return s;
    }

    private void startAndWaitCheckPointTaskService(QueryTask.Query query) {
        CheckPointTaskService.State state = validateTaskState(query);
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

    private boolean checkPointsConverged(long expectedCheckPoint, String checkPointServiceLink) {
        Set<Long> checkPoints = new HashSet<>();
        List<Operation> ops = new ArrayList<>();
        for(ServiceHost h : this.host.getInProcessHostMap().values()) {
            ops.add(Operation.createGet(UriUtils.buildUri(h, checkPointServiceLink)));
        }
        List<CheckPointService.CheckPointState> states =
                this.host.getTestRequestSender().sendAndWait(ops, CheckPointService.CheckPointState.class);
        states.stream().forEach(s -> checkPoints.add(s.checkPoint));
        if (checkPoints.size() > 1) {
            this.host.log(Level.INFO, "check point not converged %s",
                    Utils.toJson(checkPoints));
            return false;
        }
        long actualCheckPoint = checkPoints.iterator().next();
        if (actualCheckPoint != expectedCheckPoint) {
            this.host.log(Level.INFO,"Expected check point %d\nActual check point %d", expectedCheckPoint, actualCheckPoint);
            return false;
        }
        return true;
    }

    private List<ExampleService.ExampleServiceState> updateExampleServices(List<ExampleService.ExampleServiceState> exampleStates) {
        List<Operation> ops = new ArrayList<>();
        for (ExampleService.ExampleServiceState st : exampleStates) {
            ExampleService.ExampleServiceState s = new ExampleService.ExampleServiceState();
            s.counter =  st.counter + 1;
            URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), st.documentSelfLink);
            Operation patch = Operation.createPatch(serviceUri).setBody(s);
            ops.add(patch);
        }
        return this.host.getTestRequestSender().sendAndWait(ops, ExampleService.ExampleServiceState.class);
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

    private VerificationHost setUpLocalPeerHost() throws Throwable {
        VerificationHost host = this.host.setUpLocalPeerHost(0, VerificationHost.FAST_MAINT_INTERVAL_MILLIS, null, null);
        CheckPointService.CheckPointState s
                = new CheckPointService.CheckPointState();
        s.checkPoint = 0L;
        Operation post = Operation.createPost(UriUtils.buildUri(host, CUSTOM_CHECKPOINT_SELF_LINK))
                .setBody(s);
        host.startService(post, new CheckPointService());
        host.startFactory(new CheckPointTaskService());
        host.waitForServiceAvailable(CUSTOM_CHECKPOINT_SELF_LINK);
        host.waitForServiceAvailable(CheckPointTaskService.FACTORY_LINK);
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        return host;
    }

    private VerificationHost restartHost(VerificationHost host) throws Throwable {
        host.setPort(0);
        VerificationHost.restartStatefulHost(host, false);
        CheckPointService.CheckPointState s
                = new CheckPointService.CheckPointState();
        s.checkPoint = 0L;
        Operation post = Operation.createPost(UriUtils.buildUri(host, CUSTOM_CHECKPOINT_SELF_LINK))
                .setBody(s);
        host.startService(post, new CheckPointService());
        host.startFactory(new CheckPointTaskService());
        host.waitForServiceAvailable(CUSTOM_CHECKPOINT_SELF_LINK);
        host.waitForServiceAvailable(CheckPointTaskService.FACTORY_LINK);
        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.host.addPeerNode(host);
        return host;
    }

    @After
    public void cleanUp() throws Throwable {
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }
}
