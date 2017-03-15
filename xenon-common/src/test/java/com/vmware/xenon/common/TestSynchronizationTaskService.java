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

package com.vmware.xenon.common;

import static java.util.stream.Collectors.toList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.NodeGroupService;
import com.vmware.xenon.services.common.ServiceUriPaths;


class SynchRetryExampleService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/test-retry-examples";

    public static FactoryService createFactory() {
        return FactoryService.create(SynchRetryExampleService.class);
    }

    public SynchRetryExampleService() {
        super(ServiceDocument.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
    }

    @Override
    public boolean queueRequest(Operation op) {
        return false;
    }

    @Override
    public void handleRequest(Operation op) {
        if (getSelfLink().endsWith("fail")) {
            if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER)) {
                op.fail(500);
                return;
            }
        }

        super.handleRequest(op);
    }
}

public class TestSynchronizationTaskService extends BasicTestCase {

    public int updateCount = 10;
    public int serviceCount = 10;
    public int nodeCount = 3;

    @BeforeClass
    public static void setUpClass() throws Exception {
        System.setProperty(
                SynchronizationTaskService.PROPERTY_NAME_SYNCHRONIZATION_LOGGING, "true");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        System.setProperty(
                SynchronizationTaskService.PROPERTY_NAME_SYNCHRONIZATION_LOGGING, "false");
    }

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(
                VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
    }

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
        URI exampleFactoryUri = UriUtils.buildUri(
                this.host.getUri(), ExampleService.FACTORY_LINK);
        this.host.waitForReplicatedFactoryServiceAvailable(
                exampleFactoryUri);
    }

    private void setUpMultiNode() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);

        URI exampleFactoryUri = UriUtils.buildUri(
                this.host.getPeerServiceUri(ExampleService.FACTORY_LINK));
        this.host.waitForReplicatedFactoryServiceAvailable(exampleFactoryUri);
    }

    @After
    public void tearDown() {
        this.host.tearDownInProcessPeers();
        this.host.tearDown();
    }

    @Test
    public void ownershipValidation() throws Throwable {
        // This test verifies that only the owner node
        // executes the synchronization task. If the task
        // is started on a non-owner node, the task should
        // self-cancel.
        setUpMultiNode();

        this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null);

        long membershipUpdateTimeMicros = getLatestMembershipUpdateTime(this.host.getPeerHostUri());

        SynchronizationTaskService.State task = createSynchronizationTaskState(membershipUpdateTimeMicros);
        List<Operation> ops = this.host.getInProcessHostMap().keySet().stream()
                .map(uri -> Operation
                    .createPost(UriUtils.buildUri(uri, SynchronizationTaskService.FACTORY_LINK))
                    .setBody(task)
                    .setReferer(this.host.getUri())
                ).collect(toList());

        TestRequestSender sender = new TestRequestSender(this.host);
        List<SynchronizationTaskService.State> results = sender
                .sendAndWait(ops, SynchronizationTaskService.State.class);

        int finishedCount = 0;
        for (SynchronizationTaskService.State r : results) {
            assertTrue(r.taskInfo.stage == TaskState.TaskStage.FINISHED ||
                    r.taskInfo.stage == TaskState.TaskStage.CANCELLED);
            if (r.taskInfo.stage == TaskState.TaskStage.FINISHED) {
                finishedCount++;
            }
        }
        assertTrue(finishedCount == 1);
    }

    @Test
    public void serviceResynchOnFailure() throws Throwable {
        TestRequestSender sender = new TestRequestSender(this.host);

        // Test with all failed to synch services, after all retries the task will be in failed state.
        this.host.startFactory(SynchRetryExampleService.class, SynchRetryExampleService::createFactory);
        URI factoryUri = UriUtils.buildUri(
                this.host, SynchRetryExampleService.FACTORY_LINK);
        this.host.waitForReplicatedFactoryServiceAvailable(factoryUri);

        createExampleServices(sender, this.host, this.serviceCount, "fail");

        SynchronizationTaskService.State task = createSynchronizationTaskState(
                null, SynchRetryExampleService.FACTORY_LINK, ServiceDocument.class);

        // Add pagination in query results.
        task.queryResultLimit = this.serviceCount / 2;

        // Speed up the retries.
        this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(1));

        Operation op = Operation
                .createPost(UriUtils.buildUri(this.host, SynchronizationTaskService.FACTORY_LINK))
                .setBody(task);

        SynchronizationTaskService.State result = sender
                .sendAndWait(op, SynchronizationTaskService.State.class);

        assertEquals(TaskState.TaskStage.FAILED, result.taskInfo.stage);
        assertEquals(0, result.synchCompletionCount);

        // Verify that half of the child services were failed.
        waitForSynchRetries(result, SynchronizationTaskService.STAT_NAME_CHILD_SYNCH_FAILURE_COUNT,
                (synchRetryCount) -> synchRetryCount.latestValue == task.queryResultLimit);

        // Test after all retries the task will be in passed state with at-least half
        // successful synched services in each page.
        createExampleServices(sender, this.host, this.serviceCount * 3, "pass");
        task.queryResultLimit = this.serviceCount * 2;
        op = Operation
                .createPost(UriUtils.buildUri(this.host, SynchronizationTaskService.FACTORY_LINK))
                .setBody(task);

        result = sender.sendAndWait(op, SynchronizationTaskService.State.class);

        assertEquals(TaskState.TaskStage.FINISHED, result.taskInfo.stage);
        assertEquals(this.serviceCount * 4, result.synchCompletionCount);

        // Verify that half of the child services were failed.
        waitForSynchRetries(result, SynchronizationTaskService.STAT_NAME_CHILD_SYNCH_FAILURE_COUNT,
                (synchRetryCount) -> synchRetryCount.latestValue == this.serviceCount);

        waitForSynchRetries(result, SynchronizationTaskService.STAT_NAME_SYNCH_RETRY_COUNT,
                (synchRetryCount) -> synchRetryCount.latestValue > 0);
    }

    private void waitForSynchRetries(SynchronizationTaskService.State state, String statName,
                                     Function<ServiceStats.ServiceStat, Boolean> check) {
        this.host.waitFor("Expected retries not completed", () -> {
            URI statsURI = UriUtils.buildStatsUri(this.host, state.documentSelfLink);
            ServiceStats stats = this.host.getServiceState(null, ServiceStats.class, statsURI);
            ServiceStats.ServiceStat synchRetryCount = stats.entries
                    .get(statName);

            return synchRetryCount != null && check.apply(synchRetryCount);
        });
    }

    private void createExampleServices(
            TestRequestSender sender, ServiceHost h, long serviceCount, String selfLinkPostfix) {

        // create example services
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < serviceCount; i++) {
            ServiceDocument initState = new ServiceDocument();
            initState.documentSelfLink = i + selfLinkPostfix;
            Operation post = Operation.createPost(
                    UriUtils.buildUri(h, SynchRetryExampleService.FACTORY_LINK)).setBody(initState);
            ops.add(post);
        }

        sender.sendAndWait(ops, ServiceDocument.class);
    }

    @Test
    public void synchCounts() throws Throwable {
        this.host.createExampleServices(this.host, this.serviceCount, null);
        SynchronizationTaskService.State task = createSynchronizationTaskState(Long.MAX_VALUE);

        // Add pagination in query results.
        task.queryResultLimit = this.serviceCount / 2;

        Operation op = Operation
                .createPost(UriUtils.buildUri(this.host, SynchronizationTaskService.FACTORY_LINK))
                .setBody(task);

        TestRequestSender sender = new TestRequestSender(this.host);
        SynchronizationTaskService.State result = sender
                .sendAndWait(op, SynchronizationTaskService.State.class);

        assertTrue (result.taskInfo.stage == TaskState.TaskStage.FINISHED);
        assertTrue (result.synchCompletionCount == this.serviceCount);

        // Restart the task to verify counter was reset.
        task = createSynchronizationTaskState(Long.MAX_VALUE);
        task.queryResultLimit = this.serviceCount / 2;

        op = Operation
                .createPost(UriUtils.buildUri(this.host, SynchronizationTaskService.FACTORY_LINK))
                .setBody(task);
        result = sender.sendAndWait(op, SynchronizationTaskService.State.class);

        assertTrue (result.taskInfo.stage == TaskState.TaskStage.FINISHED);
        assertTrue (result.synchCompletionCount == this.serviceCount);
    }

    @Test
    public void taskRestartability() throws Throwable {
        // This test verifies that If the synchronization task
        // is already running and another request arrives, the
        // task will restart itself if the request had a higher
        // membershipUpdateTime.
        URI taskFactoryUri = UriUtils.buildUri(
                this.host.getUri(), SynchronizationTaskService.FACTORY_LINK);
        URI taskUri = UriUtils.extendUri(
                taskFactoryUri, UriUtils.convertPathCharsFromLink(ExampleService.FACTORY_LINK));

        SynchronizationTaskService.State task = this.host.getServiceState(
                null, SynchronizationTaskService.State.class, taskUri);
        assertTrue(task.taskInfo.stage == TaskState.TaskStage.FINISHED);

        long membershipUpdateTimeMicros = task.membershipUpdateTimeMicros;

        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < this.updateCount; i++) {
            membershipUpdateTimeMicros += 1;
            SynchronizationTaskService.State state =
                    createSynchronizationTaskState(membershipUpdateTimeMicros);
            Operation op = Operation
                    .createPost(taskFactoryUri)
                    .setBody(state)
                    .setReferer(this.host.getUri());
            ops.add(op);
        }

        TestRequestSender sender = new TestRequestSender(this.host);
        List<Operation> responses = sender.sendAndWait(ops, false);

        for (Operation o : responses) {
            if (o.getStatusCode() == Operation.STATUS_CODE_OK) {
                SynchronizationTaskService.State r = o.getBody(
                        SynchronizationTaskService.State.class);
                assertTrue(r.taskInfo.stage == TaskState.TaskStage.FINISHED);
            } else if (o.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST) {
                ServiceErrorResponse r = o.getBody(ServiceErrorResponse.class);
                assertTrue(r.getErrorCode() == ServiceErrorResponse.ERROR_CODE_OUTDATED_SYNCH_REQUEST);
            } else {
                throw new IllegalStateException("Unexpected operation response: "
                        + o.getStatusCode());
            }
        }

        final long updateTime = membershipUpdateTimeMicros;
        this.host.waitFor("membershipUpdateTimeMicros was not set correctly", () -> {
            SynchronizationTaskService.State t = this.host.getServiceState(
                    null, SynchronizationTaskService.State.class, taskUri);
            return t.membershipUpdateTimeMicros == updateTime;
        });
    }

    @Test
    public void outdatedSynchronizationRequests() throws Throwable {
        // This test verifies that the synch task will only get
        // restarted if the synch time is new. For requests with
        // older time-stamps, the synch task ignores the request.

        URI taskFactoryUri = UriUtils.buildUri(
                this.host.getUri(), SynchronizationTaskService.FACTORY_LINK);
        URI taskUri = UriUtils.extendUri(
                taskFactoryUri, UriUtils.convertPathCharsFromLink(ExampleService.FACTORY_LINK));

        SynchronizationTaskService.State task = this.host.getServiceState(
                null, SynchronizationTaskService.State.class, taskUri);
        assertTrue(task.taskInfo.stage == TaskState.TaskStage.FINISHED);

        List<Operation> ops = new ArrayList<>();
        long membershipUpdateTimeMicros = task.membershipUpdateTimeMicros;

        for (int i = 0; i < 10; i++) {
            membershipUpdateTimeMicros -= 1;
            SynchronizationTaskService.State state =
                    createSynchronizationTaskState(membershipUpdateTimeMicros);
            Operation op = Operation
                    .createPost(taskFactoryUri)
                    .setBody(state)
                    .setReferer(this.host.getUri());
            ops.add(op);
        }

        TestRequestSender sender = new TestRequestSender(this.host);
        List<Operation> results = sender.sendAndWait(ops, false);

        for (Operation op : results) {
            assertTrue(op.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST);
            ServiceErrorResponse body = op.getBody(ServiceErrorResponse.class);
            assertTrue(body.getErrorCode() == ServiceErrorResponse.ERROR_CODE_OUTDATED_SYNCH_REQUEST);
        }
    }

    @Test
    public void stateValidation() throws Throwable {
        // This test verifies state validation when
        // a synchronization task is created.

        // handleStart validation.
        URI taskFactoryUri = UriUtils.buildUri(
                this.host.getUri(), SynchronizationTaskService.FACTORY_LINK);

        validateInvalidStartState(taskFactoryUri, true, s -> s.factorySelfLink = null);
        validateInvalidStartState(taskFactoryUri, true, s -> s.factoryStateKind = null);
        validateInvalidStartState(taskFactoryUri, true, s -> s.nodeSelectorLink = null);
        validateInvalidStartState(taskFactoryUri, true, s -> s.queryResultLimit = -1);
        validateInvalidStartState(taskFactoryUri, true, s -> s.membershipUpdateTimeMicros = 10L);
        validateInvalidStartState(taskFactoryUri, true, s -> s.queryPageReference = taskFactoryUri);
        validateInvalidStartState(taskFactoryUri, true,
                s -> s.subStage = SynchronizationTaskService.SubStage.SYNCHRONIZE);
        validateInvalidStartState(taskFactoryUri, true,
                s -> s.childOptions = EnumSet.of(Service.ServiceOption.PERSISTENCE));
        validateInvalidStartState(taskFactoryUri, true,
                s -> {
                    s.taskInfo = new TaskState();
                    s.taskInfo.stage = TaskState.TaskStage.STARTED;
                });

        // handlePut validation
        validateInvalidPutRequest(taskFactoryUri, true, s -> s.queryResultLimit = -1);
        validateInvalidPutRequest(taskFactoryUri, true, s -> s.membershipUpdateTimeMicros = null);
        validateInvalidPutRequest(taskFactoryUri, true, s -> s.membershipUpdateTimeMicros = 0L);

        // Let's also test successful requests, to make sure our
        // test methods are doing the right thing.
        validateInvalidStartState(taskFactoryUri, false, null);
        validateInvalidPutRequest(taskFactoryUri, false, null);
    }

    private long getLatestMembershipUpdateTime(URI nodeUri) throws Throwable {
        NodeGroupService.NodeGroupState ngs = this.host.getServiceState(null,
                NodeGroupService.NodeGroupState.class,
                UriUtils.buildUri(nodeUri, ServiceUriPaths.DEFAULT_NODE_GROUP));
        return ngs.membershipUpdateTimeMicros;
    }

    private SynchronizationTaskService.State createSynchronizationTaskState(
            Long membershipUpdateTimeMicros) {
        return createSynchronizationTaskState(
                membershipUpdateTimeMicros, ExampleService.FACTORY_LINK, ExampleService.ExampleServiceState.class);
    }

    private SynchronizationTaskService.State createSynchronizationTaskState(
            Long membershipUpdateTimeMicros, String factoryLink, Class<? extends ServiceDocument> type) {
        SynchronizationTaskService.State task = new SynchronizationTaskService.State();
        task.documentSelfLink = UriUtils.convertPathCharsFromLink(factoryLink);
        task.factorySelfLink = factoryLink;
        task.factoryStateKind = Utils.buildKind(type);
        task.membershipUpdateTimeMicros = membershipUpdateTimeMicros;
        task.nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
        task.queryResultLimit = 1000;
        task.taskInfo = TaskState.create();
        task.taskInfo.isDirect = true;

        return task;
    }

    private void validateInvalidStartState(URI taskFactoryUri,
            boolean expectFailure, Consumer<SynchronizationTaskService.State> stateSetter)
    throws Throwable {
        String factorySelfLink = UUID.randomUUID().toString();
        URI taskUri = UriUtils.extendUri(
                taskFactoryUri, UriUtils.convertPathCharsFromLink(factorySelfLink));

        SynchronizationTaskService.State task = createSynchronizationTaskState(null);
        task.factorySelfLink = factorySelfLink;
        task.documentSelfLink = factorySelfLink;

        if (stateSetter != null) {
            stateSetter.accept(task);
        }

        TestContext ctx = testCreate(1);
        Operation post = Operation
                .createPost(taskUri)
                .setBody(task)
                .setCompletion((o, e) -> {
                    if (expectFailure) {
                        if (o.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST) {
                            ctx.completeIteration();
                            return;
                        }
                        ctx.failIteration(new IllegalStateException(
                                "request was expected to fail"));
                    } else {
                        if (o.getStatusCode() == Operation.STATUS_CODE_ACCEPTED) {
                            ctx.completeIteration();
                            return;
                        }
                        ctx.failIteration(new IllegalStateException(
                                "request was expected to succeed"));
                    }
                });

        SynchronizationTaskService service = SynchronizationTaskService
                .create(() -> new ExampleService());
        this.host.startService(post, service);
        testWait(ctx);
    }

    private void validateInvalidPutRequest(URI taskFactoryUri,
            boolean expectFailure, Consumer<SynchronizationTaskService.State> stateSetter)
            throws Throwable {
        SynchronizationTaskService.State state =
                createSynchronizationTaskState(Long.MAX_VALUE);

        if (stateSetter != null) {
            stateSetter.accept(state);
        }

        TestContext ctx = testCreate(1);
        Operation op = Operation
                .createPost(taskFactoryUri)
                .setBody(state)
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (expectFailure) {
                        if (o.getStatusCode() == Operation.STATUS_CODE_BAD_REQUEST) {
                            ctx.completeIteration();
                            return;
                        }
                        ctx.failIteration(new IllegalStateException(
                                "request was expected to fail"));
                    } else {
                        if (o.getStatusCode() == Operation.STATUS_CODE_OK) {
                            ctx.completeIteration();
                            return;
                        }
                        ctx.failIteration(new IllegalStateException(
                                "request was expected to succeed"));
                    }
                });
        this.host.sendRequest(op);
        testWait(ctx);
    }
}
