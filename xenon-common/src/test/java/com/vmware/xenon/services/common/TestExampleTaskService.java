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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleTaskService.ExampleTaskServiceState;
import com.vmware.xenon.services.common.QueryTask.Query;

/**
 * Validate that the ExampleTaskService works.
 * Demonstrate how to use subscriptions with a task service
 */
public class TestExampleTaskService extends BasicReusableHostTestCase {

    public int serviceCount = 10;
    private TestRequestSender sender;

    @Before
    public void prepare() throws Throwable {
        // Wait for the example and example task factories to start because the host does not
        // wait for them since since they are not core services. Note that production code
        // should be asynchronous and not wait like this
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.host.waitForServiceAvailable(ExampleTaskService.FACTORY_LINK);

        this.sender = new TestRequestSender(this.host);
    }

    @Test
    public void taskCreationWithRestart() throws Throwable {

        createExampleServices();

        ExampleTaskServiceState initialState = new ExampleTaskServiceState();

        Operation post = Operation.createPost(this.host, ExampleTaskService.FACTORY_LINK).setBody(initialState);
        ServiceDocument doc = this.sender.sendAndWait(post, ServiceDocument.class);

        String taskLink = doc.documentSelfLink;
        assertNotNull(taskLink);

        Consumer<Operation> notificationTarget = createNotificationTarget();
        subscribeTask(taskLink, notificationTarget);
        waitForTask(initialState.getClass(), taskLink, TaskState.TaskStage.FINISHED, true);

        // stop the host, and verify task deals with restart
        this.host.stop();
        this.host.setPort(0);
        if (!VerificationHost.restartStatefulHost(this.host, true)) {
            this.host.log("Failed restart of host, aborting");
            return;
        }
        this.host.waitForServiceAvailable(taskLink);
        // verify service is re-started, and in FINISHED state
        ExampleTaskServiceState state = waitForTask(
                initialState.getClass(), taskLink, TaskState.TaskStage.FINISHED, true);

        updateTaskExpirationAndValidate(state);
        validateNoServices();
    }

    @Test
    public void handleStartErrors_subStage() {
        ExampleTaskServiceState badState = new ExampleTaskServiceState();
        badState.subStage = ExampleTaskService.SubStage.QUERY_EXAMPLES;
        verifyExpectedHandleStartError(badState, "Do not specify subStage: internal use only");
    }

    @Test
    public void handleStartErrors_exampleQueryTask() {
        ExampleTaskServiceState badState = new ExampleTaskServiceState();
        badState.exampleQueryTask = QueryTask.create(null);
        verifyExpectedHandleStartError(badState, "Do not specify exampleQueryTask: internal use only");
    }

    @Test
    public void handleStartErrors_taskLifetimeNegative() {
        ExampleTaskServiceState badState = new ExampleTaskServiceState();
        badState.taskLifetime = -1L;
        verifyExpectedHandleStartError(badState, "taskLifetime must be positive");
    }

    @Test
    public void testDirectTask() {
        createExampleServices();
        URI callBackQueryUri = UriUtils.buildDocumentQueryUri(
                this.host, ServiceUriPaths.CORE_CALLBACKS + "/*", false, false, EnumSet.of(Service.ServiceOption.NONE));
        // Get the current list of callbacks and compare it at the end to verify that new callbacks were cleaned.
        ServiceDocumentQueryResult prevCallbacks =
                this.sender.sendAndWait(Operation.createGet(callBackQueryUri), ServiceDocumentQueryResult.class);

        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleTaskServiceState state = new ExampleTaskServiceState();
            state.taskInfo = TaskState.createDirect();
            // specialize each task to only delete a service with the specific id;
            state.customQueryClause = Query.Builder.create()
                    .addFieldClause(ExampleServiceState.FIELD_NAME_ID, i + "").build();
            Operation post = Operation.createPost(this.host, ExampleTaskService.FACTORY_LINK)
                    .setBody(state);
            posts.add(post);
        }

        Map<String, ServiceStat> factoryStatsBefore = this.host
                .getServiceStats(posts.get(0).getUri());
        ServiceStat subCountStatBefore = factoryStatsBefore
                .get(TaskFactoryService.STAT_NAME_ACTIVE_SUBSCRIPTION_COUNT);
        if (subCountStatBefore == null) {
            subCountStatBefore = new ServiceStat();
        }

        List<ExampleTaskServiceState> results = this.sender.sendAndWait(posts,
                ExampleTaskServiceState.class);

        for (ExampleTaskServiceState result : results) {
            assertNotNull(result.taskInfo);
            assertEquals(TaskStage.FINISHED, result.taskInfo.stage);
        }

        ServiceStat beforeStat = subCountStatBefore;
        this.host.waitFor("subscriptions never stopped", () -> {
            Map<String, ServiceStat> factoryStatsAfter = this.host
                    .getServiceStats(posts.get(0).getUri());

            ServiceStat subCountStat = factoryStatsAfter
                    .get(TaskFactoryService.STAT_NAME_ACTIVE_SUBSCRIPTION_COUNT);
            if (subCountStat == null || subCountStat.latestValue > beforeStat.latestValue) {
                return false;
            }
            return true;
        });

        this.host.waitFor("callbacks never stopped", () -> {
            ServiceDocumentQueryResult currentCallbacks =
                    this.sender.sendAndWait(Operation.createGet(callBackQueryUri), ServiceDocumentQueryResult.class);
            return currentCallbacks.documentLinks.size() <= prevCallbacks.documentLinks.size();
        });
    }

    private void verifyExpectedHandleStartError(ExampleTaskServiceState badState, String expectedMessage) {
        Operation post = Operation.createPost(this.host, ExampleTaskService.FACTORY_LINK).setBody(badState);
        FailureResponse response = this.sender.sendAndWaitFailure(post);
        Throwable failure = response.failure;
        assertNotNull(failure);

        String message = String.format("Thrown exception [thrown=%s] is not 'instanceof' [expected=%s]",
                failure.getClass(), IllegalArgumentException.class);
        assertTrue(message, failure instanceof IllegalArgumentException);
        assertEquals(expectedMessage, failure.getMessage());
    }

    /**
     * Create a set of example services, so we can test that the ExampleTaskService cleans them up
     */
    private void createExampleServices() {
        URI exampleFactoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            ExampleServiceState example = new ExampleServiceState();
            example.name = String.format("example-%s", i);
            example.id = i + "";
            ops.add(Operation.createPost(exampleFactoryUri).setBody(example));
        }
        this.sender.sendAndWait(ops);
    }

    /**
     * This creates a lambda to receive notifications. It's meant as a demonstration of how
     * to receive notifications. We don't validate that we receive notifications because
     * our task runs very quickly: there's no guarantee that we'll subscribe for notifications
     * before it completes. That said, this normally does receive notifications, and
     * the log output of the test will show them.
     */
    private Consumer<Operation> createNotificationTarget() {

        return (update) -> {
            update.complete();

            if (!update.hasBody()) {
                // This is probably a DELETE
                this.host.log(Level.INFO, "Got notification: %s", update.getAction());
                return;
            }

            ExampleTaskServiceState taskState = update.getBody(ExampleTaskServiceState.class);
            this.host.log(Level.INFO, "Got notification: %s", taskState);
            String stage = "Unknown";
            String substage = "Unknown";
            if (taskState.taskInfo != null && taskState.taskInfo.stage != null) {
                stage = taskState.taskInfo.stage.toString();
            }
            if (taskState.subStage != null) {
                substage = taskState.subStage.toString();
            }
            this.host.log(Level.INFO,
                    "Received task notification: %s, stage = %s, substage = %s, documentExpiration = %d",
                    update.getAction(), stage, substage, taskState.documentExpirationTimeMicros);
        };
    }

    /**
     * Subscribe to notifications from the task.
     *
     * Note that in this short-running test, we are not guaranteed to get all notifications:
     * we may subscribe after the task has completed some or all of its steps. However, we
     * usually get all notifications.
     *
     */
    private void subscribeTask(String taskPath, Consumer<Operation> notificationTarget)
            throws Throwable {
        URI taskUri = UriUtils.buildUri(this.host, taskPath);
        Operation subscribe = Operation.createPost(taskUri)
                .setCompletion(this.host.getCompletion())
                .setReferer(this.host.getReferer());

        this.host.testStart(1);
        this.host.startSubscriptionService(subscribe, notificationTarget);
        this.host.testWait();
    }

    private void updateTaskExpirationAndValidate(ExampleTaskServiceState state) {
        // Update expiration time to be one hour earlier and make sure state is updated accordingly
        long originalExpiration = state.documentExpirationTimeMicros;
        long newExpiration = originalExpiration - TimeUnit.HOURS.toMicros(1);
        state.documentExpirationTimeMicros = newExpiration;
        state.subStage = null;

        Operation patch = Operation.createPatch(this.host, state.documentSelfLink).setBody(state);
        ExampleTaskServiceState response = this.sender.sendAndWait(patch, ExampleTaskServiceState.class);

        assertEquals("The PATCHed expiration date was not updated correctly",
                response.documentExpirationTimeMicros, newExpiration);
    }

    /**
     * Verify that the task correctly cleaned up all the example services: none should be left.
     */
    private void validateNoServices() throws Throwable {
        URI exampleFactoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        ServiceDocumentQueryResult exampleServices = this.host.getServiceState(null,
                ServiceDocumentQueryResult.class, exampleFactoryUri);

        assertNotNull(exampleServices);
        assertNotNull(exampleServices.documentLinks);
        assertEquals(exampleServices.documentLinks.size(), 0);
    }
}
