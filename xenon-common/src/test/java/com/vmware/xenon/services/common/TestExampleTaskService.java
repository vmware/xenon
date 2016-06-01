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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleTaskService.ExampleTaskServiceState;

/**
 * Validate that the ExampleTaskService works.
 * Demonstrate how to use subscriptions with a task service
 */
public class TestExampleTaskService extends BasicReusableHostTestCase {

    public int numServices = 10;

    @Before
    public void prepare() throws Throwable {
        // Wait for the example and example task factories to start because the host does not
        // wait for them since since they are not core services. Note that production code
        // should be asynchronous and not wait like this
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        this.host.waitForServiceAvailable(ExampleTaskService.FACTORY_LINK);
    }

    @Test
    public void taskCreationWithRestart() throws Throwable {

        createExampleServices();
        Consumer<Operation> notificationTarget = createNotificationTarget();

        String[] taskLink = new String[1];
        CompletionHandler successCompletion = getCompletionWithSelfLink(taskLink);
        ExampleTaskServiceState initialState = new ExampleTaskServiceState();
        sendFactoryPost(ExampleTaskService.class, initialState, successCompletion);
        assertNotNull(taskLink[0]);

        subscribeTask(taskLink[0], notificationTarget);

        ExampleTaskServiceState state = waitForFinishedTask(initialState.getClass(), taskLink[0]);

        // stop the host, and verify task deals with restart
        this.host.stop();
        this.host.setPort(0);
        VerificationHost.restartStatefulHost(this.host);
        this.host.waitForServiceAvailable(taskLink[0]);
        // verify service is re-started, and in FINISHED state
        state = waitForFinishedTask(initialState.getClass(), taskLink[0]);

        updateTaskExpirationAndValidate(state);
        validateNoServices();
    }

    @Test
    public void handleStartError_taskBody() throws Throwable {
        ExampleTaskServiceState badState = new ExampleTaskServiceState();
        badState.taskInfo = new TaskState();
        badState.taskInfo.stage = TaskState.TaskStage.CREATED;
        testExpectedHandleStartError(badState, IllegalArgumentException.class,
                "Do not specify taskInfo: internal use only");
    }

    @Test
    public void handleStartErrors_subStage() throws Throwable {
        ExampleTaskServiceState badState = new ExampleTaskServiceState();
        badState.subStage = ExampleTaskService.SubStage.QUERY_EXAMPLES;
        testExpectedHandleStartError(badState, IllegalArgumentException.class, "Do not specify subStage: internal use only");
    }

    @Test
    public void handleStartErrors_exampleQueryTask() throws Throwable {
        ExampleTaskServiceState badState = new ExampleTaskServiceState();
        badState.exampleQueryTask = QueryTask.create(null);
        testExpectedHandleStartError(badState, IllegalArgumentException.class, "Do not specify exampleQueryTask: internal use only");
    }

    @Test
    public void handleStartErrors_taskLifetimeNegative() throws Throwable {
        ExampleTaskServiceState badState = new ExampleTaskServiceState();
        badState.taskLifetime = -1L;
        testExpectedHandleStartError(badState, IllegalArgumentException.class, "taskLifetime must be positive");
    }

    private void testExpectedHandleStartError(ExampleTaskServiceState badState,
            Class<? extends Throwable> expectedException, String expectedMessage) throws Throwable {
        Throwable[] thrown = new Throwable[1];
        Operation.CompletionHandler errorHandler = getExpectedFailureCompletionReturningThrowable(thrown);
        sendFactoryPost(ExampleTaskService.class, badState, errorHandler);

        assertNotNull(thrown[0]);

        String message = String.format("Thrown exception [thrown=%s] is not 'instanceof' [expected=%s]", thrown[0].getClass(), expectedException);
        assertTrue(message, expectedException.isAssignableFrom(thrown[0].getClass()));
        assertEquals(expectedMessage, thrown[0].getMessage());
    }

    /**
     * Create a set of example services, so we can test that the ExampleTaskService cleans them up
     */
    private void createExampleServices() throws Throwable {
        URI exampleFactoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        this.host.testStart(this.numServices);
        for (int i = 0; i < this.numServices; i++) {
            ExampleServiceState example = new ExampleServiceState();
            example.name = String.format("example-%s", i);
            Operation createPost = Operation.createPost(exampleFactoryUri)
                    .setBody(example)
                    .setCompletion(this.host.getCompletion());
            this.host.send(createPost);
        }
        this.host.testWait();
    }

    /**
     * This creates a lambda to receive notifications. It's meant as a demonstration of how
     * to receive notifications. We don't validate that we receive notifications because
     * our task runs very quickly: there's no guarantee that we'll subscribe for notifications
     * before it completes. That said, this normally does receive notifications, and
     * the log output of the test will show them.
     */
    private Consumer<Operation> createNotificationTarget() {

        Consumer<Operation> notificationTarget = (update) -> {
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
        return notificationTarget;
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

    private void updateTaskExpirationAndValidate(ExampleTaskServiceState state) throws Throwable {
        // Update expiration time to be one hour earlier and make sure state is updated accordingly
        long originalExpiration = state.documentExpirationTimeMicros;
        long newExpiration = originalExpiration - TimeUnit.HOURS.toMicros(1);
        state.documentExpirationTimeMicros = newExpiration;
        state.subStage = null;
        long[] patchedExpiration = new long[1];

        URI exampleTaskUri = UriUtils.buildUri(this.host, state.documentSelfLink);
        Operation patchExpiration = Operation.createPatch(exampleTaskUri)
                .setBody(state)
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                        return;
                    }
                    ExampleTaskServiceState taskResponse = op.getBody(ExampleTaskServiceState.class);
                    patchedExpiration[0] = taskResponse.documentExpirationTimeMicros;
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        this.host.send(patchExpiration);
        this.host.testWait();

        assertEquals("The PATCHed expiration date was not updated correctly",
                patchedExpiration[0], newExpiration);
    }

    /**
     * Verify that the task correctly cleaned up all the example services: none should be left.
     */
    private void validateNoServices() throws Throwable {
        URI exampleFactoryUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        ServiceDocumentQueryResult exampleServices = this.host.getServiceState(null,
                ServiceDocumentQueryResult.class,
                exampleFactoryUri);

        assertNotNull(exampleServices);
        assertNotNull(exampleServices.documentLinks);
        assertEquals(exampleServices.documentLinks.size(), 0);
    }
}
