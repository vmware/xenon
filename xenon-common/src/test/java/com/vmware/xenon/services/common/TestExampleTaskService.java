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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
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
    }

    @Test
    public void testExampleTestServices() throws Throwable {

        createExampleServices();
        Consumer<Operation> notificationTarget = createNotificationTarget();
        String taskPath = createExampleTask();
        subscribeTask(taskPath, notificationTarget);

        ExampleTaskServiceState state = waitForTask(taskPath);
        updateTaskExpirationAndValidate(state);
        validateNoServices();
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
     * Create the task that will delete the examples
     */
    private String createExampleTask() throws Throwable {
        URI exampleTaskFactoryUri = UriUtils.buildFactoryUri(this.host, ExampleTaskService.class);

        String[] taskUri = new String[1];
        long[] initialExpiration = new long[1];
        ExampleTaskServiceState task = new ExampleTaskServiceState();
        Operation createPost = Operation.createPost(exampleTaskFactoryUri)
                .setBody(task)
                .setCompletion(
                        (op, ex) -> {
                            if (ex != null) {
                                this.host.failIteration(ex);
                                return;
                            }
                            ExampleTaskServiceState taskResponse = op.getBody(ExampleTaskServiceState.class);
                            taskUri[0] = taskResponse.documentSelfLink;
                            initialExpiration[0] = taskResponse.documentExpirationTimeMicros;
                            this.host.completeIteration();
                        });

        this.host.testStart(1);
        this.host.send(createPost);
        this.host.testWait();

        assertNotNull(taskUri[0]);

        // Since our task body didn't set expiration, the default from TaskService should be used
        long expectedExpiration = Utils.getNowMicrosUtc() + TimeUnit.MINUTES.toMicros(TaskService.DEFAULT_EXPIRATION_MINUTES);
        long wiggleRoom = TimeUnit.MINUTES.toMicros(5); // ensure it's accurate within 5 minutes
        long minExpectedTime = expectedExpiration - wiggleRoom;
        long maxExpectedTime = expectedExpiration + wiggleRoom;
        assertThat("The task's expiration time is incorrect", initialExpiration[0],
                is(both(greaterThan(minExpectedTime)).and(lessThan(maxExpectedTime))));
        return taskUri[0];
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

    /**
     * Wait for the task to complete. It's fast, but it does take time.
     */
    private ExampleTaskServiceState waitForTask(String taskUri) throws Throwable {
        URI exampleTaskUri = UriUtils.buildUri(this.host, taskUri);

        ExampleTaskServiceState state = null;
        for (int i = 0; i < 20; i++) {
            state = this.host.getServiceState(null, ExampleTaskServiceState.class,
                    exampleTaskUri);
            if (state.taskInfo != null) {
                assertNotEquals(state.taskInfo.stage, TaskStage.FAILED);
                if (state.taskInfo.stage == TaskStage.FINISHED) {
                    break;
                }
            }
            Thread.sleep(250);
        }
        assertEquals(state.taskInfo.stage, TaskStage.FINISHED);
        return state;
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

        assertThat("The PATCHed expiration date was not updated correctly",
                patchedExpiration[0], equalTo(newExpiration));
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
