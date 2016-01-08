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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
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
        this.host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);
    }

    @Test
    public void testExampleTestServices() throws Throwable {

        createExampleServices();
        Consumer<Operation> notificationTarget = createNotificationTarget();
        String taskPath = createExampleTask();
        subscribeTask(taskPath, notificationTarget);
        waitForTask(taskPath);
        validateNoServices();
    }

    /**
     * Create a set of example services, so we can test that the ExampleTaskService clean them up
     */
    private void createExampleServices() throws Throwable {
        URI exampleFactoryUri = UriUtils.buildUri(this.host, ExampleFactoryService.class);

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
                    "Received task notification: %s, stage = %s, substage = %s",
                    update.getAction(), stage, substage);
        };
        return notificationTarget;
    }

    /**
     * Create the task that will delete the examples
     */
    private String createExampleTask() throws Throwable {
        URI exampleTaskFactoryUri = UriUtils.buildUri(this.host, ExampleTaskFactoryService.class);

        String[] taskUri = new String[1];
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
                            this.host.completeIteration();
                        });

        this.host.testStart(1);
        this.host.send(createPost);
        this.host.testWait();

        assertNotNull(taskUri[0]);
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
    private void waitForTask(String taskUri) throws Throwable {
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
    }

    /**
     * Verify that the task correctly cleaned up all the example services: none should be left.
     */
    private void validateNoServices() throws Throwable {
        URI exampleFactoryUri = UriUtils.buildUri(this.host, ExampleFactoryService.class);

        ServiceDocumentQueryResult exampleServices = this.host.getServiceState(null,
                ServiceDocumentQueryResult.class,
                exampleFactoryUri);

        assertNotNull(exampleServices);
        assertNotNull(exampleServices.documentLinks);
        assertEquals(exampleServices.documentLinks.size(), 0);
    }
}
