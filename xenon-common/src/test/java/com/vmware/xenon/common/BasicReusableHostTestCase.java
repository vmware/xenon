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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;

import static com.vmware.xenon.common.TaskState.TaskStage;
import static com.vmware.xenon.services.common.TaskService.TaskServiceState;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;

public class BasicReusableHostTestCase {

    private static final int MAINTENANCE_INTERVAL_MILLIS = 250;

    private static VerificationHost HOST;

    protected VerificationHost host;

    public int requestCount = 1000;

    public long serviceCount = 10;

    public long testDurationSeconds = 0;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        HOST = VerificationHost.create(0);
        HOST.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(MAINTENANCE_INTERVAL_MILLIS));
        CommandLineArgumentParser.parseFromProperties(HOST);
        HOST.setStressTest(HOST.isStressTest);
        try {
            HOST.start();
            HOST.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Before
    public void setUpPerMethod() {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = HOST;
    }

    public TestContext testCreate(int c) {
        return this.host.testCreate(c);
    }

    public void testWait(TestContext ctx) throws Throwable {
        ctx.await();
    }

    protected TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            HOST.log("Running test: " + description.getMethodName());
        }
    };

    @Rule
    public TestRule chain = RuleChain.outerRule(this.watcher);

    @AfterClass
    public static void tearDownOnce() {
        HOST.tearDown();
    }

    /**
     * @see VerificationHost#getSafeHandler(CompletionHandler)
     * @param handler
     * @return
     */
    public static CompletionHandler getSafeHandler(CompletionHandler handler) {
        return HOST.getSafeHandler(handler);
    }

    /**
     * Creates a new service instance of type {@code service} via a {@code HTTP POST} to the service
     * factory URI (which is discovered automatically based on {@code service}). It passes {@code
     * state} as the body of the {@code POST}.
     * <p/>
     * See javadoc for <i>handler</i> param for important details on how to properly use this
     * method. If your test expects the service instance to be created successfully, you might use:
     * <pre>
     * String[] taskUri = new String[1];
     * CompletionHandler successHandler = getCompletionWithUri(taskUri);
     * createServiceInstance(ExampleTaskService.class, new ExampleTaskServiceState(), successHandler);
     * </pre>
     *
     * @param service the type of service to create
     * @param state   the body of the {@code POST} to use to create the service instance
     * @param handler the completion handler to use when creating the service instance.
     *                <b>IMPORTANT</b>: This handler must properly call {@code host.failIteration()}
     *                or {@code host.completeIteration()}.
     * @param <T>     the state that represents the service instance
     * @see com.vmware.xenon.services.common.TestExampleTaskService#testExampleTestServices()
     */
    protected <T extends ServiceDocument> void createServiceInstance(
            Class<? extends Service> service,
            T state, CompletionHandler handler) throws Throwable {
        URI factoryURI = UriUtils.buildFactoryUri(this.host, service);
        this.host.log(Level.INFO, "Creating POST for [uri=%s] [body=%s]", factoryURI, state);
        Operation createPost = Operation.createPost(factoryURI)
                .setBody(state)
                .setCompletion(handler);

        this.host.testStart(1);
        this.host.send(createPost);
        this.host.testWait();
    }

    /**
     * Helper completion handler that:
     * <ul>
     * <li>Expects valid response to be returned; no exceptions when processing the operation</li>
     * <li>Expects a {@code ServiceDocument} to be returned in the response body. The response's
     * {@link ServiceDocument#documentSelfLink} will be stored in {@code storeUri[0]} so it can be
     * used for test assertions and logic</li>
     * </ul>
     *
     * @param storeUri The {@code documentSelfLink} of the created {@code ServiceDocument} will be
     *                 stored in {@code storeUri[0]} so it can be used for test assertions and
     *                 logic. This must be non-null and its length cannot be zero
     * @return a completion handler, handy for using in methods like {@link
     * #createServiceInstance(Class, ServiceDocument, CompletionHandler)}
     */
    protected static CompletionHandler getCompletionWithUri(String[] storeUri) {
        if (storeUri == null || storeUri.length == 0) {
            throw new IllegalArgumentException(
                    "storeUri must be initialized and have room for at least one item");
        }

        return (op, ex) -> {
            if (ex != null) {
                HOST.failIteration(ex);
                return;
            }

            ServiceDocument response = op.getBody(ServiceDocument.class);
            if (response == null) {
                HOST.failIteration(new IllegalStateException(
                        "Expected non-null ServiceDocument in response body"));
                return;
            }

            HOST.log(Level.INFO, "Created service instance. [selfLink=%s] [kind=%s]",
                    response.documentSelfLink, response.documentKind);
            storeUri[0] = response.documentSelfLink;
            HOST.completeIteration();
        };
    }

    /**
     * Helper completion handler that:
     * <ul>
     * <li>Expects an exception when processing the handler; it is a {@code failIteration} if an
     * exception is <b>not</b> thrown.</li>
     * <li>The exception will be stored in {@code storeException[0]} so it can be used for test
     * assertions and logic.</li>
     * </ul>
     *
     * @param storeException the exception that occurred in completion handler will be stored in
     *                       {@code storeException[0]} so it can be used for test assertions and
     *                       logic. This must be non-null and its length cannot be zero.
     * @return a completion handler, handy for using in methods like {@link
     * #createServiceInstance(Class, ServiceDocument, CompletionHandler)}
     */
    protected static CompletionHandler getExpectedFailureCompletion(Throwable[] storeException) {
        if (storeException == null || storeException.length == 0) {
            throw new IllegalArgumentException(
                    "storeException must be initialized and have room for at least one item");
        }

        return (op, ex) -> {
            if (ex == null) {
                HOST.failIteration(new IllegalStateException("Failure expected"));
            }
            storeException[0] = ex;
            HOST.completeIteration();
        };
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskStage} == {@code
     * TaskStage.FINISHED}.
     *
     * @param type    The class type that represent's the task's state
     * @param taskUri the URI of the task to wait for
     * @param <T>     the type that represent's the task's state
     * @return the state of the task once's it's {@code FINISHED}
     */
    protected <T extends TaskServiceState> T waitForFinishedTask(Class<T> type, String taskUri)
            throws Throwable {
        return waitForTask(type, taskUri, TaskStage.FINISHED);
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskStage} == {@code
     * TaskStage.FAILED}.
     *
     * @param type    The class type that represent's the task's state
     * @param taskUri the URI of the task to wait for
     * @param <T>     the type that represent's the task's state
     * @return the state of the task once's it s {@code FAILED}
     */
    protected <T extends TaskServiceState> T waitForFailedTask(Class<T> type, String taskUri)
            throws Throwable {
        return waitForTask(type, taskUri, TaskStage.FAILED);
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskStage} == {@code
     * expectedStage}.
     *
     * @param type          The class type of that represent's the task's state
     * @param taskUri       the URI of the task to wait for
     * @param expectedStage the stage we expect the task to eventually get to
     * @param <T>           the type that represent's the task's state
     * @return the state of the task once it's {@link TaskStage} == {@code expectedStage}
     */
    protected <T extends TaskServiceState> T waitForTask(Class<T> type, String taskUri,
            TaskStage expectedStage) throws Throwable {
        URI uri = UriUtils.buildUri(this.host, taskUri);

        // If the task's state ever reaches one of these "final" stages, we can stop waiting...
        List<TaskStage> finalTaskStages = Arrays
                .asList(TaskStage.CANCELLED, TaskStage.FAILED, TaskStage.FINISHED, expectedStage);

        T state = null;
        for (int i = 0; i < 20; i++) {
            state = this.host.getServiceState(null, type, uri);
            if (state.taskInfo != null) {
                if (finalTaskStages.contains(state.taskInfo)) {
                    break;
                }
            }
            Thread.sleep(250);
        }
        assertEquals("Task did not reach expected state", state.taskInfo.stage, expectedStage);
        return state;
    }
}
