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

import java.util.concurrent.TimeUnit;

import org.junit.After;
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
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TaskService;

public class BasicReusableHostTestCase {

    private static final int MAINTENANCE_INTERVAL_MILLIS = 250;

    private static VerificationHost HOST;

    protected VerificationHost host;

    public int requestCount = 1000;

    public long serviceCount = 10;

    public long testDurationSeconds = 0;

    public boolean enableAuth = false;

    public String adminEmail = "admin@vmware.com";

    public String adminPassword = "changeme";

    @BeforeClass
    public static void setUpOnce() throws Exception {
        startHost(false);
    }

    private static void startHost(boolean enableAuth) throws Exception {
        HOST = VerificationHost.create(0);
        HOST.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(MAINTENANCE_INTERVAL_MILLIS));
        CommandLineArgumentParser.parseFromProperties(HOST);
        HOST.setStressTest(HOST.isStressTest);
        HOST.setAuthorizationEnabled(enableAuth);
        try {
            HOST.start();
            HOST.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Before
    public void setUpPerMethod() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = HOST;

        if (this.enableAuth) {

            if (!this.host.isAuthorizationEnabled()) {
                this.host.log("Restarting host to enable authorization");
                tearDownOnce();
                startHost(true);
                this.host = HOST;
            }

            this.host.log("Auth is enabled. Creating users");
            setUpAuthUsers();
            switchToAuthUser();
        }
    }

    protected void setUpAuthUsers()throws Throwable  {

        TestContext testContext = this.host.testCreate(1);

        AuthorizationSetupHelper.AuthSetupCompletion authCompletion = (ex) -> {
            if (ex != null) {
                testContext.failIteration(ex);
                return;
            }
            testContext.completeIteration();
        };

        // create admin user. if it already exists, skip creation.
        this.host.setSystemAuthorizationContext();
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(this.adminEmail)
                .setUserPassword(this.adminPassword)
                .setUserSelfLink(this.adminEmail)
                .setIsAdmin(true)
                .setCompletion(authCompletion)
                .start();
        testContext.await();
        this.host.resetAuthorizationContext();

    }

    protected void switchToAuthUser() throws Throwable {
        String userServicePath = UriUtils
                .buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, this.adminEmail);
        this.host.assumeIdentity(userServicePath);
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
        HOST.tearDownInProcessPeers();
        HOST.tearDown();
    }

    @After
    public void tearDownPerMethod() {
        if (this.enableAuth) {
            clearAuthorization();
        }
    }

    protected void clearAuthorization() {
        this.host.resetAuthorizationContext();
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
     * @see VerificationHost#sendFactoryPost(Class, ServiceDocument, Operation.CompletionHandler)
     */
    public static <T extends ServiceDocument> void sendFactoryPost(Class<? extends Service> service,
            T state, CompletionHandler handler) throws Throwable {
        HOST.sendFactoryPost(service, state, handler);
    }

    /** @see VerificationHost#getCompletionWithSelflink(String[]) */
    public static CompletionHandler getCompletionWithSelfLink(String[] storedLink) {
        return HOST.getCompletionWithSelflink(storedLink);
    }

    /** @see VerificationHost#getExpectedFailureCompletionReturningThrowable(Throwable[]) */
    public static CompletionHandler getExpectedFailureCompletionReturningThrowable(
            Throwable[] storeException) {
        return HOST.getExpectedFailureCompletionReturningThrowable(storeException);
    }

    /** @see VerificationHost#waitForFinishedTask(Class, String) */
    public static <T extends TaskService.TaskServiceState> T waitForFinishedTask(Class<T> type,
            String taskUri) throws Throwable {
        return HOST.waitForFinishedTask(type, taskUri);
    }

    /** @see VerificationHost#waitForFailedTask(Class, String) */
    public static <T extends TaskService.TaskServiceState> T waitForFailedTask(Class<T> type,
            String taskUri) throws Throwable {
        return HOST.waitForFailedTask(type, taskUri);
    }

    /** @see VerificationHost#waitForTask(Class, String, TaskState.TaskStage) */
    public static <T extends TaskService.TaskServiceState> T waitForTask(Class<T> type, String taskUri,
            TaskState.TaskStage expectedStage) throws Throwable {
        return HOST.waitForTask(type, taskUri, expectedStage);
    }

}
