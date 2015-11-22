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

package com.vmware.xenon.common.test;

import java.net.URI;
import java.util.logging.Level;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.vmware.xenon.common.BasicReportTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.monitoring.JVMStatsService.JVMStats;
import com.vmware.xenon.common.test.monitoring.JVMStatsService.JVMStatsFactoryService;

/**
 * Gather various statistics for running testcases. We augment junit to decouple the monitoring
 * service from the verificationHost (would be the obvious alternative). The main drawback is that
 * this requires spawning a new host (which is OK, since some testcases might actually not spawn a
 * host) to send data to a remote service.
 *
 * We assume the IP address is the same, and use a different, arbitrary port number.
 */
public class TestResultWatcher extends TestWatcher {
    /**
     * The test results state document, to be sent across.
     */
    TestResultServiceState trState;

    /**
     * Host on which tests are run (with info like IP, port, commit hash etc.).
     * Using VerificationHost instead of Host, to be able to wait when sending request to server.
     */
    BasicReportTestCase reportTestCase;

    /**
     * Set by DCP_REMOTE_MONITOR_FACTORY environmental variable (if one wants to start
     * capturing and post results, then set it to, say, "http://10.20.128.46:8000/")
     * which is in turn captured here.
     */
    URI remoteTestResultService;

    /**
     * Pointer to the periodic JVMStat instance
     */
    URI jvmStatsInstanceUri;

    /**
     * Override the constructor so that we can get handle of the host
     *
     * @param reportTestCase pointer to the basic test case
     * @param remoteUri a uri to the remote monitor factory
     */
    public TestResultWatcher(BasicReportTestCase reportTestCase, URI remoteUri) {
        super();
        this.reportTestCase = reportTestCase;
        this.remoteTestResultService = remoteUri;
    }

    /**
     * Run at the beginning of each test case.
     *
     * @param description
     */
    @Override
    protected void starting(Description description) {
        // Start the periodic monitoring service
        this.reportTestCase.host.startService(
                Operation.createPost(UriUtils.buildUri(this.reportTestCase.host,
                        JVMStatsFactoryService.class)),
                new JVMStatsFactoryService());
        try {
            // make sure monitoring test service is started
            spawnPeriodicStatsService();
            this.trState = TestResultServiceState.populate();
            this.trState.testName = description.getClassName() + "." + description.getMethodName();
            this.trState.startTimeMicros = Utils.getNowMicrosUtc();
            this.trState.gitCommit = ServiceHost.GIT_COMMIT_SOURCE_PROPERTY_COMMIT_ID;
        } catch (Throwable throwable) {
            this.reportTestCase.host.log(Level.INFO, throwable.toString());
            throwable.printStackTrace();
        }
    }

    /**
     * Run when a test case fails
     *
     * @param description
     */
    @Override
    protected void failed(Throwable e, Description description) {
        this.trState.isSuccess = false;
        this.trState.failureReason = Utils.toString(e);
    }

    /**
     * Run when a test case succeeds
     *
     * @param description
     */
    @Override
    protected void succeeded(Description description) {
        this.trState.isSuccess = true;
    }

    /**
     * Run at the end of each test case.
     *
     * @param description
     */
    @Override
    protected void finished(Description description) {
        this.trState.testSiteAddress = this.reportTestCase.host.getPreferredAddress();
        this.trState.endTimeMicros = Utils.getNowMicrosUtc();
        this.trState.jvmStats = retrievePeriodicStats();
        postResults();
    }

    /**
     * Send current test state to a remote server, by creating an instance for this particular run.
     */
    private void postResults() {
        this.reportTestCase.host.updateSystemInfo(false);
        this.trState.systemInfo = this.reportTestCase.host.getSystemInfo();
        Operation factoryPost = Operation
                .createPost(this.remoteTestResultService)
                .setReferer(this.reportTestCase.host.getReferer())
                .setBody(this.trState).setCompletion((o, e) -> {
                    if (e != null) {
                        this.reportTestCase.host.log(Level.INFO, e.toString());
                        this.reportTestCase.host.failIteration(e);
                        return;
                    }
                    this.reportTestCase.host.completeIteration();
                });

        this.reportTestCase.host.testStart(1);
        this.reportTestCase.host.sendRequest(factoryPost);
        try {
            this.reportTestCase.host.testWait();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    /**
     * Spawn a periodic monitoring task instnace.
     */
    private void spawnPeriodicStatsService() {
        try {
            this.reportTestCase.host.waitForServiceAvailable(JVMStatsFactoryService.SELF_LINK);
        } catch (Throwable throwable) {
            this.reportTestCase.host.log(Level.INFO, throwable.toString());
            throwable.printStackTrace();
        }
        // Create an instance
        JVMStats instanceStats = new JVMStats();
        URI factoryUri = UriUtils.buildUri(this.reportTestCase.host,
                JVMStatsFactoryService.SELF_LINK);

        Operation factoryPost = Operation
                .createPost(factoryUri)
                .setReferer(this.reportTestCase.host.getReferer())
                .setBody(instanceStats)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.reportTestCase.host.log(Level.INFO, e.toString());
                                this.reportTestCase.host.failIteration(e);
                                return;
                            }
                            ServiceDocument rsp = o.getBody(ServiceDocument.class);
                            this.jvmStatsInstanceUri = UriUtils.buildUri(this.reportTestCase.host,
                                    rsp.documentSelfLink);
                            this.reportTestCase.host.completeIteration();
                        });
        this.reportTestCase.host.testStart(1);
        this.reportTestCase.host.sendRequest(factoryPost);
        try {
            this.reportTestCase.host.testWait();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    /**
     * Retrieve all of the periodically collected stats
     */
    private JVMStats retrievePeriodicStats() {
        JVMStats currentState;
        try {
            currentState = this.reportTestCase.host.getServiceState(null,
                    JVMStats.class, this.jvmStatsInstanceUri);
        } catch (Throwable throwable) {
            currentState = new JVMStats();
            throwable.printStackTrace();
        }
        return currentState;
    }
}
