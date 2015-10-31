/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.common;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.vmware.dcp.common.test.VerificationHost;

public class BasicReusableHostTestCase {

    private static final int MAINTENANCE_INTERVAL_MILLIS = 1000;

    private static VerificationHost HOST;

    protected static TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    protected VerificationHost host;

    public int requestCount = 1000;

    public long serviceCount = 10;

    public long testDurationSeconds = 0;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        TMP_FOLDER.create();
        HOST = VerificationHost.create(0, TMP_FOLDER.getRoot().toURI());
        HOST.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(MAINTENANCE_INTERVAL_MILLIS));
        CommandLineArgumentParser.parseFromProperties(HOST);
        try {
            HOST.start();
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Before
    public void setUpPerMethod() {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = HOST;
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
        TMP_FOLDER.delete();
    }

}
