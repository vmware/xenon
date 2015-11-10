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

package com.vmware.dcp.common;

import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.vmware.dcp.common.test.VerificationHost;

/**
 * BasicTestCase creates and starts a VerificationHost on a random port, using
 * a temporary directory for its storage sandbox.
 *
 * The implementation uses jUnit's @Rule annotations which means that subclasses
 * can use either @Rule annotations or @Before blocks to access the started host.
 *
 * Note about jUnit's sequencing: all {@link org.junit.Rule} annotated test rules
 * _anywhere_ in the class hierarchy are executed before and after any
 * {@link org.junit.Before} and {@link org.junit.After} blocks. Test rules defined in
 * superclasses execute before rules defined in subclasses. The sequencing of
 * multiple rules within a class is undefined. If order between these rules is
 * required, use {@link org.junit.rules.RuleChain}.
 */
public class BasicTestCase {
    public VerificationHost host;
    public boolean isStressTest;
    protected TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected ExternalResource verificationHostRule = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            CommandLineArgumentParser.parseFromProperties(BasicTestCase.this);
            BasicTestCase.this.host = VerificationHost.create(0, BasicTestCase.this.temporaryFolder
                    .getRoot().toURI());
            CommandLineArgumentParser.parseFromProperties(BasicTestCase.this.host);
            BasicTestCase.this.host.setStressTest(BasicTestCase.this.isStressTest);
            beforeHostStart(BasicTestCase.this.host);
            BasicTestCase.this.host.start();
        }

        @Override
        protected void after() {
            beforeHostTearDown(BasicTestCase.this.host);
            // we use a TemporaryFolder instance which will auto delete on exit
            boolean cleanupStorage = false;
            BasicTestCase.this.host.tearDown(cleanupStorage);
        }
    };

    protected TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            BasicTestCase.this.host.log("Running test: " + description.getMethodName());
        }
    };

    public void beforeHostStart(VerificationHost host) {
    }

    public void beforeHostTearDown(VerificationHost host) {
    }

    @Rule
    public TestRule chain = RuleChain.outerRule(this.temporaryFolder).around(
            this.verificationHostRule).around(this.watcher);
}
