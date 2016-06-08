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

import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;

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
    protected ExternalResource verificationHostRule = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            CommandLineArgumentParser.parseFromProperties(BasicTestCase.this);
            BasicTestCase.this.host = createHost();
            CommandLineArgumentParser.parseFromProperties(BasicTestCase.this.host);
            BasicTestCase.this.host.setStressTest(BasicTestCase.this.isStressTest);
            initializeHost(BasicTestCase.this.host);
            beforeHostStart(BasicTestCase.this.host);
            BasicTestCase.this.host.start();
        }

        @Override
        protected void after() {
            beforeHostTearDown(BasicTestCase.this.host);
            BasicTestCase.this.host.tearDown();
        }
    };

    protected TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            BasicTestCase.this.host.log("Running test: " + description.getMethodName());
        }
    };

    public VerificationHost createHost() throws Exception {
        return VerificationHost.create();
    }

    public void initializeHost(VerificationHost host) throws Exception {
        ServiceHost.Arguments args = VerificationHost.buildDefaultServiceHostArguments(0);
        VerificationHost.initialize(host, args);
    }

    public void beforeHostStart(VerificationHost host) throws Exception {

    }

    public void beforeHostTearDown(VerificationHost host) {
    }

    public TestContext testCreate(int c) {
        return this.host.testCreate(c);
    }

    public void testWait(TestContext ctx) throws Throwable {
        ctx.await();
    }

    /**
     * @see VerificationHost#getSafeHandler(CompletionHandler)
     * @param handler
     * @return
     */
    public CompletionHandler getSafeHandler(CompletionHandler handler) {
        return this.host.getSafeHandler(handler);
    }

    @Rule
    public TestRule chain = RuleChain.outerRule(this.verificationHostRule).around(this.watcher);
}
