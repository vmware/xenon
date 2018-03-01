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
import static org.junit.Assert.assertNotEquals;

import java.net.URI;
import java.time.Duration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmware.xenon.common.StatelessTestService.ComputeSquare;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ServiceUriPaths;

class StatelessTestService extends StatelessService {

    public static class ComputeSquare {
        public int a;
        public int b;
        public int result;
    }

    public StatelessTestService() {
    }

    @Override
    public void handlePost(Operation post) {
        ComputeSquare instructions = post.getBody(ComputeSquare.class);
        instructions.result = instructions.a * instructions.a + instructions.b * instructions.b;
        post.setBodyNoCloning(instructions);
        post.complete();
    }

    @Override
    public void handleGet(Operation get) {
        ServiceDocument doc = new ServiceDocument();
        doc.documentOwner = this.getHost().getId();
        doc.documentSelfLink = this.getSelfLink();
        get.setBody(doc);
        get.complete();
    }
}

class StatelessOwnerSelectedTestService extends StatelessTestService {

    public static final String SELF_LINK = "/stateless/owner-selected-service";

    public StatelessOwnerSelectedTestService() {
        super();
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }
}

public class TestStatelessService extends BasicReusableHostTestCase {

    public int requestCount = 1000;

    public int iterationCount = 3;

    public int nodeCount = 3;

    @Rule
    public TestResults testResults = new TestResults();

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
    }

    private void setUpMultiNode() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount - 1);

        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.startServiceAndWait(new StatelessOwnerSelectedTestService(),
                    StatelessOwnerSelectedTestService.SELF_LINK, null);
        }
    }

    @Test
    public void statelessWithOwnerSelectionWithoutReplication() throws Throwable {
        setUpMultiNode();
        TestRequestSender sender = new TestRequestSender(this.host);

        Operation op = Operation.createGet(this.host.getPeerHost(), StatelessOwnerSelectedTestService.SELF_LINK);
        ServiceDocument doc = sender.sendAndWait(op, ServiceDocument.class);
        VerificationHost owner = this.host.getOwnerPeer(doc.documentSelfLink, ServiceUriPaths.DEFAULT_NODE_SELECTOR);

        // Verify a new owner is selected after original owner stopped.
        this.host.stopHost(owner);
        op = Operation.createGet(this.host.getPeerHost(), StatelessOwnerSelectedTestService.SELF_LINK);
        ServiceDocument doc1 = sender.sendAndWait(op, ServiceDocument.class);
        assertNotEquals(doc.documentOwner, doc1.documentOwner);

        owner = this.host.getOwnerPeer(doc.documentSelfLink, ServiceUriPaths.DEFAULT_NODE_SELECTOR);

        // Create new nodes until we find a new owner for our test service.
        String newOwnerId = owner.getId();
        while (owner.getId().equals(newOwnerId)) {
            VerificationHost newHost = this.host.setUpLocalPeerHost(0, VerificationHost.FAST_MAINT_INTERVAL_MILLIS, null, null);
            this.host.joinNodesAndVerifyConvergence(this.host.getPeerCount());
            newOwnerId = this.host.getOwnerPeer(doc.documentSelfLink, ServiceUriPaths.DEFAULT_NODE_SELECTOR).getId();
            newHost.startServiceAndWait(new StatelessOwnerSelectedTestService(),
                    StatelessOwnerSelectedTestService.SELF_LINK, null);
        }

        op = Operation.createGet(this.host.getPeerHost(), StatelessOwnerSelectedTestService.SELF_LINK);
        ServiceDocument doc2 = sender.sendAndWait(op, ServiceDocument.class);
        assertNotEquals(doc1.documentOwner, doc2.documentOwner);
        assertEquals(newOwnerId, doc2.documentOwner);
    }

    @Test (expected = IllegalArgumentException.class)
    public void serviceOptionsTest() throws Throwable {

        StatelessService service = new StatelessTestService();
        service.options.add(Service.ServiceOption.PERSISTENCE);
        service.options.add(Service.ServiceOption.REPLICATION);
        service.options.add(Service.ServiceOption.OWNER_SELECTION);

        this.host.startServiceAndWait(service, "bad-stateless/service", null);
    }

    @Test
    public void throughputPost() throws Throwable {
        Service s = this.host.startServiceAndWait(new StatelessTestService(), "stateless/service",
                null);
        for (int i = 0; i < this.iterationCount; i++) {
            doThroughputPost(s);
            System.gc();
        }
    }

    private void doThroughputPost(Service s) throws Throwable {

        TestContext ctx = new TestContext(this.requestCount, Duration.ofSeconds(this.host
                .getTimeoutSeconds()));
        ComputeSquare c = new ComputeSquare();
        c.a = 2;
        c.b = 3;
        int expectedResult = c.a * c.a + c.b * c.b;
        ctx.setTestName("Stateless service POST throughput").logBefore();
        // cache service URI, Service.getUri() computes it each time, so its expensive
        URI uri = s.getUri();
        for (int i = 0; i < this.requestCount; i++) {

            this.host.send(Operation.createPost(uri)
                    .setBody(c)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            ctx.fail(e);
                            return;
                        }
                        ComputeSquare res = o.getBody(ComputeSquare.class);
                        if (res.result != expectedResult) {
                            ctx.fail(new IllegalStateException("unexpected result"));
                            return;
                        }
                        ctx.complete();
                    }));
        }
        ctx.await();
        double tput = ctx.logAfter();
        this.testResults.getReport().lastValue(TestResults.KEY_THROUGHPUT, tput);

    }
}