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

import java.net.URI;
import java.time.Duration;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.StatelessTestService.ComputeSquare;
import com.vmware.xenon.common.test.TestContext;

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
}

public class TestStatelessService extends BasicReusableHostTestCase {

    public int requestCount = 1000;

    public int iterationCount = 3;

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
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
        ctx.logAfter();

    }
}