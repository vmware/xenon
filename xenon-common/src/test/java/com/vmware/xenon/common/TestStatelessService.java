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
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Rule;
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
        boolean test = true;
        while (test) {
            log(Level.INFO, "Slowing down the completion of the operation");
        }
        ComputeSquare instructions = post.getBody(ComputeSquare.class);
        instructions.result = instructions.a * instructions.a + instructions.b * instructions.b;
        post.setBodyNoCloning(instructions);
        post.complete();
    }
}

public class TestStatelessService extends BasicReusableHostTestCase {

    public int requestCount = 1000;

    public int iterationCount = 3;

    @Rule
    public TestResults testResults = new TestResults();

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
    }
    
    @Test
    public void testTimeout() throws Throwable {
        LocalTime localTimeStart = LocalTime.now();
        host.log(Level.INFO, "Starting the test: " + localTimeStart.toString());
        Service s = this.host.startServiceAndWait(new StatelessTestService(), "stateless/service",
                null);
        TestContext ctx = new TestContext(this.requestCount, Duration.ofSeconds(60000));
        ComputeSquare c = new ComputeSquare();
        c.a = 2;
        c.b = 3;
        URI uri = s.getUri();
        this.host.send(Operation.createPost(uri)
                    .setBody(c)
                    .setCompletion((o, e) -> {
                        LocalTime localTimeEnd = LocalTime.now();
                        host.log(Level.INFO, "Start time: " + localTimeStart.toString());
                        host.log(Level.INFO, "End time: " + localTimeEnd.toString());
                        ctx.complete();
                    }));
        ctx.await();
        
        

    }
}