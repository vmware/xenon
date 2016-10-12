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

import java.time.Duration;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.test.TestContext;


public class TestFactoryWithStatelessChildServices extends BasicTestCase {

    @Test
    public void singleNodeFactoryPost() throws Throwable {
        host.startFactoryServicesSynchronously(ExampleBarService.createFactory());

        TestContext waitContext = new TestContext(1, Duration.ofSeconds(30));

        ExampleBarService.ExampleBarServiceContext[] response = new ExampleBarService.ExampleBarServiceContext[1];

        Operation post = Operation.createPost(host, ExampleBarService.FACTORY_LINK)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        waitContext.failIteration(e);
                        return;
                    }
                    response[0] = o.getBody(ExampleBarService.ExampleBarServiceContext.class);
                    waitContext.completeIteration();
                });

        host.send(post);
        waitContext.await();

        assert (response[0].documentSelfLink != null);


        TestContext waitContext1 = new TestContext(1, Duration.ofSeconds(30));

        Operation get = Operation.createGet(host, response[0].documentSelfLink)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        waitContext1.failIteration(e);
                        return;
                    }
                    response[0] = o.getBody(ExampleBarService.ExampleBarServiceContext.class);
                    waitContext1.completeIteration();
                });

        host.send(get);
        waitContext1.await();
        assert (response[0].message.equals("Default Message"));

        TestContext waitContext2 = new TestContext(1, Duration.ofSeconds(30));

        post = Operation.createPost(host, response[0].documentSelfLink)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        waitContext2.failIteration(e);
                        return;
                    }
                    response[0] = o.getBody(ExampleBarService.ExampleBarServiceContext.class);
                    waitContext2.completeIteration();
                });

        host.send(post);
        waitContext2.await();
        assert (response[0].message.equals("Default Message modified"));

    }

    public static class ExampleBarService extends StatelessService {
        public static final String FACTORY_LINK = ServiceUriPaths.CORE + "/stateless-examples";
        public ExampleBarServiceContext context;

        public ExampleBarService() {
            super(ExampleBarServiceContext.class);
        }

        public static class ExampleBarServiceContext extends ServiceDocument{
            public String message;
        }

        public static FactoryService createFactory() {
            return FactoryService.create(ExampleBarService.class);
        }

        public void handleStart(Operation start) {
            ExampleBarServiceContext body = start.getBody(ExampleBarServiceContext.class);
            if (body.message == null) {
                body.message = "Default Message";
            }
            this.context = body;
            start.setBody(body);
            start.complete();
        }

        public void handleGet(Operation get) {
            get.setBody(this.context).complete();
        }

        public void handlePost(Operation post) {
            this.context.message += " modified";
            post.setBody(this.context).complete();
        }
    }
}
