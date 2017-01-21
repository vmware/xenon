/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.workshop;

import java.util.HashMap;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.ExampleService;

/**
 * StatelessService demo
 */
public class QueryDemo {

    public static void main(String[] args) throws Throwable {
        ServiceHost host = ServiceHost.create(args);
        host.start();
        host.startDefaultCoreServicesSynchronously();
        host.startFactory(ExampleService.class, ExampleService::createFactory);

        // When ExampleService is ready, perform the given callback
        host.registerForServiceAvailability(performQuery(host), ExampleService.FACTORY_LINK);
    }

    private static CompletionHandler performQuery(ServiceHost host) {
        return (o, e) -> {
            ExampleService.ExampleServiceState state = new ExampleService.ExampleServiceState();
            state.counter = 1L;
            state.documentSelfLink = "a";
            state.keyValues = new HashMap<>();
            state.keyValues.put("key1", "value1");
            state.name = "Amanda";
            state.sortedCounter = 1L;

            Operation
                    .createPost(host, ExampleService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer("localhost")
                    .sendWith(host);

            state = new ExampleService.ExampleServiceState();
            state.counter = 10L;
            state.documentSelfLink = "b";
            state.keyValues = new HashMap<>();
            state.keyValues.put("key2", "value1");
            state.name = "Bernard";
            state.sortedCounter = 10L;

            Operation
                    .createPost(host, ExampleService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer("localhost")
                    .sendWith(host);

            state = new ExampleService.ExampleServiceState();
            state.counter = 100L;
            state.documentSelfLink = "c";
            state.keyValues = new HashMap<>();
            state.keyValues.put("key1", "value3");
            state.name = "Commander Adama";
            state.sortedCounter = 100L;

            Operation
                    .createPost(host, ExampleService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer("localhost")
                    .sendWith(host);
        };
    }

    private QueryDemo() {
    }
}
