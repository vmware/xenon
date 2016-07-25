/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleServiceHost;
import com.vmware.xenon.services.common.ExampleServiceHost.ExampleHostArguments;
import com.vmware.xenon.test.ExceptionTestUtils;
import com.vmware.xenon.test.NodeClient;
import com.vmware.xenon.test.TestNodeGroup;

public class Sample {

    // This demonstrates:
    //   - use TestNodeGroup and NodeClient to represent xenon cluster and client
    //   - test code does not throw checked exception
    //   - synchronously perform operations without explicit testContext
    //
    public static void main(String[] args) {    // no throwable

        ExampleServiceHost host = new ExampleServiceHost();

        // TODO: cleanup. Ideally xenon should not throw Throwable. wrapping it for now.
        try {
            host.initialize(new ExampleHostArguments());
            host.setPort(8001);
            host.start();
        } catch (Throwable throwable) {
            ExceptionTestUtils.throwAsUnchecked(throwable);
        }

        // create nodegroup and wait conversion and service to be ready
        TestNodeGroup<ExampleServiceHost> nodeGroup = new TestNodeGroup<>();
        nodeGroup.add(host);
        nodeGroup.waitConversion();
        nodeGroup.waitService("/core/examples");

        NodeClient<ExampleServiceHost> client = nodeGroup.getRandomClient();

        // POST
        ExampleServiceState postBody = new ExampleServiceState();
        postBody.name = "foo";
        postBody.documentSelfLink = "/foo";
        Operation post = Operation.createPost(client.getServiceHost(), "/core/examples")
                .setBody(postBody);

        // synchronously perform operation
        client.sendExpectSuccess(post);

        nodeGroup.waitService("/core/examples/foo");

        // GET and get body
        Operation get = Operation.createGet(client.getServiceHost(), "/core/examples/foo")
                .forceRemote();

        // synchronously perform operation, then return body
        ExampleServiceState result = client.sendThenGetBody(get, ExampleServiceState.class);

        // assert the result.
        // user is recommended to use assertion from test framework such as JUnit5, Junit4, TestNG,
        // or assertion library such as AssertJ, Hamcrest, Truth, etc.
        System.out.println(result.documentOwner);

    }
}
