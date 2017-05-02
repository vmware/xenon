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

package com.vmware.xenon.common.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender.OperationStats;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestRequestSenderTest extends BasicReusableHostTestCase {

    @Test
    public void operationStats() {

        TestRequestSender sender = this.host.getTestRequestSender();

        String selfLink = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, "foo");
        ExampleServiceState state = new ExampleServiceState();
        state.name = "foo";
        state.documentSelfLink = selfLink;
        Operation post = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
        sender.sendAndWait(post, ExampleServiceState.class);

        // multiple patch
        List<Operation> patches = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ExampleServiceState patchBody = new ExampleServiceState();
            patchBody.name = "foo-" + i;
            Operation patch = Operation.createPatch(this.host, selfLink).setBody(patchBody);
            patches.add(patch);
        }
        sender.sendAndWait(patches);

        ExampleServiceState putBody = new ExampleServiceState();
        putBody.name = "foo-put";
        sender.sendAndWait(Operation.createPut(this.host, selfLink).setBody(putBody));

        OperationStats postStats = sender.getOperationStats(Action.POST);
        OperationStats patchStats = sender.getOperationStats(Action.PATCH);
        OperationStats putStats = sender.getOperationStats(Action.PUT);

        assertEquals(1, postStats.count);
        assertEquals(10, patchStats.count);
        assertEquals(1, putStats.count);
    }

    @Test
    public void operationStatsToggle() {
        TestRequestSender sender = this.host.getTestRequestSender();

        sender.sendAndWait(Operation.createGet(this.host, "/"));
        OperationStats stats = sender.getOperationStats(Action.GET);
        assertEquals("default operation stats is enabled", 1, stats.count);

        sender.resetOperationStats();
        stats = sender.getOperationStats(Action.GET);
        assertEquals("default operation stats is reset", 0, stats.count);

        sender.disableOperationStats();
        sender.sendAndWait(Operation.createGet(this.host, "/"));
        stats = sender.getOperationStats(Action.GET);
        assertEquals("operation stats is disabled", 0, stats.count);


        sender.enableOperationStats();
        sender.sendAndWait(Operation.createGet(this.host, "/"));
        stats = sender.getOperationStats(Action.GET);
        assertEquals("operation stats is enabled", 1, stats.count);
    }
}
