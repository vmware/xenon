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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceRuntimeContext;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;

public class TestLuceneServiceContextIndexService extends BasicTestCase {
    /**
     * Parameter that specifies number of requests
     */
    public int requestCount = 100;

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
    }

    @Test
    public void postAndGet() throws Throwable {
        Set<String> keys = postContexts();
        this.host.testStart(keys.size());
        for (String key : keys) {
            Operation get = ServiceContextIndexService.createGet(this.host, key);
            this.host.send(get.setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();
        this.host.logThroughput();
    }

    private Set<String> postContexts() throws Throwable {
        TestContext ctx = this.host.testCreate(this.requestCount);
        Set<String> keys = new HashSet<>();
        ctx.setTestName("service context index post").logBefore();
        for (int i = 0; i < this.requestCount; i++) {
            ServiceRuntimeContext src = new ServiceRuntimeContext();
            Operation post = Operation.createPost(this.host, ServiceContextIndexService.SELF_LINK)
                    .setBody(src)
                    .setCompletion(ctx.getCompletion());
            this.host.send(post);
        }
        ctx.await();
        ctx.logAfter();
        return keys;
    }

}
