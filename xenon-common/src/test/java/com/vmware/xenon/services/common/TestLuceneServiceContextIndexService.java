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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;

public class TestLuceneServiceContextIndexService extends BasicTestCase {
    /**
     * Parameter that specifies number of concurrent update requests
     */
    public int serviceCount = 100;

    @Override
    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
    }

    @Test
    public void postAndGet() throws Throwable {
        Set<String> keys = postServiceInstances();
        this.host.testStart(keys.size());
        for (String key : keys) {
            Operation get = ServiceContextIndexService.createGet(this.host, key);
            this.host.send(get.setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();
        this.host.logThroughput();
    }

    private Set<String> postServiceInstances() throws Throwable {
        TestContext ctx = this.host.testCreate(this.serviceCount);
        Set<String> keys = new HashSet<>();
        List<Service> services = new ArrayList<>();
        // create N services
        for (int i = 0; i < this.serviceCount; i++) {
            Service s = new MinimalTestService();
            String link = Utils.buildUUID(this.host.getIdHash());
            Operation post = Operation.createPost(this.host, link)
                    .setReferer(this.host.getReferer())
                    .setCompletion(ctx.getCompletion());
            keys.add(link);
            services.add(s);
            this.host.startService(post, s);
        }
        ctx.await();
        ctx = this.host.testCreate(this.serviceCount);
        ctx.setTestName("service context index post").logBefore();
        for (Service s : services) {
            Operation post = ServiceContextIndexService.createPost(this.host, s);
            this.host.send(post.setCompletion(ctx.getCompletion()));
        }
        ctx.await();
        ctx.logAfter();
        return keys;
    }

}
