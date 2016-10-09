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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceRuntimeContext;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.serialization.KryoSerializers;
import com.vmware.xenon.common.test.TestContext;

public class TestServiceContextIndexService extends BasicReusableHostTestCase {
    /**
     * Parameter that specifies number of requests
     */
    public int requestCount = 100;

    public long testStartTimeMicros = Utils.getNowMicrosUtc();

    @Test
    public void postGetDirect() throws Throwable {
        Set<String> keys = postContextsDirect();
        getContextDirect(keys);
    }

    private void getContextDirect(Set<String> keys) {
        Map<String, ServiceRuntimeContext> results = new ConcurrentSkipListMap<>();
        TestContext ctx = this.host.testCreate(keys.size());
        ctx.setTestName("service context index get").logBefore();
        for (String key : keys) {
            Operation get = ServiceContextIndexService.createGet(this.host, key);
            this.host.send(get.setCompletion((o, e) -> {
                if (e != null) {
                    ctx.fail(e);
                    return;
                }
                ServiceRuntimeContext r = o.getBody(ServiceRuntimeContext.class);
                results.put(r.selfLink, r);
                ctx.complete();
            }));
        }
        ctx.await();
        ctx.logAfter();

        ServiceRuntimeContext template = buildRuntimeContext();
        MinimalTestService templateService = (MinimalTestService) KryoSerializers
                .deserializeObject(template.serializedService.array(),
                        0,
                        template.serializedService.limit());
        for (ServiceRuntimeContext r : results.values()) {
            assertNotNull(r.selfLink);
            assertTrue(r.serializationTimeMicros > this.testStartTimeMicros);
            assertNotNull(r.serializedService);
            MinimalTestService service = (MinimalTestService) KryoSerializers
                    .deserializeObject(r.serializedService.array(), 0, r.serializedService.limit());
            assertEquals(templateService.getSelfLink(), service.getSelfLink());
        }
    }

    private Set<String> postContextsDirect() throws Throwable {
        TestContext ctx = this.host.testCreate(this.requestCount);
        Set<String> keys = new HashSet<>();
        ctx.setTestName("service context index post").logBefore();
        for (int i = 0; i < this.requestCount; i++) {
            ServiceRuntimeContext src = buildRuntimeContext();
            keys.add(src.selfLink);
            Operation post = Operation.createPost(this.host, ServiceContextIndexService.SELF_LINK)
                    .setBodyNoCloning(src)
                    .setCompletion(ctx.getCompletion());
            this.host.send(post);
        }
        ctx.await();
        ctx.logAfter();
        return keys;
    }

    private ServiceRuntimeContext buildRuntimeContext() {
        ServiceRuntimeContext src = new ServiceRuntimeContext();
        src.selfLink = "/some/link";
        src.serializationTimeMicros = Utils.getNowMicrosUtc();
        MinimalTestService service = new MinimalTestService();
        service.setSelfLink(src.selfLink);
        service.toggleOption(ServiceOption.INSTRUMENTATION, true);
        ByteBuffer bb = KryoSerializers.serializeObject(service, Service.MAX_SERIALIZED_SIZE_BYTES);
        src.serializedService = bb;
        return src;
    }

}
