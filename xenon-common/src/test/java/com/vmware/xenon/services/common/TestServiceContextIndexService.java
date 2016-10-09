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
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.UriUtils;
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
    public void serDesDirect() throws Throwable {
        Set<String> keys = serializeContexts();
        deserializeContexts(keys);
        verifyFileDeletion();
    }

    private void deserializeContexts(Set<String> keys) {
        Map<String, ServiceRuntimeContext> results = new ConcurrentSkipListMap<>();
        TestContext ctx = this.host.testCreate(keys.size());
        ctx.setTestName("service context index get").logBefore();
        for (String key : keys) {
            Operation putResume = Operation.createPut(this.host,
                    ServiceContextIndexService.SELF_LINK);
            putResume.setBodyNoCloning(ServiceRuntimeContext.create(key));
            this.host.send(putResume.setCompletion((o, e) -> {
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
            assertTrue(service.getSelfLink().contains(this.host.getId()));
            for (ServiceOption option : templateService.getOptions()) {
                assertTrue(service.hasOption(option));
            }
        }
    }

    private Set<String> serializeContexts() throws Throwable {
        TestContext ctx = this.host.testCreate(this.requestCount);
        Set<String> keys = new HashSet<>();
        ctx.setTestName("service context index post").logBefore();
        for (int i = 0; i < this.requestCount; i++) {
            ServiceRuntimeContext src = buildRuntimeContext();
            keys.add(src.selfLink);
            Operation put = Operation.createPut(this.host, ServiceContextIndexService.SELF_LINK)
                    .setBodyNoCloning(src)
                    .setCompletion(ctx.getCompletion());
            this.host.send(put);
        }
        ctx.await();
        ctx.logAfter();
        return keys;
    }

    private void verifyFileDeletion() {
        this.host.waitFor("Delete file stat not present", () -> {
            Map<String, ServiceStat> stats = this.host.getServiceStats(
                    UriUtils.buildUri(this.host, ServiceContextIndexService.SELF_LINK));
            ServiceStat deleteStat = stats
                    .get(ServiceContextIndexService.STAT_NAME_FILE_DELETE_COUNT);
            if (deleteStat == null || deleteStat.latestValue < this.serviceCount) {
                return false;
            }
            return true;
        });
    }

    private ServiceRuntimeContext buildRuntimeContext() {
        ServiceRuntimeContext src = new ServiceRuntimeContext();
        src.selfLink = UriUtils.buildUriPath(this.host.getId(), Utils.getNowMicrosUtc() + "");
        src.serializationTimeMicros = Utils.getNowMicrosUtc();
        MinimalTestService service = new MinimalTestService();
        service.setSelfLink(src.selfLink);
        service.toggleOption(ServiceOption.INSTRUMENTATION, true);
        ByteBuffer bb = KryoSerializers.serializeObject(service, Service.MAX_SERIALIZED_SIZE_BYTES);
        src.serializedService = bb;
        return src;
    }

}
