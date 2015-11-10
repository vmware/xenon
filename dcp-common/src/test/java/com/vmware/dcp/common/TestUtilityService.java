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

package com.vmware.dcp.common;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Test;

import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.ServiceStats.ServiceStat;
import com.vmware.dcp.services.common.ExampleFactoryService;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.MinimalTestService;

public class TestUtilityService extends BasicReusableHostTestCase {

    private List<Service> createServices(int count) throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(
                count, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);
        return services;
    }

    @Test
    public void patchConfiguration() throws Throwable {
        int count = 10;

        host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);
        // try config patch on a factory
        ServiceConfigUpdateRequest updateBody = ServiceConfigUpdateRequest.create();
        updateBody.removeOptions = EnumSet.of(ServiceOption.IDEMPOTENT_POST);
        this.host.testStart(1);

        URI configUri = UriUtils.buildConfigUri(host, ExampleFactoryService.SELF_LINK);
        this.host.send(Operation.createPatch(configUri).setBody(updateBody)
                .setCompletion(this.host.getCompletion()));

        this.host.testWait();
        this.host.testStart(1);
        // verify option removed
        host.send(Operation.createGet(configUri).setCompletion((o, e) -> {
            if (e != null) {
                host.failIteration(e);
                return;
            }

            ServiceConfiguration cfg = o.getBody(ServiceConfiguration.class);
            if (!cfg.options.contains(ServiceOption.IDEMPOTENT_POST)) {
                host.completeIteration();
            } else {
                host.failIteration(new IllegalStateException(Utils.toJsonHtml(cfg)));
            }

        }));

        this.host.testWait();

        List<Service> services = createServices(count);
        // verify no stats exist before we enable that capability
        for (Service s : services) {
            ServiceStats stats = getStats(s.getUri());
            assertTrue(stats != null);
            assertTrue(stats.entries.isEmpty());
        }

        updateBody = ServiceConfigUpdateRequest.create();
        updateBody.addOptions = EnumSet.of(ServiceOption.INSTRUMENTATION);
        this.host.testStart(services.size());
        for (Service s : services) {
            configUri = UriUtils.buildConfigUri(s.getUri());
            this.host.send(Operation.createPatch(configUri).setBody(updateBody)
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        // get configuration and verify options
        this.host.testStart(services.size());
        for (Service s : services) {
            URI u = UriUtils.buildConfigUri(s.getUri());
            host.send(Operation.createGet(u).setCompletion((o, e) -> {
                if (e != null) {
                    host.failIteration(e);
                    return;
                }

                ServiceConfiguration cfg = o.getBody(ServiceConfiguration.class);
                if (cfg.options.contains(ServiceOption.INSTRUMENTATION)) {
                    host.completeIteration();
                } else {
                    host.failIteration(new IllegalStateException(Utils.toJsonHtml(cfg)));
                }

            }));
        }
        this.host.testWait();

        this.host.testStart(services.size());
        // issue some updates so stats get updated
        for (Service s : services) {
            this.host.send(Operation.createPatch(s.getUri())
                    .setBody(this.host.buildMinimalTestState())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        for (Service s : services) {
            ServiceStats stats = getStats(s.getUri());
            assertTrue(stats != null);
            assertTrue(stats.entries != null);
            assertTrue(!stats.entries.isEmpty());
        }
    }

    @Test
    public void redirectToUiServiceIndex() throws Throwable {

        this.host.testStart(1);
        this.host.registerForServiceAvailability(this.host.getCompletion(),
                ExampleFactoryService.SELF_LINK);
        this.host.testWait();

        // create an example child service and also verify it has a default UI html page
        ExampleServiceState s = new ExampleServiceState();
        s.name = UUID.randomUUID().toString();
        s.documentSelfLink = s.name;
        this.host.testStart(1);
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.host, ExampleFactoryService.class))
                .setBody(s)
                .setCompletion(this.host.getCompletion());
        this.host.send(post);
        this.host.testWait();

        // do a get on examples/ui and examples/<uuid>/ui, twice to test the code path that caches
        // the resource file lookup
        for (int i = 0; i < 2; i++) {
            Operation htmlResponse = this.host.sendUIHttpRequest(
                    UriUtils.buildUri(
                            this.host,
                            UriUtils.buildUriPath(ExampleFactoryService.SELF_LINK,
                                    ServiceHost.SERVICE_URI_SUFFIX_UI))
                            .toString(), null, 1);

            validateServiceUiHtmlResponse(htmlResponse);

            htmlResponse = this.host.sendUIHttpRequest(
                    UriUtils.buildUri(
                            this.host,
                            UriUtils.buildUriPath(ExampleFactoryService.SELF_LINK, s.name,
                                    ServiceHost.SERVICE_URI_SUFFIX_UI))
                            .toString(), null, 1);

            validateServiceUiHtmlResponse(htmlResponse);
        }
    }

    @Test
    public void testUtilityStats() throws Throwable {

        // Step 1: Create an ExampleService instance
        this.host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);
        String name = UUID.randomUUID().toString();
        ExampleServiceState s = new ExampleServiceState();
        s.name = name;
        Consumer<Operation> bodySetter = (o) -> {
            o.setBody(s);
        };
        URI factoryURI = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK);
        long c = 2;
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null, c,
                ExampleServiceState.class, bodySetter, factoryURI);
        ExampleServiceState exampleServiceState = states.values().iterator().next();
        // Step 2 - POST a stat to the service instance and verify we can fetch the stat just posted
        ServiceStats.ServiceStat stat = new ServiceStat();
        stat.name = "key1";
        stat.latestValue = 100;
        this.host.testStart(1);
        this.host.send(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();
        ServiceStats retStat = this.host.getServiceState(null, ServiceStats.class,
                UriUtils.buildStatsUri(
                        this.host, exampleServiceState.documentSelfLink));
        ServiceStat retStatEntry = retStat.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 100);
        assertTrue(retStatEntry.latestValue == 100);
        assertTrue(retStatEntry.version == 1);
        // Step 3 - POST a stat with the same key again and verify that the
        // version and accumulated value are updated
        stat.latestValue = 50;
        this.host.testStart(1);
        this.host.send(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();
        retStat = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = retStat.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 150);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 2);
        // Step 4 - POST a stat with a new key and verify that the
        // previously posted stat is not updated
        stat.name = "key2";
        stat.latestValue = 50;
        this.host.testStart(1);
        this.host.send(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();
        retStat = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = retStat.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 150);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 2);
        retStatEntry = retStat.entries.get("key2");
        assertTrue(retStatEntry.accumulatedValue == 50);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 1);

        // Step 5 - Issue a PUT for the first stat key and verify that the doc state is replaced
        stat.name = "key1";
        stat.latestValue = 75;
        this.host.testStart(1);
        this.host.send(Operation.createPut(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();
        retStat = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = retStat.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 75);
        assertTrue(retStatEntry.latestValue == 75);
        assertTrue(retStatEntry.version == 1);
        // Step 6 - Issue a bulk PUT and verify that the complete set of stats is updated
        ServiceStats stats = new ServiceStats();
        stat.name = "key3";
        stat.latestValue = 200;
        stats.entries.put("key3", stat);
        this.host.testStart(1);
        this.host.send(Operation.createPut(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stats)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();
        retStat = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        assertTrue(retStat.entries.size() == 1);
        retStatEntry = retStat.entries.get("key3");
        assertTrue(retStatEntry.accumulatedValue == 200);
        assertTrue(retStatEntry.latestValue == 200);
        assertTrue(retStatEntry.version == 1);
        // Step 7 - Issue a PATCH and verify that the latestValue is updated
        stat.latestValue = 25;
        this.host.testStart(1);
        this.host.send(Operation.createPatch(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat)
                .setCompletion(this.host.getCompletion()));
        this.host.testWait();
        retStat = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = retStat.entries.get("key3");
        assertTrue(retStatEntry.latestValue == 225);
        assertTrue(retStatEntry.version == 2);
    }

    public void validateServiceUiHtmlResponse(Operation op) {
        assertTrue(op.getStatusCode() == Operation.STATUS_CODE_MOVED_TEMP);
        assertTrue(op.getResponseHeaders().get("Location").contains(
                "/core/ui/default#"));
    }

    public ServiceStats getStats(URI uri) throws Throwable {
        URI statsURI = UriUtils.buildStatsUri(uri);
        ServiceStats stats = this.host.getServiceState(null, ServiceStats.class, statsURI);
        return stats;
    }
}
