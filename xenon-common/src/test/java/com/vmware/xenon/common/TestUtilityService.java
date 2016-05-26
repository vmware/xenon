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

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.DataPoint;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MinimalTestService;

public class TestUtilityService extends BasicReusableHostTestCase {

    private List<Service> createServices(int count) throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(
                count, MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);
        return services;
    }

    @Before
    public void setUp() {
        // We tell the verification host that we re-use it across test methods. This enforces
        // the use of TestContext, to isolate test methods from each other.
        // In this test class we host.testCreate(count) to get an isolated test context and
        // then either wait on the context itself, or ask the convenience method host.testWait(ctx)
        // to do it for us.
        this.host.setSingleton(true);
    }

    @Test
    public void patchConfiguration() throws Throwable {
        int count = 10;

        host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        // try config patch on a factory
        ServiceConfigUpdateRequest updateBody = ServiceConfigUpdateRequest.create();
        updateBody.removeOptions = EnumSet.of(ServiceOption.IDEMPOTENT_POST);
        TestContext ctx = this.testCreate(1);

        URI configUri = UriUtils.buildConfigUri(host, ExampleService.FACTORY_LINK);
        this.host.send(Operation.createPatch(configUri).setBody(updateBody)
                .setCompletion(ctx.getCompletion()));

        this.testWait(ctx);

        TestContext ctx2 = this.testCreate(1);
        // verify option removed
        this.host.send(Operation.createGet(configUri).setCompletion((o, e) -> {
            if (e != null) {
                ctx2.failIteration(e);
                return;
            }

            ServiceConfiguration cfg = o.getBody(ServiceConfiguration.class);
            if (!cfg.options.contains(ServiceOption.IDEMPOTENT_POST)) {
                ctx2.completeIteration();
            } else {
                ctx2.failIteration(new IllegalStateException(Utils.toJsonHtml(cfg)));
            }

        }));

        this.testWait(ctx2);

        List<Service> services = createServices(count);
        // verify no stats exist before we enable that capability
        for (Service s : services) {
            ServiceStats stats = getStats(s.getUri());
            assertTrue(stats != null);
            assertTrue(stats.entries.isEmpty());
        }

        updateBody = ServiceConfigUpdateRequest.create();
        updateBody.addOptions = EnumSet.of(ServiceOption.INSTRUMENTATION);
        ctx = this.testCreate(services.size());
        for (Service s : services) {
            configUri = UriUtils.buildConfigUri(s.getUri());
            this.host.send(Operation.createPatch(configUri).setBody(updateBody)
                    .setCompletion(ctx.getCompletion()));
        }
        this.testWait(ctx);

        // get configuration and verify options
        TestContext ctx3 = testCreate(services.size());
        for (Service s : services) {
            URI u = UriUtils.buildConfigUri(s.getUri());
            host.send(Operation.createGet(u).setCompletion((o, e) -> {
                if (e != null) {
                    ctx3.failIteration(e);
                    return;
                }

                ServiceConfiguration cfg = o.getBody(ServiceConfiguration.class);
                if (cfg.options.contains(ServiceOption.INSTRUMENTATION)) {
                    ctx3.completeIteration();
                } else {
                    ctx3.failIteration(new IllegalStateException(Utils.toJsonHtml(cfg)));
                }

            }));
        }
        testWait(ctx3);

        ctx = testCreate(services.size());
        // issue some updates so stats get updated
        for (Service s : services) {
            this.host.send(Operation.createPatch(s.getUri())
                    .setBody(this.host.buildMinimalTestState())
                    .setCompletion(ctx.getCompletion()));
        }
        testWait(ctx);

        for (Service s : services) {
            ServiceStats stats = getStats(s.getUri());
            assertTrue(stats != null);
            assertTrue(stats.entries != null);
            assertTrue(!stats.entries.isEmpty());
        }
    }

    @Test
    public void redirectToUiServiceIndex() throws Throwable {
        // create an example child service and also verify it has a default UI html page
        ExampleServiceState s = new ExampleServiceState();
        s.name = UUID.randomUUID().toString();
        s.documentSelfLink = s.name;
        Operation post = Operation
                .createPost(UriUtils.buildFactoryUri(this.host, ExampleService.class))
                .setBody(s);
        this.host.sendAndWaitExpectSuccess(post);

        // do a get on examples/ui and examples/<uuid>/ui, twice to test the code path that caches
        // the resource file lookup
        for (int i = 0; i < 2; i++) {
            Operation htmlResponse = this.host.sendUIHttpRequest(
                    UriUtils.buildUri(
                            this.host,
                            UriUtils.buildUriPath(ExampleService.FACTORY_LINK,
                                    ServiceHost.SERVICE_URI_SUFFIX_UI))
                            .toString(), null, 1);

            validateServiceUiHtmlResponse(htmlResponse);

            htmlResponse = this.host.sendUIHttpRequest(
                    UriUtils.buildUri(
                            this.host,
                            UriUtils.buildUriPath(ExampleService.FACTORY_LINK, s.name,
                                    ServiceHost.SERVICE_URI_SUFFIX_UI))
                            .toString(), null, 1);

            validateServiceUiHtmlResponse(htmlResponse);
        }
    }

    @Test
    public void testUtilityStats() throws Throwable {
        String name = UUID.randomUUID().toString();
        ExampleServiceState s = new ExampleServiceState();
        s.name = name;
        Consumer<Operation> bodySetter = (o) -> {
            o.setBody(s);
        };
        URI factoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        long c = 2;
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null, c,
                ExampleServiceState.class, bodySetter, factoryURI);
        ExampleServiceState exampleServiceState = states.values().iterator().next();
        // Step 2 - POST a stat to the service instance and verify we can fetch the stat just posted
        ServiceStats.ServiceStat stat = new ServiceStat();
        stat.name = "key1";
        stat.latestValue = 100;
        stat.unit = "unit";
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        ServiceStats allStats = this.host.getServiceState(null, ServiceStats.class,
                UriUtils.buildStatsUri(
                        this.host, exampleServiceState.documentSelfLink));
        ServiceStat retStatEntry = allStats.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 100);
        assertTrue(retStatEntry.latestValue == 100);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.unit.equals("unit"));
        // Step 3 - POST a stat with the same key again and verify that the
        // version and accumulated value are updated
        stat.latestValue = 50;
        stat.unit = "unit1";
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        allStats = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = allStats.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 150);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 2);
        assertTrue(retStatEntry.unit.equals("unit1"));
        // Step 4 - POST a stat with a new key and verify that the
        // previously posted stat is not updated
        stat.name = "key2";
        stat.latestValue = 50;
        stat.unit = "unit2";
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        allStats = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = allStats.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 150);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 2);
        assertTrue(retStatEntry.unit.equals("unit1"));
        retStatEntry = allStats.entries.get("key2");
        assertTrue(retStatEntry.accumulatedValue == 50);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.unit.equals("unit2"));

        // Step 5 - Issue a PUT for the first stat key and verify that the doc state is replaced
        stat.name = "key1";
        stat.latestValue = 75;
        stat.unit = "replaceUnit";
        this.host.sendAndWaitExpectSuccess(Operation.createPut(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        allStats = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = allStats.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 75);
        assertTrue(retStatEntry.latestValue == 75);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.unit.equals("replaceUnit"));
        // Step 6 - Issue a bulk PUT and verify that the complete set of stats is updated
        ServiceStats stats = new ServiceStats();
        stat.name = "key3";
        stat.latestValue = 200;
        stat.unit = "unit3";
        stats.entries.put("key3", stat);
        this.host.sendAndWaitExpectSuccess(Operation.createPut(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stats));
        allStats = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        if (allStats.entries.size() != 1) {
            // there is a possibility of node group maintenance kicking in and adding a stat
            ServiceStat nodeGroupStat = allStats.entries.get(
                    Service.STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT);

            if (nodeGroupStat == null) {
                throw new IllegalStateException(
                        "Expected single stat, got: " + Utils.toJsonHtml(allStats));
            }
        }
        retStatEntry = allStats.entries.get("key3");
        assertTrue(retStatEntry.accumulatedValue == 200);
        assertTrue(retStatEntry.latestValue == 200);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.unit.equals("unit3"));
        // Step 7 - Issue a PATCH and verify that the latestValue is updated
        stat.latestValue = 25;
        this.host.sendAndWaitExpectSuccess(Operation.createPatch(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        allStats = this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
        retStatEntry = allStats.entries.get("key3");
        assertTrue(retStatEntry.latestValue == 225);
        assertTrue(retStatEntry.version == 2);
    }

    @Test
    public void testTimeSeriesStats() throws Throwable {
        long startTime = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        int numBuckets = 4;
        long interval = 1000;
        double value = 100;
        // set data to fill up the specified number of buckets
        TimeSeriesStats timeSeriesStats = new TimeSeriesStats(numBuckets, interval, EnumSet.allOf(AggregationType.class));
        for (int i = 0; i < numBuckets; i++) {
            startTime += TimeUnit.MILLISECONDS.toMicros(interval);
            value += 1;
            timeSeriesStats.add(startTime, value);
        }
        assertTrue(timeSeriesStats.dataPoints.size() == numBuckets);
        // insert additional unique datapoints; the earliest entries should be dropped
        for (int i = 0; i < numBuckets / 2; i++) {
            startTime += TimeUnit.MILLISECONDS.toMicros(interval);
            value += 1;
            timeSeriesStats.add(startTime, value);
        }
        assertTrue(timeSeriesStats.dataPoints.size() == numBuckets);
        long timeMicros = startTime - TimeUnit.MILLISECONDS.toMicros(interval * (numBuckets - 1));
        long timeMillis = TimeUnit.MICROSECONDS.toMillis(timeMicros);
        timeMillis -= (timeMillis % interval);
        assertTrue(timeSeriesStats.dataPoints.firstKey() == timeMillis);

        // insert additional datapoints for an existing bucket. The count should increase,
        // min, max, average computed appropriately
        double origValue = value;
        double accumulatedValue = value;
        double newValue = value;
        double count = 1;
        for (int i = 0; i < numBuckets / 2; i++) {
            newValue++;
            count++;
            timeSeriesStats.add(startTime, newValue);
            accumulatedValue += newValue;
        }
        DataPoint lastDatapoint = timeSeriesStats.dataPoints.get(timeSeriesStats.dataPoints.lastKey());
        assertTrue(lastDatapoint.avg.equals(accumulatedValue / count));
        assertTrue(lastDatapoint.count == count);
        assertTrue(lastDatapoint.max.equals(newValue));
        assertTrue(lastDatapoint.min.equals(origValue));

        // test with a subset of the aggregation types specified
        timeSeriesStats = new TimeSeriesStats(numBuckets, interval, EnumSet.of(AggregationType.AVG));
        timeSeriesStats.add(startTime, value);
        lastDatapoint = timeSeriesStats.dataPoints.get(timeSeriesStats.dataPoints.lastKey());
        assertTrue(lastDatapoint.avg != null);
        assertTrue(lastDatapoint.count != 0);
        assertTrue(lastDatapoint.max == null);
        assertTrue(lastDatapoint.min == null);

        timeSeriesStats = new TimeSeriesStats(numBuckets, interval, EnumSet.of(AggregationType.MIN, AggregationType.MAX));
        timeSeriesStats.add(startTime, value);
        lastDatapoint = timeSeriesStats.dataPoints.get(timeSeriesStats.dataPoints.lastKey());
        assertTrue(lastDatapoint.avg == null);
        assertTrue(lastDatapoint.count == 0);
        assertTrue(lastDatapoint.max != null);
        assertTrue(lastDatapoint.min != null);

        // Step 2 - POST a stat to the service instance and verify we can fetch the stat just posted
        String name = UUID.randomUUID().toString();
        ExampleServiceState s = new ExampleServiceState();
        s.name = name;
        Consumer<Operation> bodySetter = (o) -> {
            o.setBody(s);
        };
        URI factoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null, 1,
                ExampleServiceState.class, bodySetter, factoryURI);
        ExampleServiceState exampleServiceState = states.values().iterator().next();
        ServiceStats.ServiceStat stat = new ServiceStat();
        stat.name = "key1";
        stat.latestValue = 100;
        // set bucket size to 1ms
        stat.timeSeriesStats = new TimeSeriesStats(numBuckets, 1, EnumSet.allOf(AggregationType.class));
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        for (int i = 0; i < numBuckets; i++) {
            Thread.sleep(1);
            this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                    this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        }
        ServiceStats allStats = this.host.getServiceState(null, ServiceStats.class,
                UriUtils.buildStatsUri(
                        this.host, exampleServiceState.documentSelfLink));
        ServiceStat retStatEntry = allStats.entries.get(stat.name);
        assertTrue(retStatEntry.accumulatedValue == 100 * (numBuckets + 1));
        assertTrue(retStatEntry.latestValue == 100);
        assertTrue(retStatEntry.version == numBuckets + 1);
        assertTrue(retStatEntry.timeSeriesStats.dataPoints.size() == numBuckets);
    }

    public static class SetAvailableValidationService extends StatefulService {

        public SetAvailableValidationService() {
            super(ExampleServiceState.class);
        }

        @Override
        public void handleStart(Operation op) {
            setAvailable(false);
            // we will transition to available only when we receive a special PATCH.
            // This simulates a service that starts, but then self patch itself sometime
            // later to indicate its done with some complex init. It does not do it in handle
            // start, since it wants to make POST quick.
            op.complete();
        }

        @Override
        public void handlePatch(Operation op) {
            // regardless of body, just become available
            setAvailable(true);
            op.complete();
        }
    }

    @Test
    public void failureOnReservedSuffixServiceStart() throws Throwable {
        TestContext ctx = this.testCreate(ServiceHost.RESERVED_SERVICE_URI_PATHS.length);
        for (String reservedSuffix : ServiceHost.RESERVED_SERVICE_URI_PATHS) {
            Operation post = Operation.createPost(this.host,
                    UUID.randomUUID().toString() + "/" + reservedSuffix)
                    .setCompletion(ctx.getExpectedFailureCompletion());
            this.host.startService(post, new MinimalTestService());
        }
        this.testWait(ctx);
    }

    @Test
    public void testIsAvailableStatAndSuffix() throws Throwable {
        long c = 1;
        URI factoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        String name = UUID.randomUUID().toString();
        ExampleServiceState s = new ExampleServiceState();
        s.name = name;
        Consumer<Operation> bodySetter = (o) -> {
            o.setBody(s);
        };
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null, c,
                ExampleServiceState.class, bodySetter, factoryURI);

        // first verify that service that do not explicitly use the setAvailable method,
        // appear available. Both a factory and a child service
        this.host.waitForServiceAvailable(factoryURI);

        // expect 200 from /factory/<child>/available
        TestContext ctx = testCreate(states.size());
        for (URI u : states.keySet()) {
            Operation get = Operation.createGet(UriUtils.buildAvailableUri(u))
                    .setCompletion(ctx.getCompletion());
            this.host.send(get);
        }
        testWait(ctx);

        // verify that PUT on /available can make it switch to unavailable (503)
        ServiceStat body = new ServiceStat();
        body.name = Service.STAT_NAME_AVAILABLE;
        body.latestValue = 0.0;

        Operation put = Operation.createPut(
                UriUtils.buildAvailableUri(this.host, factoryURI.getPath()))
                .setBody(body);
        this.host.sendAndWaitExpectSuccess(put);

        // verify factory now appears unavailable
        Operation get = Operation.createGet(UriUtils.buildAvailableUri(factoryURI));
        this.host.sendAndWaitExpectFailure(get);

        // verify PUT on child services makes them unavailable
        ctx = testCreate(states.size());
        for (URI u : states.keySet()) {
            put = put.clone().setUri(UriUtils.buildAvailableUri(u))
                    .setBody(body)
                    .setCompletion(ctx.getCompletion());
            this.host.send(put);
        }
        testWait(ctx);

        // expect 503 from /factory/<child>/available
        ctx = testCreate(states.size());
        for (URI u : states.keySet()) {
            get = get.clone().setUri(UriUtils.buildAvailableUri(u))
                    .setCompletion(ctx.getExpectedFailureCompletion());
            this.host.send(get);
        }
        testWait(ctx);

        // now validate a stateful service that is in memory, and explicitly calls setAvailable
        // sometime after it starts
        Service service = this.host.startServiceAndWait(new SetAvailableValidationService(),
                UUID.randomUUID().toString(), new ExampleServiceState());

        // verify service is NOT available, since we have not yet poked it, to become available
        get = Operation.createGet(UriUtils.buildAvailableUri(service.getUri()));
        this.host.sendAndWaitExpectFailure(get);

        // send a PATCH to this special test service, to make it switch to available
        Operation patch = Operation.createPatch(service.getUri())
                .setBody(new ExampleServiceState());
        this.host.sendAndWaitExpectSuccess(patch);

        // verify service now appears available
        get = Operation.createGet(UriUtils.buildAvailableUri(service.getUri()));
        this.host.sendAndWaitExpectSuccess(get);
    }

    public void validateServiceUiHtmlResponse(Operation op) {
        assertTrue(op.getStatusCode() == Operation.STATUS_CODE_MOVED_TEMP);
        assertTrue(op.getResponseHeader("Location").contains(
                "/core/ui/default/#"));
    }

    public ServiceStats getStats(URI uri) throws Throwable {
        URI statsURI = UriUtils.buildStatsUri(uri);
        ServiceStats stats = this.host.getServiceState(null, ServiceStats.class, statsURI);
        return stats;
    }
}
