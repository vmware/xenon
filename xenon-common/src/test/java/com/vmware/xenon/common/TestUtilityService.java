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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
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
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.TimeBin;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestUtilityService extends BasicReusableHostTestCase {

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

        List<URI> services = this.host.createExampleServices(this.host, count, null);

        updateBody = ServiceConfigUpdateRequest.create();
        updateBody.addOptions = EnumSet.of(ServiceOption.PERIODIC_MAINTENANCE);
        updateBody.peerNodeSelectorPath = ServiceUriPaths.DEFAULT_1X_NODE_SELECTOR;
        ctx = this.testCreate(services.size());
        for (URI u : services) {
            configUri = UriUtils.buildConfigUri(u);
            this.host.send(Operation.createPatch(configUri).setBody(updateBody)
                    .setCompletion(ctx.getCompletion()));
        }
        this.testWait(ctx);

        // get configuration and verify options
        TestContext ctx3 = testCreate(services.size());
        for (URI serviceUri : services) {
            URI u = UriUtils.buildConfigUri(serviceUri);
            host.send(Operation.createGet(u).setCompletion((o, e) -> {
                if (e != null) {
                    ctx3.failIteration(e);
                    return;
                }
                ServiceConfiguration cfg = o.getBody(ServiceConfiguration.class);
                if (!cfg.options.contains(ServiceOption.PERIODIC_MAINTENANCE)) {
                    ctx3.failIteration(new IllegalStateException(Utils.toJsonHtml(cfg)));
                    return;
                }

                if (!ServiceUriPaths.DEFAULT_1X_NODE_SELECTOR.equals(cfg.peerNodeSelectorPath)) {
                    ctx3.failIteration(new IllegalStateException(Utils.toJsonHtml(cfg)));
                    return;
                }
                ctx3.completeIteration();
            }));
        }
        testWait(ctx3);

        // since we enabled periodic maintenance, verify the new maintenance related stat is present
        this.host.waitFor("maintenance stat not present", () -> {
            for (URI u : services) {
                Map<String, ServiceStat> stats = this.host.getServiceStats(u);
                ServiceStat maintStat = stats.get(Service.STAT_NAME_MAINTENANCE_COUNT);
                if (maintStat == null) {
                    return false;
                }
                if (maintStat.latestValue == 0) {
                    return false;
                }
            }
            return true;
        });

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
    public void statRESTActions() throws Throwable {
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
        assertTrue(retStatEntry.sourceTimeMicrosUtc == null);

        // Step 3 - POST a stat with the same key again and verify that the
        // version and accumulated value are updated
        stat.latestValue = 50;
        stat.unit = "unit1";
        Long updatedMicrosUtc1 = Utils.getNowMicrosUtc();
        stat.sourceTimeMicrosUtc = updatedMicrosUtc1;
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        allStats = getStats(exampleServiceState);
        retStatEntry = allStats.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 150);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 2);
        assertTrue(retStatEntry.unit.equals("unit1"));
        assertTrue(retStatEntry.sourceTimeMicrosUtc == updatedMicrosUtc1);

        // Step 4 - POST a stat with a new key and verify that the
        // previously posted stat is not updated
        stat.name = "key2";
        stat.latestValue = 50;
        stat.unit = "unit2";
        Long updatedMicrosUtc2 = Utils.getNowMicrosUtc();
        stat.sourceTimeMicrosUtc = updatedMicrosUtc2;
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        allStats = getStats(exampleServiceState);
        retStatEntry = allStats.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 150);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 2);
        assertTrue(retStatEntry.unit.equals("unit1"));
        assertTrue(retStatEntry.sourceTimeMicrosUtc == updatedMicrosUtc1);

        retStatEntry = allStats.entries.get("key2");
        assertTrue(retStatEntry.accumulatedValue == 50);
        assertTrue(retStatEntry.latestValue == 50);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.unit.equals("unit2"));
        assertTrue(retStatEntry.sourceTimeMicrosUtc == updatedMicrosUtc2);

        // Step 5 - Issue a PUT for the first stat key and verify that the doc state is replaced
        stat.name = "key1";
        stat.latestValue = 75;
        stat.unit = "replaceUnit";
        stat.sourceTimeMicrosUtc = null;
        this.host.sendAndWaitExpectSuccess(Operation.createPut(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        allStats = getStats(exampleServiceState);
        retStatEntry = allStats.entries.get("key1");
        assertTrue(retStatEntry.accumulatedValue == 75);
        assertTrue(retStatEntry.latestValue == 75);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.unit.equals("replaceUnit"));
        assertTrue(retStatEntry.sourceTimeMicrosUtc == null);

        // Step 6 - Issue a bulk PUT and verify that the complete set of stats is updated
        ServiceStats stats = new ServiceStats();
        stat.name = "key3";
        stat.latestValue = 200;
        stat.unit = "unit3";
        stats.entries.put("key3", stat);
        this.host.sendAndWaitExpectSuccess(Operation.createPut(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stats));
        allStats = getStats(exampleServiceState);
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
        allStats = getStats(exampleServiceState);
        retStatEntry = allStats.entries.get("key3");
        assertTrue(retStatEntry.latestValue == 225);
        assertTrue(retStatEntry.version == 2);

        verifyGetWithODataOnStats(exampleServiceState);

        verifyStatCreationAttemptAfterGet();

    }

    private void verifyGetWithODataOnStats(ExampleServiceState exampleServiceState) {
        URI exampleStatsUri = UriUtils.buildStatsUri(this.host,
                exampleServiceState.documentSelfLink);
        // bulk PUT to set stats to a known state
        ServiceStats stats = new ServiceStats();
        stats.kind = ServiceStats.KIND;
        ServiceStat stat = new ServiceStat();
        stat.name = "key1";
        stat.latestValue = 100;
        stats.entries.put(stat.name, stat);
        stat = new ServiceStat();
        stat.name = "key2";
        stat.latestValue = 0.0;
        stats.entries.put(stat.name, stat);
        stat = new ServiceStat();
        stat.name = "key3";
        stat.latestValue = -200;
        stats.entries.put(stat.name, stat);
        stat = new ServiceStat();
        stat.name = "someKey" + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        stat.latestValue = 1000;
        stats.entries.put(stat.name, stat);
        stat = new ServiceStat();
        stat.name = "someOtherKey" + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        stat.latestValue = 2000;
        stats.entries.put(stat.name, stat);
        this.host.sendAndWaitExpectSuccess(Operation.createPut(exampleStatsUri).setBody(stats));

        // negative tests
        URI exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_COUNT, Boolean.TRUE.toString());
        this.host.sendAndWaitExpectFailure(Operation.createGet(exampleStatsUriWithODATA));

        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_ORDER_BY, "name");
        this.host.sendAndWaitExpectFailure(Operation.createGet(exampleStatsUriWithODATA));

        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_SKIP_TO, "100");
        this.host.sendAndWaitExpectFailure(Operation.createGet(exampleStatsUriWithODATA));

        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_TOP, "100");
        this.host.sendAndWaitExpectFailure(Operation.createGet(exampleStatsUriWithODATA));

        // attempt long value LE on latestVersion, should fail
        String odataFilterValue = String.format("%s le %d",
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                1001);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        this.host.sendAndWaitExpectFailure(Operation.createGet(exampleStatsUriWithODATA));

        // Positive filter tests
        String statName = "key1";
        // test filter for exact match
        odataFilterValue = String.format("%s eq %s",
                ServiceStat.FIELD_NAME_NAME,
                statName);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        ServiceStats filteredStats = getStats(exampleStatsUriWithODATA);
        assertTrue(filteredStats.entries.size() == 1);
        assertTrue(filteredStats.entries.containsKey(statName));

        // test filter with prefix match
        odataFilterValue = String.format("%s eq %s*",
                ServiceStat.FIELD_NAME_NAME,
                "key");
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        // three entries start with "key"
        assertTrue(filteredStats.entries.size() == 3);
        assertTrue(filteredStats.entries.containsKey("key1"));
        assertTrue(filteredStats.entries.containsKey("key2"));
        assertTrue(filteredStats.entries.containsKey("key3"));

        // test filter with suffix match, common for time series filtering
        odataFilterValue = String.format("%s eq *%s",
                ServiceStat.FIELD_NAME_NAME,
                ServiceStats.STAT_NAME_SUFFIX_PER_DAY);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        // two entries end with "Day"
        assertTrue(filteredStats.entries.size() == 2);
        assertTrue(filteredStats.entries
                .containsKey("someKey" + ServiceStats.STAT_NAME_SUFFIX_PER_DAY));
        assertTrue(filteredStats.entries
                .containsKey("someOtherKey" + ServiceStats.STAT_NAME_SUFFIX_PER_DAY));

        // filter on latestValue, GE
        odataFilterValue = String.format("%s ge %f",
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                0.0);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        assertTrue(filteredStats.entries.size() == 4);

        // filter on latestValue, GT
        odataFilterValue = String.format("%s gt %f",
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                0.0);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        assertTrue(filteredStats.entries.size() == 3);

        // filter on latestValue, eq
        odataFilterValue = String.format("%s eq %f",
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                -200.0);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        assertTrue(filteredStats.entries.size() == 1);

        // filter on latestValue, le
        odataFilterValue = String.format("%s le %f",
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                1000.0);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        assertTrue(filteredStats.entries.size() == 2);

        // filter on latestValue, lt AND gt
        odataFilterValue = String.format("%s lt %f and %s gt %f",
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                2000.0,
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                1000.0);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        // two entries end with "Day"
        assertTrue(filteredStats.entries.size() == 0);

        // test dual filter with suffix match, and latest value LEQ
        odataFilterValue = String.format("%s eq *%s and %s le %f",
                ServiceStat.FIELD_NAME_NAME,
                ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                ServiceStat.FIELD_NAME_LATEST_VALUE,
                1001.0);
        exampleStatsUriWithODATA = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        filteredStats = getStats(exampleStatsUriWithODATA);
        // single entry ends with "Day" and has latestValue <= 1000
        assertTrue(filteredStats.entries.size() == 1);
        assertTrue(filteredStats.entries
                .containsKey("someKey" + ServiceStats.STAT_NAME_SUFFIX_PER_DAY));

    }

    private void verifyStatCreationAttemptAfterGet() throws Throwable {
        // Create a stat without a log histogram or time series, then try to recreate with
        // the extra features and make sure its updated

        List<Service> services = this.host.doThroughputServiceStart(
                1, MinimalTestService.class,
                this.host.buildMinimalTestState(), EnumSet.of(ServiceOption.INSTRUMENTATION), null);

        final String statName = "foo";
        for (Service service : services) {
            service.setStat(statName, 1.0);
            ServiceStat st = service.getStat(statName);
            assertTrue(st.timeSeriesStats == null);
            assertTrue(st.logHistogram == null);
            ServiceStat stNew = new ServiceStat();
            stNew.name = statName;
            stNew.logHistogram = new ServiceStatLogHistogram();
            stNew.timeSeriesStats = new TimeSeriesStats(60,
                    TimeUnit.MINUTES.toMillis(1), EnumSet.of(AggregationType.AVG));
            service.setStat(stNew, 11.0);
            st = service.getStat(statName);
            assertTrue(st.timeSeriesStats != null);
            assertTrue(st.logHistogram != null);
        }
    }

    private ServiceStats getStats(ExampleServiceState exampleServiceState) {
        return this.host.getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink));
    }

    private ServiceStats getStats(URI statsUri) {
        return this.host.getServiceState(null, ServiceStats.class, statsUri);
    }

    @Test
    public void testTimeSeriesStats() throws Throwable {
        long startTime = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        int numBins = 4;
        long interval = 1000;
        double value = 100;
        // set data to fill up the specified number of bins
        TimeSeriesStats timeSeriesStats = new TimeSeriesStats(numBins, interval,
                EnumSet.allOf(AggregationType.class));
        for (int i = 0; i < numBins; i++) {
            startTime += TimeUnit.MILLISECONDS.toMicros(interval);
            value += 1;
            timeSeriesStats.add(startTime, value, 1);
        }
        assertTrue(timeSeriesStats.bins.size() == numBins);
        // insert additional unique datapoints; the earliest entries should be dropped
        for (int i = 0; i < numBins / 2; i++) {
            startTime += TimeUnit.MILLISECONDS.toMicros(interval);
            value += 1;
            timeSeriesStats.add(startTime, value, 1);
        }
        assertTrue(timeSeriesStats.bins.size() == numBins);
        long timeMicros = startTime - TimeUnit.MILLISECONDS.toMicros(interval * (numBins - 1));
        long timeMillis = TimeUnit.MICROSECONDS.toMillis(timeMicros);
        timeMillis -= (timeMillis % interval);
        assertTrue(timeSeriesStats.bins.firstKey() == timeMillis);

        // insert additional datapoints for an existing bin. The count should increase,
        // min, max, average computed appropriately
        double origValue = value;
        double accumulatedValue = value;
        double newValue = value;
        double count = 1;
        for (int i = 0; i < numBins / 2; i++) {
            newValue++;
            count++;
            timeSeriesStats.add(startTime, newValue, 2);
            accumulatedValue += newValue;
        }
        TimeBin lastBin = timeSeriesStats.bins.get(timeSeriesStats.bins.lastKey());
        assertTrue(lastBin.avg.equals(accumulatedValue / count));
        assertTrue(lastBin.sum.equals((2 * count) - 1));
        assertTrue(lastBin.count == count);
        assertTrue(lastBin.max.equals(newValue));
        assertTrue(lastBin.min.equals(origValue));
        assertTrue(lastBin.latest.equals(newValue));

        // test with a subset of the aggregation types specified
        timeSeriesStats = new TimeSeriesStats(numBins, interval, EnumSet.of(AggregationType.AVG));
        timeSeriesStats.add(startTime, value, value);
        lastBin = timeSeriesStats.bins.get(timeSeriesStats.bins.lastKey());
        assertTrue(lastBin.avg != null);
        assertTrue(lastBin.count != 0);
        assertTrue(lastBin.sum == null);
        assertTrue(lastBin.max == null);
        assertTrue(lastBin.min == null);

        timeSeriesStats = new TimeSeriesStats(numBins, interval, EnumSet.of(AggregationType.MIN,
                AggregationType.MAX));
        timeSeriesStats.add(startTime, value, value);
        lastBin = timeSeriesStats.bins.get(timeSeriesStats.bins.lastKey());
        assertTrue(lastBin.avg == null);
        assertTrue(lastBin.count == 0);
        assertTrue(lastBin.sum == null);
        assertTrue(lastBin.max != null);
        assertTrue(lastBin.min != null);

        timeSeriesStats = new TimeSeriesStats(numBins, interval, EnumSet.of(AggregationType.LATEST));
        timeSeriesStats.add(startTime, value, value);
        lastBin = timeSeriesStats.bins.get(timeSeriesStats.bins.lastKey());
        assertTrue(lastBin.avg == null);
        assertTrue(lastBin.count == 0);
        assertTrue(lastBin.sum == null);
        assertTrue(lastBin.max == null);
        assertTrue(lastBin.min == null);
        assertTrue(lastBin.latest.equals(value));

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
        // set bin size to 1ms
        stat.timeSeriesStats = new TimeSeriesStats(numBins, 1, EnumSet.allOf(AggregationType.class));
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        for (int i = 0; i < numBins; i++) {
            Thread.sleep(1);
            this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                    this.host, exampleServiceState.documentSelfLink)).setBody(stat));
        }
        ServiceStats allStats = this.host.getServiceState(null, ServiceStats.class,
                UriUtils.buildStatsUri(
                        this.host, exampleServiceState.documentSelfLink));
        ServiceStat retStatEntry = allStats.entries.get(stat.name);
        assertTrue(retStatEntry.accumulatedValue == 100 * (numBins + 1));
        assertTrue(retStatEntry.latestValue == 100);
        assertTrue(retStatEntry.version == numBins + 1);
        assertTrue(retStatEntry.timeSeriesStats.bins.size() == numBins);

        // Step 3 - POST a stat to the service instance with sourceTimeMicrosUtc and verify we can fetch the stat just posted
        String statName = UUID.randomUUID().toString();
        ExampleServiceState exampleState = new ExampleServiceState();
        exampleState.name = statName;
        Consumer<Operation> setter = (o) -> {
            o.setBody(exampleState);
        };
        Map<URI, ExampleServiceState> stateMap = this.host.doFactoryChildServiceStart(null, 1,
                ExampleServiceState.class, setter,
                UriUtils.buildFactoryUri(this.host, ExampleService.class));
        ExampleServiceState returnExampleState = stateMap.values().iterator().next();
        ServiceStats.ServiceStat sourceStat1 = new ServiceStat();
        sourceStat1.name = "sourceKey1";
        sourceStat1.latestValue = 100;
        // Timestamp 946713600000000 equals Jan 1, 2000
        Long sourceTimeMicrosUtc1 = 946713600000000L;
        sourceStat1.sourceTimeMicrosUtc = sourceTimeMicrosUtc1;
        ServiceStats.ServiceStat sourceStat2 = new ServiceStat();
        sourceStat2.name = "sourceKey2";
        sourceStat2.latestValue = 100;
        // Timestamp 946713600000000 equals Jan 2, 2000
        Long sourceTimeMicrosUtc2 = 946800000000000L;
        sourceStat2.sourceTimeMicrosUtc = sourceTimeMicrosUtc2;
        // set bucket size to 1ms
        sourceStat1.timeSeriesStats = new TimeSeriesStats(numBins, 1, EnumSet.allOf(AggregationType.class));
        sourceStat2.timeSeriesStats = new TimeSeriesStats(numBins, 1, EnumSet.allOf(AggregationType.class));
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, returnExampleState.documentSelfLink)).setBody(sourceStat1));
        this.host.sendAndWaitExpectSuccess(Operation.createPost(UriUtils.buildStatsUri(
                this.host, returnExampleState.documentSelfLink)).setBody(sourceStat2));
        allStats = getStats(returnExampleState);
        retStatEntry = allStats.entries.get(sourceStat1.name);
        assertTrue(retStatEntry.accumulatedValue == 100);
        assertTrue(retStatEntry.latestValue == 100);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.timeSeriesStats.bins.size() == 1);
        assertTrue(retStatEntry.timeSeriesStats.bins.firstKey()
                .equals(TimeUnit.MICROSECONDS.toMillis(sourceTimeMicrosUtc1)));

        retStatEntry = allStats.entries.get(sourceStat2.name);
        assertTrue(retStatEntry.accumulatedValue == 100);
        assertTrue(retStatEntry.latestValue == 100);
        assertTrue(retStatEntry.version == 1);
        assertTrue(retStatEntry.timeSeriesStats.bins.size() == 1);
        assertTrue(retStatEntry.timeSeriesStats.bins.firstKey()
                .equals(TimeUnit.MICROSECONDS.toMillis(sourceTimeMicrosUtc2)));
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

    public static void validateTimeSeriesStat(ServiceStat stat, long expectedBinDurationMillis) {
        assertTrue(stat != null);
        assertTrue(stat.timeSeriesStats != null);
        assertTrue(stat.version >= 1);
        assertEquals(expectedBinDurationMillis, stat.timeSeriesStats.binDurationMillis);
        if (stat.timeSeriesStats.aggregationType.contains(AggregationType.AVG)) {
            double maxCount = 0;
            for (TimeBin bin : stat.timeSeriesStats.bins.values()) {
                if (bin.count > maxCount) {
                    maxCount = bin.count;
                }
            }
            assertTrue(maxCount >= 1);
        }
    }

    @Test
    public void statsKeyOrder() {

        ExampleServiceState state = new ExampleServiceState();
        state.name = "foo";
        Operation post = Operation.createPost(this.host, ExampleService.FACTORY_LINK).setBody(state);
        state = this.sender.sendAndWait(post, ExampleServiceState.class);

        ServiceStats stats = new ServiceStats();
        ServiceStat stat = new ServiceStat();
        stat.name = "keyBBB";
        stat.latestValue = 10;
        stats.entries.put(stat.name, stat);
        stat = new ServiceStat();
        stat.name = "keyCCC";
        stat.latestValue = 10;
        stats.entries.put(stat.name, stat);
        stat = new ServiceStat();
        stat.name = "keyAAA";
        stat.latestValue = 10;
        stats.entries.put(stat.name, stat);

        URI exampleStatsUri = UriUtils.buildStatsUri(this.host, state.documentSelfLink);
        this.sender.sendAndWait(Operation.createPut(exampleStatsUri).setBody(stats));

        // odata stats prefix query
        String odataFilterValue = String.format("%s eq %s*", ServiceStat.FIELD_NAME_NAME, "key");
        URI filteredStats = UriUtils.extendUriWithQuery(exampleStatsUri,
                UriUtils.URI_PARAM_ODATA_FILTER, odataFilterValue);
        ServiceStats result = getStats(filteredStats);

        // verify stats key order
        assertEquals(3, result.entries.size());
        List<String> statList = new ArrayList<>(result.entries.keySet());
        assertEquals("stat index 0", "keyAAA", statList.get(0));
        assertEquals("stat index 1", "keyBBB", statList.get(1));
        assertEquals("stat index 2", "keyCCC", statList.get(2));
    }

}
