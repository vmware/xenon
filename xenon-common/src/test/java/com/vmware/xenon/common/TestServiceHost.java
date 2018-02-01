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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost.RequestRateInfo;
import com.vmware.xenon.common.ServiceHost.ServiceAlreadyStartedException;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.http.netty.NettyHttpListener;
import com.vmware.xenon.common.jwt.Rfc7519Claims;
import com.vmware.xenon.common.jwt.Signer;
import com.vmware.xenon.common.jwt.Verifier;
import com.vmware.xenon.common.test.AuthTestUtils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleNonPersistedService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleServiceHost;
import com.vmware.xenon.services.common.FileContentService;
import com.vmware.xenon.services.common.LuceneDocumentIndexService;
import com.vmware.xenon.services.common.MinimalFactoryTestService;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.OnDemandLoadFactoryService;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceHostLogService.LogServiceState;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UiFileContentService;
import com.vmware.xenon.services.common.UserService;

public class TestServiceHost {

    private static final int MAINTENANCE_INTERVAL_MILLIS = 100;

    private VerificationHost host;

    public String testURI;

    public int requestCount = 1000;

    public int rateLimitedRequestCount = 10;

    public int connectionCount = 32;

    public long serviceCount = 10;

    public int iterationCount = 1;

    public long testDurationSeconds = 0;

    public int indexFileThreshold = 100;

    public long serviceCacheClearDelaySeconds = 2;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();


    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(MAINTENANCE_INTERVAL_MILLIS));
    }

    private void setUp(boolean initOnly) throws Exception {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = VerificationHost.create(0);
        CommandLineArgumentParser.parseFromProperties(this.host);
        if (initOnly) {
            return;
        }

        try {
            this.host.start();
        } catch (Throwable e) {
            throw new Exception(e);
        }
    }

    @Test(expected = TimeoutException.class)
    public void startCoreServicesSynchronouslyWithTimeout() throws Throwable {
        setUp(false);

        // use reflection to shorten operation timeout value
        Field field = ServiceHost.class.getDeclaredField("state");
        field.setAccessible(true);
        ServiceHost.ServiceHostState state = (ServiceHostState) field.get(this.host);
        state.operationTimeoutMicros = TimeUnit.MILLISECONDS.toMicros(100);

        this.host.startCoreServicesSynchronously(new StatelessService() {
            @SuppressWarnings("unused")
            public static final String SELF_LINK = "/foo";

            @Override
            public void handleStart(Operation startPost) {
                // do not complete
            }
        });
    }

    @Test
    public void allocateExecutor() throws Throwable {
        setUp(false);
        Service s = this.host.startServiceAndWait(MinimalTestService.class, UUID.randomUUID()
                .toString());
        ExecutorService exec = this.host.allocateExecutor(s);
        this.host.testStart(1);
        exec.execute(() -> {
            this.host.completeIteration();
        });
        this.host.testWait();
    }

    @Test
    public void operationTracingFineFiner() throws Throwable {
        setUp(false);
        TestRequestSender sender = this.host.getTestRequestSender();
        this.host.toggleOperationTracing(this.host.getUri(), Level.FINE, true);
        // send some requests and confirm stats get populated

        URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null,
                this.serviceCount,
                ExampleServiceState.class, (op) -> {
                    ExampleServiceState st = new ExampleServiceState();
                    st.name = "foo";
                    op.setBody(st);
                }, factoryUri);
        TestContext ctx = this.host.testCreate(states.size() * 2);
        for (URI u : states.keySet()) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = this.host.nextUUID();
            sender.sendRequest(Operation.createGet(u).setCompletion(ctx.getCompletion()));
            sender.sendRequest(
                    Operation.createPatch(u)
                            .setContextId(this.host.nextUUID())
                            .setBody(state).setCompletion(ctx.getCompletion()));
        }
        ctx.await();
        ServiceStats after = sender.sendStatsGetAndWait(this.host.getManagementServiceUri());
        for (URI u : states.keySet()) {
            String getStatName = u.getPath() + ":" + Action.GET;
            String patchStatName = u.getPath() + ":" + Action.PATCH;
            ServiceStat getStat = after.entries.get(getStatName);
            assertTrue(getStat != null && getStat.latestValue > 0);
            ServiceStat patchStat = after.entries.get(patchStatName);
            assertTrue(patchStat != null && getStat.latestValue > 0);
        }
        this.host.toggleOperationTracing(this.host.getUri(), Level.FINE, false);

        // toggle on again, to FINER, confirm we get some log output
        this.host.toggleOperationTracing(this.host.getUri(), Level.FINER, true);

        // send some operations
        ctx = this.host.testCreate(states.size() * 2);
        for (URI u : states.keySet()) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = this.host.nextUUID();
            sender.sendRequest(Operation.createGet(u).setCompletion(ctx.getCompletion()));
            sender.sendRequest(
                    Operation.createPatch(u).setContextId(this.host.nextUUID()).setBody(state)
                            .setCompletion(ctx.getCompletion()));
        }
        ctx.await();

        LogServiceState logsAfterFiner = sender.sendGetAndWait(
                UriUtils.buildUri(this.host, ServiceUriPaths.PROCESS_LOG),
                LogServiceState.class);

        boolean foundTrace = false;
        for (String line : logsAfterFiner.items) {
            for (URI u : states.keySet()) {
                if (line.contains(u.getPath())) {
                    foundTrace = true;
                    break;
                }
            }
        }
        assertTrue(foundTrace);
    }

    @Test
    public void buildDocumentDescription() throws Throwable {
        setUp(false);
        URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null,
                this.serviceCount,
                ExampleServiceState.class, (op) -> {
                    ExampleServiceState st = new ExampleServiceState();
                    st.name = "foo";
                    op.setBody(st);
                }, factoryUri);

        // verify we have valid descriptions for all example services we created
        // explicitly
        validateDescriptions(states);

        // verify we can recover a description, even for services that are stopped
        TestContext ctx = this.host.testCreate(states.size());
        for (URI childUri : states.keySet()) {
            Operation delete = Operation.createDelete(childUri)
                    .setCompletion(ctx.getCompletion());
            this.host.send(delete);
        }
        this.host.testWait(ctx);

        // do the description lookup again, on stopped services
        validateDescriptions(states);
    }

    private void validateDescriptions(Map<URI, ExampleServiceState> states) {
        for (URI childUri : states.keySet()) {
            ServiceDocumentDescription desc = this.host
                    .buildDocumentDescription(childUri.getPath());
            // do simple verification of returned description, its not exhaustive
            assertTrue(desc != null);
            assertTrue(desc.serviceCapabilities.contains(ServiceOption.PERSISTENCE));
            assertTrue(desc.serviceCapabilities.contains(ServiceOption.INSTRUMENTATION));
            assertTrue(desc.propertyDescriptions.size() > 1);
            // check that a description was replaced with contents from HTML file
            assertTrue(desc.propertyDescriptions.get("keyValues").propertyDocumentation.startsWith("Key/Value"));
        }
    }

    @Test
    public void requestRateLimits() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        for (int i = 0; i < this.iterationCount; i++) {
            doRequestRateLimits();
            tearDown();
        }
    }

    private void doRequestRateLimits() throws Throwable {
        setUp(true);

        this.host.setAuthorizationService(new AuthorizationContextService());
        this.host.setAuthorizationEnabled(true);
        this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
        this.host.start();

        this.host.setSystemAuthorizationContext();

        String userPath = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, "example-user");
        String exampleUser = "example@localhost";
        TestContext authCtx = this.host.testCreate(1);
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserSelfLink(userPath)
                .setUserEmail(exampleUser)
                .setUserPassword(exampleUser)
                .setIsAdmin(false)
                .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                .setCompletion(authCtx.getCompletion())
                .start();
        authCtx.await();

        this.host.resetAuthorizationContext();

        this.host.assumeIdentity(userPath);

        URI factoryUri = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK);
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null,
                this.serviceCount,
                ExampleServiceState.class, (op) -> {
                    ExampleServiceState st = new ExampleServiceState();
                    st.name = exampleUser;
                    op.setBody(st);
                }, factoryUri);

        try {
            RequestRateInfo ri = new RequestRateInfo();
            this.host.setRequestRateLimit(userPath, ri);
            throw new IllegalStateException("call should have failed, rate limit is zero");
        } catch (IllegalArgumentException e) {

        }

        try {
            RequestRateInfo ri = new RequestRateInfo();
            // use a custom time series but of the wrong aggregation type
            ri.timeSeries = new TimeSeriesStats(10,
                    TimeUnit.SECONDS.toMillis(1),
                    EnumSet.of(AggregationType.AVG));
            this.host.setRequestRateLimit(userPath, ri);
            throw new IllegalStateException("call should have failed, aggregation is not SUM");
        } catch (IllegalArgumentException e) {

        }

        RequestRateInfo ri = new RequestRateInfo();
        ri.limit = 1.1;
        this.host.setRequestRateLimit(userPath, ri);
        // verify no side effects on instance we supplied
        assertTrue(ri.timeSeries == null);

        double limit = (this.rateLimitedRequestCount * this.serviceCount) / 100;

        // set limit for this user to 1 request / second, overwrite previous limit
        this.host.setRequestRateLimit(userPath, limit);

        ri = this.host.getRequestRateLimit(userPath);
        assertTrue(Double.compare(ri.limit, limit) == 0);
        assertTrue(!ri.options.isEmpty());
        assertTrue(ri.options.contains(RequestRateInfo.Option.FAIL));
        assertTrue(ri.timeSeries != null);
        assertTrue(ri.timeSeries.numBins == 60);
        assertTrue(ri.timeSeries.aggregationType.contains(AggregationType.SUM));

        // set maintenance to default time to see how throttling behaves with default interval
        this.host.setMaintenanceIntervalMicros(
                ServiceHostState.DEFAULT_MAINTENANCE_INTERVAL_MICROS);

        AtomicInteger failureCount = new AtomicInteger();
        AtomicInteger successCount = new AtomicInteger();

        // send N requests, at once, clearly violating the limit, and expect failures
        int count = this.rateLimitedRequestCount;
        TestContext ctx = this.host.testCreate(count * states.size());
        ctx.setTestName("Rate limiting with failure").logBefore();
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (o.getStatusCode() != Operation.STATUS_CODE_UNAVAILABLE) {
                    ctx.failIteration(e);
                    return;
                }
                failureCount.incrementAndGet();
            } else {
                successCount.incrementAndGet();
            }

            ctx.completeIteration();
        };

        ExampleServiceState patchBody = new ExampleServiceState();
        patchBody.name = Utils.getSystemNowMicrosUtc() + "";
        for (URI serviceUri : states.keySet()) {
            for (int i = 0; i < count; i++) {
                Operation op = Operation.createPatch(serviceUri)
                        .setBody(patchBody)
                        .forceRemote()
                        .setCompletion(c);
                this.host.send(op);
            }
        }
        this.host.testWait(ctx);
        ctx.logAfter();

        assertTrue(failureCount.get() > 0);

        // now change the options, and instead of fail, request throttling. this will literally
        // throttle the HTTP listener (does not work on local, in process calls)

        ri = new RequestRateInfo();
        ri.limit = limit;
        ri.options = EnumSet.of(RequestRateInfo.Option.PAUSE_PROCESSING);
        this.host.setRequestRateLimit(userPath, ri);
        this.host.assumeIdentity(userPath);

        ServiceStat rateLimitStatBefore = getRateLimitOpCountStat();
        if (rateLimitStatBefore == null) {
            rateLimitStatBefore = new ServiceStat();
            rateLimitStatBefore.latestValue = 0.0;
        }
        TestContext ctx2 = this.host.testCreate(count * states.size());
        ctx2.setTestName("Rate limiting with auto-read pause of channels").logBefore();
        for (URI serviceUri : states.keySet()) {
            for (int i = 0; i < count; i++) {
                // expect zero failures, but rate limit applied stat should have hits
                Operation op = Operation.createPatch(serviceUri)
                        .setBody(patchBody)
                        .forceRemote()
                        .setCompletion(ctx2.getCompletion());
                this.host.send(op);
            }
        }
        this.host.testWait(ctx2);
        ctx2.logAfter();
        ServiceStat rateLimitStatAfter = getRateLimitOpCountStat();
        assertTrue(rateLimitStatAfter.latestValue > rateLimitStatBefore.latestValue);

        this.host.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));

        // effectively remove limit, verify all requests complete
        ri = new RequestRateInfo();
        ri.limit = 1000000;
        ri.options = EnumSet.of(RequestRateInfo.Option.PAUSE_PROCESSING);
        this.host.setRequestRateLimit(userPath, ri);
        this.host.assumeIdentity(userPath);

        count = this.rateLimitedRequestCount;
        TestContext ctx3 = this.host.testCreate(count * states.size());
        ctx3.setTestName("No limit").logBefore();
        for (URI serviceUri : states.keySet()) {
            for (int i = 0; i < count; i++) {
                // expect zero failures
                Operation op = Operation.createPatch(serviceUri)
                        .setBody(patchBody)
                        .forceRemote()
                        .setCompletion(ctx3.getCompletion());
                this.host.send(op);
            }
        }
        this.host.testWait(ctx3);
        ctx3.logAfter();

        // verify rate limiting did not happen
        ServiceStat rateLimitStatExpectSame = getRateLimitOpCountStat();
        assertTrue(rateLimitStatAfter.latestValue == rateLimitStatExpectSame.latestValue);
    }

    @Test
    public void postFailureOnAlreadyStarted() throws Throwable {
        setUp(false);
        Service s = this.host.startServiceAndWait(MinimalTestService.class, UUID.randomUUID()
                .toString());
        this.host.testStart(1);
        Operation post = Operation.createPost(s.getUri()).setCompletion(
                (o, e) -> {
                    if (e == null) {
                        this.host.failIteration(new IllegalStateException(
                                "Request should have failed"));
                        return;
                    }

                    if (!(e instanceof ServiceAlreadyStartedException)) {
                        this.host.failIteration(new IllegalStateException(
                                "Request should have failed with different exception"));
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.startService(post, new MinimalTestService());
        this.host.testWait();
    }

    @Test
    public void startUpWithArgumentsAndHostConfigValidation() throws Throwable {
        setUp(false);
        ExampleServiceHost h = new ExampleServiceHost();
        try {
            String bindAddress = "127.0.0.1";
            URI publicUri = new URI("http://somehost.com:1234");
            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--sandbox=" + this.tmpFolder.getRoot().toURI(),
                    "--port=0",
                    "--bindAddress=" + bindAddress,
                    "--publicUri=" + publicUri.toString(),
                    "--id=" + hostId
            };

            h.initialize(args);

            // set memory limits for some services
            double queryTasksRelativeLimit = 0.1;
            double hostLimit = 0.29;
            h.setServiceMemoryLimit(ServiceHost.ROOT_PATH, hostLimit);
            h.setServiceMemoryLimit(ServiceUriPaths.CORE_QUERY_TASKS, queryTasksRelativeLimit);

            // attempt to set limit that brings total > 1.0
            try {
                h.setServiceMemoryLimit(ServiceUriPaths.CORE_OPERATION_INDEX, 0.99);
                throw new IllegalStateException("Should have failed");
            } catch (Throwable e) {

            }

            h.start();

            assertTrue(UriUtils.isHostEqual(h, publicUri));
            assertTrue(UriUtils.isHostEqual(h, new URI("http://127.0.0.1:" + h.getPort())));
            assertFalse(UriUtils.isHostEqual(h, new URI("https://somehost.com:" + h.getPort())));
            assertFalse(UriUtils.isHostEqual(h, new URI("http://somehost.com")));
            assertFalse(UriUtils.isHostEqual(h, new URI("http://somehost2.com:1234")));

            assertEquals(bindAddress, h.getPreferredAddress());

            assertEquals(bindAddress, h.getUri().getHost());

            assertEquals(hostId, h.getId());
            assertEquals(publicUri, h.getPublicUri());

            // confirm the node group self node entry uses the public URI for the bind address
            NodeGroupState ngs = this.host.getServiceState(null, NodeGroupState.class,
                    UriUtils.buildUri(h.getUri(), ServiceUriPaths.DEFAULT_NODE_GROUP));

            NodeState selfEntry = ngs.nodes.get(h.getId());
            assertEquals(publicUri.getHost(), selfEntry.groupReference.getHost());
            assertEquals(publicUri.getPort(), selfEntry.groupReference.getPort());

            // validate memory limits per service
            long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
            double hostRelativeLimit = hostLimit;
            double indexRelativeLimit = ServiceHost.DEFAULT_PCT_MEMORY_LIMIT_DOCUMENT_INDEX;

            long expectedHostLimitMB = (long) (maxMemory * hostRelativeLimit);
            Long hostLimitMB = h.getServiceMemoryLimitMB(ServiceHost.ROOT_PATH,
                    MemoryLimitType.EXACT);
            assertTrue("Expected host limit outside bounds",
                    Math.abs(expectedHostLimitMB - hostLimitMB) < 10);

            long expectedIndexLimitMB = (long) (maxMemory * indexRelativeLimit);
            Long indexLimitMB = h.getServiceMemoryLimitMB(ServiceUriPaths.CORE_DOCUMENT_INDEX,
                    MemoryLimitType.EXACT);
            assertTrue("Expected index service limit outside bounds",
                    Math.abs(expectedIndexLimitMB - indexLimitMB) < 10);

            long expectedQueryTaskLimitMB = (long) (maxMemory * queryTasksRelativeLimit);
            Long queryTaskLimitMB = h.getServiceMemoryLimitMB(ServiceUriPaths.CORE_QUERY_TASKS,
                    MemoryLimitType.EXACT);
            assertTrue("Expected host limit outside bounds",
                    Math.abs(expectedQueryTaskLimitMB - queryTaskLimitMB) < 10);

            // also check the water marks
            long lowW = h.getServiceMemoryLimitMB(ServiceUriPaths.CORE_QUERY_TASKS,
                    MemoryLimitType.LOW_WATERMARK);
            assertTrue("Expected  low watermark to be less than exact",
                    lowW < queryTaskLimitMB);

            long highW = h.getServiceMemoryLimitMB(ServiceUriPaths.CORE_QUERY_TASKS,
                    MemoryLimitType.HIGH_WATERMARK);
            assertTrue("Expected high watermark to be greater than low but less than exact",
                    highW > lowW && highW < queryTaskLimitMB);

            // attempt to set the limit for a service after a host has started, it should fail
            try {
                h.setServiceMemoryLimit(ServiceUriPaths.CORE_OPERATION_INDEX, 0.2);
                throw new IllegalStateException("Should have failed");
            } catch (Throwable e) {

            }

            // verify service host configuration file reflects command line arguments
            File s = new File(h.getStorageSandbox());
            s = new File(s, ServiceHost.SERVICE_HOST_STATE_FILE);

            this.host.testStart(1);
            ServiceHostState[] state = new ServiceHostState[1];
            Operation get = Operation.createGet(h.getUri()).setCompletion((o, e) -> {
                if (e != null) {
                    this.host.failIteration(e);
                    return;
                }
                state[0] = o.getBody(ServiceHostState.class);
                this.host.completeIteration();
            });
            FileUtils.readFileAndComplete(get, s);
            this.host.testWait();

            assertEquals(h.getStorageSandbox(), state[0].storageSandboxFileReference);
            assertEquals(h.getOperationTimeoutMicros(), state[0].operationTimeoutMicros);
            assertEquals(h.getMaintenanceIntervalMicros(), state[0].maintenanceIntervalMicros);
            assertEquals(bindAddress, state[0].bindAddress);
            assertEquals(h.getPort(), state[0].httpPort);
            assertEquals(hostId, state[0].id);

            // now stop the host, change some arguments, restart, verify arguments override config
            h.stop();

            bindAddress = "localhost";
            hostId = UUID.randomUUID().toString();

            String[] args2 = {
                    "--port=" + 0,
                    "--bindAddress=" + bindAddress,
                    "--sandbox=" + this.tmpFolder.getRoot().toURI(),
                    "--id=" + hostId
            };

            h.initialize(args2);
            h.start();

            assertEquals(bindAddress, h.getState().bindAddress);
            assertEquals(hostId, h.getState().id);

            verifyCoreServiceOption(h);
        } finally {
            h.stop();
        }

    }

    private void verifyCoreServiceOption(ExampleServiceHost h) {
        List<URI> coreServices = new ArrayList<>();
        URI defaultNodeGroup = UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_GROUP);
        URI defaultNodeSelector = UriUtils.buildUri(h, ServiceUriPaths.DEFAULT_NODE_SELECTOR);

        coreServices.add(UriUtils.buildConfigUri(defaultNodeGroup));
        coreServices.add(UriUtils.buildConfigUri(defaultNodeSelector));
        coreServices.add(UriUtils.buildConfigUri(h.getDocumentIndexServiceUri()));
        Map<URI, ServiceConfiguration> cfgs = this.host.getServiceState(null,
                ServiceConfiguration.class, coreServices);
        for (ServiceConfiguration c : cfgs.values()) {
            assertTrue(c.options.contains(ServiceOption.CORE));
        }
    }

    @Test
    public void setPublicUri() throws Throwable {
        setUp(false);
        ExampleServiceHost h = new ExampleServiceHost();

        try {

            // try invalid arguments
            ServiceHost.Arguments hostArgs = new ServiceHost.Arguments();
            hostArgs.publicUri = "";
            try {
                h.initialize(hostArgs);
                throw new IllegalStateException("should have failed");
            } catch (IllegalArgumentException e) {

            }

            hostArgs = new ServiceHost.Arguments();
            hostArgs.bindAddress = "";
            try {
                h.initialize(hostArgs);
                throw new IllegalStateException("should have failed");
            } catch (IllegalArgumentException e) {

            }

            hostArgs = new ServiceHost.Arguments();
            hostArgs.port = -2;
            try {
                h.initialize(hostArgs);
                throw new IllegalStateException("should have failed");
            } catch (IllegalArgumentException e) {

            }

            String bindAddress = "127.0.0.1";
            String publicAddress = "10.1.1.19";
            int publicPort = 1634;
            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--sandbox=" + this.tmpFolder.getRoot().getAbsolutePath(),
                    "--port=0",
                    "--bindAddress=" + bindAddress,
                    "--publicUri=" + new URI("http://" + publicAddress + ":" + publicPort),
                    "--id=" + hostId
            };

            h.initialize(args);
            h.start();

            assertEquals(bindAddress, h.getPreferredAddress());

            assertEquals(h.getPort(), h.getUri().getPort());
            assertEquals(bindAddress, h.getUri().getHost());

            // confirm that public URI takes precedence over bind address
            assertEquals(publicAddress, h.getPublicUri().getHost());
            assertEquals(publicPort, h.getPublicUri().getPort());

            // confirm the node group self node entry uses the public URI for the bind address
            NodeGroupState ngs = this.host.getServiceState(null, NodeGroupState.class,
                    UriUtils.buildUri(h.getUri(), ServiceUriPaths.DEFAULT_NODE_GROUP));

            NodeState selfEntry = ngs.nodes.get(h.getId());
            assertEquals(publicAddress, selfEntry.groupReference.getHost());
            assertEquals(publicPort, selfEntry.groupReference.getPort());
        } finally {
            h.stop();
        }

    }

    @Test
    public void jwtSecret() throws Throwable {
        setUp(false);

        Claims claims = new Claims.Builder().setSubject("foo").getResult();

        Signer bogusSigner = new Signer("bogus".getBytes());
        Signer defaultSigner = this.host.getTokenSigner();
        Verifier defaultVerifier = this.host.getTokenVerifier();

        String signedByBogus = bogusSigner.sign(claims);
        String signedByDefault = defaultSigner.sign(claims);

        try {
            defaultVerifier.verify(signedByBogus);
            fail("Signed by bogusSigner should be invalid for defaultVerifier.");
        } catch (Verifier.InvalidSignatureException ex) {
        }

        Rfc7519Claims verified = defaultVerifier.verify(signedByDefault);
        assertEquals("foo", verified.getSubject());

        this.host.stop();

        // assign cert and private-key. private-key is used for JWT seed.
        URI certFileUri = getClass().getResource("/ssl/server.crt").toURI();
        URI keyFileUri = getClass().getResource("/ssl/server.pem").toURI();

        this.host.setCertificateFileReference(certFileUri);
        this.host.setPrivateKeyFileReference(keyFileUri);
        // must assign port to zero, so we get a *new*, available port on restart.
        this.host.setPort(0);
        this.host.start();

        Signer newSigner = this.host.getTokenSigner();
        Verifier newVerifier = this.host.getTokenVerifier();

        assertNotSame("new signer must be created", defaultSigner, newSigner);
        assertNotSame("new verifier must be created", defaultVerifier, newVerifier);

        try {
            newVerifier.verify(signedByDefault);
            fail("Signed by defaultSigner should be invalid for newVerifier");
        } catch (Verifier.InvalidSignatureException ex) {
        }

        // sign by newSigner
        String signedByNewSigner = newSigner.sign(claims);

        verified = newVerifier.verify(signedByNewSigner);
        assertEquals("foo", verified.getSubject());

        try {
            defaultVerifier.verify(signedByNewSigner);
            fail("Signed by newSigner should be invalid for defaultVerifier");
        } catch (Verifier.InvalidSignatureException ex) {
        }

    }

    @Test
    public void startWithNonEncryptedPem() throws Throwable {
        ExampleServiceHost h = new ExampleServiceHost();
        String tmpFolderPath = this.tmpFolder.getRoot().getAbsolutePath();

        // We run test from filesystem so far, thus expect files to be on file system.
        // For example, if we run test from jar file, needs to copy the resource to tmp dir.
        Path certFilePath = Paths.get(getClass().getResource("/ssl/server.crt").toURI());
        Path keyFilePath = Paths.get(getClass().getResource("/ssl/server.pem").toURI());
        String certFile = certFilePath.toFile().getAbsolutePath();
        String keyFile = keyFilePath.toFile().getAbsolutePath();

        String[] args = {
                "--sandbox=" + tmpFolderPath,
                "--port=0",
                "--securePort=0",
                "--certificateFile=" + certFile,
                "--keyFile=" + keyFile
        };

        try {
            h.initialize(args);
            h.start();
        } finally {
            h.stop();
        }

        // with wrong password
        args = new String[]{
                "--sandbox=" + tmpFolderPath,
                "--port=0",
                "--securePort=0",
                "--certificateFile=" + certFile,
                "--keyFile=" + keyFile,
                "--keyPassphrase=WRONG_PASSWORD",
        };

        try {
            h.initialize(args);
            h.start();
            fail("Host should NOT start with password for non-encrypted pem key");
        } catch (Exception ex) {
        } finally {
            h.stop();
        }

    }

    @Test
    public void startWithEncryptedPem() throws Throwable {
        ExampleServiceHost h = new ExampleServiceHost();
        String tmpFolderPath = this.tmpFolder.getRoot().getAbsolutePath();

        // We run test from filesystem so far, thus expect files to be on file system.
        // For example, if we run test from jar file, needs to copy the resource to tmp dir.
        Path certFilePath = Paths.get(getClass().getResource("/ssl/server.crt").toURI());
        Path keyFilePath = Paths.get(getClass().getResource("/ssl/server-with-pass.p8").toURI());
        String certFile = certFilePath.toFile().getAbsolutePath();
        String keyFile = keyFilePath.toFile().getAbsolutePath();

        String[] args = {
                "--sandbox=" + tmpFolderPath,
                "--port=0",
                "--securePort=0",
                "--certificateFile=" + certFile,
                "--keyFile=" + keyFile,
                "--keyPassphrase=password",
        };

        try {
            h.initialize(args);
            h.start();
        } finally {
            h.stop();
        }

        // with wrong password
        args = new String[]{
                "--sandbox=" + tmpFolderPath,
                "--port=0",
                "--securePort=0",
                "--certificateFile=" + certFile,
                "--keyFile=" + keyFile,
                "--keyPassphrase=WRONG_PASSWORD",
        };

        try {
            h.initialize(args);
            h.start();
            fail("Host should NOT start with wrong password for encrypted pem key");
        } catch (Exception ex) {
        } finally {
            h.stop();
        }

        // with no password
        args = new String[]{
                "--sandbox=" + tmpFolderPath,
                "--port=0",
                "--securePort=0",
                "--certificateFile=" + certFile,
                "--keyFile=" + keyFile,
        };

        try {
            h.initialize(args);
            h.start();
            fail("Host should NOT start when no password is specified for encrypted pem key");
        } catch (Exception ex) {
        } finally {
            h.stop();
        }

    }

    @Test
    public void httpsOnly() throws Throwable {
        ExampleServiceHost h = new ExampleServiceHost();
        String tmpFolderPath = this.tmpFolder.getRoot().getAbsolutePath();

        // We run test from filesystem so far, thus expect files to be on file system.
        // For example, if we run test from jar file, needs to copy the resource to tmp dir.
        Path certFilePath = Paths.get(getClass().getResource("/ssl/server.crt").toURI());
        Path keyFilePath = Paths.get(getClass().getResource("/ssl/server.pem").toURI());
        String certFile = certFilePath.toFile().getAbsolutePath();
        String keyFile = keyFilePath.toFile().getAbsolutePath();

        // set -1 to disable http
        String[] args = {
                "--sandbox=" + tmpFolderPath,
                "--port=-1",
                "--securePort=0",
                "--certificateFile=" + certFile,
                "--keyFile=" + keyFile
        };

        try {
            h.initialize(args);
            h.start();

            assertNull("http should be disabled", h.getListener());
            assertNotNull("https should be enabled", h.getSecureListener());
        } finally {
            h.stop();
        }
    }


    @Test
    public void setAuthEnforcement() throws Throwable {
        setUp(false);
        ExampleServiceHost h = new ExampleServiceHost();
        try {
            String bindAddress = "127.0.0.1";
            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--sandbox=" + this.tmpFolder.getRoot().getAbsolutePath(),
                    "--port=0",
                    "--bindAddress=" + bindAddress,
                    "--isAuthorizationEnabled=" + Boolean.TRUE.toString(),
                    "--id=" + hostId
            };

            h.initialize(args);
            assertTrue(h.isAuthorizationEnabled());
            h.setAuthorizationEnabled(false);
            assertFalse(h.isAuthorizationEnabled());
            h.setAuthorizationEnabled(true);
            h.start();

            this.host.testStart(1);
            h.sendRequest(Operation
                    .createGet(UriUtils.buildUri(h.getUri(), ServiceUriPaths.DEFAULT_NODE_GROUP))
                    .setReferer(this.host.getReferer())
                    .setCompletion((o, e) -> {
                        if (o.getStatusCode() == Operation.STATUS_CODE_FORBIDDEN) {
                            this.host.completeIteration();
                            return;
                        }
                        this.host.failIteration(new IllegalStateException(
                                "Op succeded when failure expected"));
                    }));
            this.host.testWait();
        } finally {
            h.stop();
        }

    }

    @Test
    public void serviceStartExpiration() throws Throwable {
        setUp(false);
        long maintenanceIntervalMicros = TimeUnit.MILLISECONDS.toMicros(100);
        // set a small period so its pretty much guaranteed to execute
        // maintenance during this test
        this.host.setMaintenanceIntervalMicros(maintenanceIntervalMicros);

        // start a service but tell it to not complete the start POST. This will induce a timeout
        // failure from the host

        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = MinimalTestService.STRING_MARKER_TIMEOUT_REQUEST;
        this.host.testStart(1);
        Operation startPost = Operation
                .createPost(UriUtils.buildUri(this.host, UUID.randomUUID().toString()))
                .setExpiration(Utils.fromNowMicrosUtc(maintenanceIntervalMicros))
                .setBody(initialState)
                .setCompletion(this.host.getExpectedFailureCompletion());
        this.host.startService(startPost, new MinimalTestService());
        this.host.testWait();
    }

    @Test
    public void startServiceSelfLinkWithStar() throws Throwable {
        setUp(false);
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = this.host.nextUUID();
        TestContext ctx = this.host.testCreate(1);
        Operation startPost = Operation
                .createPost(UriUtils.buildUri(this.host, this.host.nextUUID() + "*"))
                .setBody(initialState).setCompletion(ctx.getExpectedFailureCompletion());
        this.host.startService(startPost, new MinimalTestService());
        this.host.testWait(ctx);
    }

    public static class StopOrderTestService extends StatefulService {

        public int stopOrder;

        public AtomicInteger globalStopOrder;

        public StopOrderTestService() {
            super(MinimalTestServiceState.class);
        }

        @Override
        public void handleStop(Operation delete) {
            this.stopOrder = this.globalStopOrder.incrementAndGet();
            delete.complete();
        }

    }

    public static class PrivilegedStopOrderTestService extends StatefulService {

        public int stopOrder;

        public AtomicInteger globalStopOrder;

        public PrivilegedStopOrderTestService() {
            super(MinimalTestServiceState.class);
        }

        @Override
        public void handleStop(Operation delete) {
            this.stopOrder = this.globalStopOrder.incrementAndGet();
            delete.complete();
        }

    }

    @Test
    public void serviceStopOrder() throws Throwable {
        setUp(false);

        // start a service but tell it to not complete the start POST. This will induce a timeout
        // failure from the host

        int serviceCount = 10;
        AtomicInteger order = new AtomicInteger(0);
        this.host.testStart(serviceCount);
        List<StopOrderTestService> normalServices = new ArrayList<>();
        for (int i = 0; i < serviceCount; i++) {
            MinimalTestServiceState initialState = new MinimalTestServiceState();
            initialState.id = UUID.randomUUID().toString();
            StopOrderTestService normalService = new StopOrderTestService();
            normalServices.add(normalService);
            normalService.globalStopOrder = order;
            Operation post = Operation.createPost(UriUtils.buildUri(this.host, initialState.id))
                    .setBody(initialState)
                    .setCompletion(this.host.getCompletion());
            this.host.startService(post, normalService);
        }
        this.host.testWait();


        this.host.addPrivilegedService(PrivilegedStopOrderTestService.class);
        List<PrivilegedStopOrderTestService> pServices = new ArrayList<>();
        this.host.testStart(serviceCount);
        for (int i = 0; i < serviceCount; i++) {
            MinimalTestServiceState initialState = new MinimalTestServiceState();
            initialState.id = UUID.randomUUID().toString();
            PrivilegedStopOrderTestService ps = new PrivilegedStopOrderTestService();
            pServices.add(ps);
            ps.globalStopOrder = order;
            Operation post = Operation.createPost(UriUtils.buildUri(this.host, initialState.id))
                    .setBody(initialState)
                    .setCompletion(this.host.getCompletion());
            this.host.startService(post, ps);
        }
        this.host.testWait();

        this.host.stop();

        for (PrivilegedStopOrderTestService pService : pServices) {
            for (StopOrderTestService normalService : normalServices) {
                this.host.log("normal order: %d, privileged: %d", normalService.stopOrder,
                        pService.stopOrder);
                assertTrue(normalService.stopOrder < pService.stopOrder);
            }
        }
    }

    @Test
    public void maintenanceAndStatsReporting() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        for (int i = 0; i < this.iterationCount; i++) {
            this.tearDown();
            doMaintenanceAndStatsReporting();
        }
    }

    private void doMaintenanceAndStatsReporting() throws Throwable {
        setUp(true);

        long maintIntervalMillis = 100;
        long maintenanceIntervalMicros = TimeUnit.MILLISECONDS.toMicros(maintIntervalMillis);
        this.host.setMaintenanceIntervalMicros(maintenanceIntervalMicros);
        this.host.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS
                .toMicros(maintIntervalMillis * 5));
        this.host.start();

        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.INSTRUMENTATION, ServiceOption.PERIODIC_MAINTENANCE);

        List<Service> services = this.host.doThroughputServiceStart(
                this.serviceCount, MinimalTestService.class, this.host.buildMinimalTestState(),
                caps,
                null);

        long start = System.nanoTime() / 1000;
        long slowMaintInterval = this.host.getMaintenanceIntervalMicros() * 10;
        List<Service> slowMaintServices = this.host.doThroughputServiceStart(null,
                this.serviceCount, MinimalTestService.class, this.host.buildMinimalTestState(),
                caps,
                null, slowMaintInterval);

        double maintCount = getHostMaintenanceCount();
        this.host.waitFor("wait for main.", () -> {
            double latestCount = getHostMaintenanceCount();
            return latestCount > maintCount + 10;
        });

        long end = System.nanoTime() / 1000;
        double expectedMaintIntervals = Math.max(1, (end - start) / slowMaintInterval);

        // verify that services with slow maintenance did not get more than one maint cycle
        URI[] statUris = buildStatsUris(this.serviceCount, slowMaintServices);
        Map<URI, ServiceStats> stats = this.host.getServiceState(null,
                ServiceStats.class, statUris);

        for (ServiceStats s : stats.values()) {

            for (ServiceStat st : s.entries.values()) {
                if (st.name.equals(Service.STAT_NAME_MAINTENANCE_COUNT)) {
                    // give a slop of 3 extra intervals:
                    // 1 due to rounding, 2 due to interval running before we do setMaintenance
                    // to a slower interval ( notice we start services, then set the interval)
                    if (st.latestValue > expectedMaintIntervals + 3) {
                        throw new IllegalStateException(
                                "too many maintenance runs for slow maint. service:"
                                        + st.latestValue);
                    }
                }
            }
        }

        this.host.testStart(services.size());
        // delete all minimal service instances
        for (Service s : services) {
            this.host.send(Operation.createDelete(s.getUri()).setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        this.host.testStart(slowMaintServices.size());
        // delete all slow minimal service instances
        for (Service s : slowMaintServices) {
            this.host.send(Operation.createDelete(s.getUri()).setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        // before we increase maintenance interval, verify stats reported by MGMT service
        verifyMgmtServiceStats();

        // now validate that service handleMaintenance does not get called right after start, but at least
        // one interval later. We set the interval to 30 seconds so we can verify it did not get called within
        // one second or so
        long maintMicros = TimeUnit.SECONDS.toMicros(30);
        this.host.setMaintenanceIntervalMicros(maintMicros);

        // there is a small race: if the host scheduled a maintenance task already, using the default
        // 1 second interval, its possible it executes maintenance on the newly added services using
        // the 1 second schedule, instead of 30 seconds. So wait at least one maint. interval with the
        // default interval
        Thread.sleep(1000);

        slowMaintServices = this.host.doThroughputServiceStart(
                this.serviceCount, MinimalTestService.class, this.host.buildMinimalTestState(),
                caps,
                null);

        // sleep again and check no maintenance run right after start
        Thread.sleep(250);

        statUris = buildStatsUris(this.serviceCount, slowMaintServices);
        stats = this.host.getServiceState(null,
                ServiceStats.class, statUris);

        for (ServiceStats s : stats.values()) {
            for (ServiceStat st : s.entries.values()) {
                if (st.name.equals(Service.STAT_NAME_MAINTENANCE_COUNT)) {
                    throw new IllegalStateException("Maintenance run before first expiration:"
                            + Utils.toJsonHtml(s));
                }
            }
        }

        // some services are at 100ms maintenance and the host is at 30 seconds, verify the
        // check maintenance interval is the minimum of the two
        long currentMaintInterval = this.host.getMaintenanceIntervalMicros();
        long currentCheckInterval = this.host.getMaintenanceCheckIntervalMicros();
        assertTrue(currentMaintInterval > currentCheckInterval);

        // create new set of services
        services = this.host.doThroughputServiceStart(
                this.serviceCount, MinimalTestService.class, this.host.buildMinimalTestState(),
                caps,
                null);

        // set the  interval for a service to something smaller than the host interval, then confirm
        // that only the maintenance *check* interval changed, not the host global maintenance interval, which
        // can affect all services
        for (Service s : services) {
            s.setMaintenanceIntervalMicros(currentCheckInterval / 2);
            break;
        }

        this.host.waitFor("check interval not updated", () -> {
            // verify the check interval is now lower
            if (currentCheckInterval / 2 != this.host.getMaintenanceCheckIntervalMicros()) {
                return false;
            }
            if (currentMaintInterval != this.host.getMaintenanceIntervalMicros()) {
                return false;
            }
            return true;
        });
    }

    private void verifyMgmtServiceStats() {
        URI serviceHostMgmtURI = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_MANAGEMENT);
        this.host.waitFor("wait for http stat update.", () -> {
            Operation get = Operation.createGet(this.host, ServiceHostManagementService.SELF_LINK);
            this.host.send(get.forceRemote());
            this.host.send(get.clone().forceRemote().setConnectionSharing(true));

            Map<String, ServiceStat> hostMgmtStats = this.host
                    .getServiceStats(serviceHostMgmtURI);
            ServiceStat http1ConnectionCountDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP11_CONNECTION_COUNT_PER_DAY);
            if (http1ConnectionCountDaily == null
                    || http1ConnectionCountDaily.version < 3) {
                return false;
            }
            ServiceStat http2ConnectionCountDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP2_CONNECTION_COUNT_PER_DAY);
            if (http2ConnectionCountDaily == null
                    || http2ConnectionCountDaily.version < 3) {
                return false;
            }
            return true;
        });


        this.host.waitFor("stats never populated", () -> {
            // confirm host global time series stats have been created / updated
            Map<String, ServiceStat> hostMgmtStats = this.host.getServiceStats(serviceHostMgmtURI);

            ServiceStat serviceCount = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_SERVICE_COUNT);
            if (serviceCount == null || serviceCount.latestValue < 2) {
                this.host.log("not ready: %s", Utils.toJson(serviceCount));
                return false;
            }

            ServiceStat freeMemDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_AVAILABLE_MEMORY_BYTES_PER_DAY);
            if (!isTimeSeriesStatReady(freeMemDaily)) {
                this.host.log("not ready: %s", Utils.toJson(freeMemDaily));
                return false;
            }
            ServiceStat freeMemHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_AVAILABLE_MEMORY_BYTES_PER_HOUR);
            if (!isTimeSeriesStatReady(freeMemHourly)) {
                this.host.log("not ready: %s", Utils.toJson(freeMemHourly));
                return false;
            }
            ServiceStat freeDiskDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_AVAILABLE_DISK_BYTES_PER_DAY);
            if (!isTimeSeriesStatReady(freeDiskDaily)) {
                this.host.log("not ready: %s", Utils.toJson(freeDiskDaily));
                return false;
            }
            ServiceStat freeDiskHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_AVAILABLE_DISK_BYTES_PER_HOUR);
            if (!isTimeSeriesStatReady(freeDiskHourly)) {
                this.host.log("not ready: %s", Utils.toJson(freeDiskHourly));
                return false;
            }
            ServiceStat cpuUsageDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_CPU_USAGE_PCT_PER_DAY);
            if (!isTimeSeriesStatReady(cpuUsageDaily)) {
                this.host.log("not ready: %s", Utils.toJson(cpuUsageDaily));
                return false;
            }
            ServiceStat cpuUsageHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_CPU_USAGE_PCT_PER_HOUR);
            if (!isTimeSeriesStatReady(cpuUsageHourly)) {
                this.host.log("not ready: %s", Utils.toJson(cpuUsageHourly));
                return false;
            }
            ServiceStat threadCountDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_JVM_THREAD_COUNT_PER_DAY);
            if (!isTimeSeriesStatReady(threadCountDaily)) {
                this.host.log("not ready: %s", Utils.toJson(threadCountDaily));
                return false;
            }
            ServiceStat threadCountHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_JVM_THREAD_COUNT_PER_HOUR);
            if (!isTimeSeriesStatReady(threadCountHourly)) {
                this.host.log("not ready: %s", Utils.toJson(threadCountHourly));
                return false;
            }

            ServiceStat http1PendingCountDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP11_PENDING_OP_COUNT_PER_DAY);
            if (!isTimeSeriesStatReady(http1PendingCountDaily)) {
                this.host.log("not ready: %s", Utils.toJson(http1PendingCountDaily));
                return false;
            }
            ServiceStat http1PendingCountHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP11_PENDING_OP_COUNT_PER_HOUR);
            if (!isTimeSeriesStatReady(http1PendingCountHourly)) {
                this.host.log("not ready: %s", Utils.toJson(http1PendingCountHourly));
                return false;
            }
            ServiceStat http2PendingCountDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP2_PENDING_OP_COUNT_PER_DAY);
            if (!isTimeSeriesStatReady(http2PendingCountDaily)) {
                this.host.log("not ready: %s", Utils.toJson(http2PendingCountDaily));
                return false;
            }
            ServiceStat http2PendingCountHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP2_PENDING_OP_COUNT_PER_HOUR);
            if (!isTimeSeriesStatReady(http2PendingCountHourly)) {
                this.host.log("not ready: %s", Utils.toJson(http2PendingCountHourly));
                return false;
            }

            ServiceStat http1AvailableConnectionCountDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP11_AVAILABLE_CONNECTION_COUNT_PER_DAY);
            if (!isTimeSeriesStatReady(http1AvailableConnectionCountDaily)) {
                this.host.log("not ready: %s", Utils.toJson(http1AvailableConnectionCountDaily));
                return false;
            }
            ServiceStat http1AvailableConnectionCountHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP11_AVAILABLE_CONNECTION_COUNT_PER_HOUR);
            if (!isTimeSeriesStatReady(http1AvailableConnectionCountHourly)) {
                this.host.log("not ready: %s", Utils.toJson(http1AvailableConnectionCountHourly));
                return false;
            }
            ServiceStat http2AvailableConnectionCountDaily = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP2_AVAILABLE_CONNECTION_COUNT_PER_DAY);
            if (!isTimeSeriesStatReady(http2AvailableConnectionCountDaily)) {
                this.host.log("not ready: %s", Utils.toJson(http2AvailableConnectionCountDaily));
                return false;
            }
            ServiceStat http2AvailableConnectionCountHourly = hostMgmtStats
                    .get(ServiceHostManagementService.STAT_NAME_HTTP2_AVAILABLE_CONNECTION_COUNT_PER_HOUR);
            if (!isTimeSeriesStatReady(http2AvailableConnectionCountHourly)) {
                this.host.log("not ready: %s", Utils.toJson(http2AvailableConnectionCountHourly));
                return false;
            }

            TestUtilityService.validateTimeSeriesStat(freeMemDaily, TimeUnit.HOURS.toMillis(1));
            TestUtilityService.validateTimeSeriesStat(freeMemHourly, TimeUnit.MINUTES.toMillis(1));
            TestUtilityService.validateTimeSeriesStat(freeDiskDaily, TimeUnit.HOURS.toMillis(1));
            TestUtilityService.validateTimeSeriesStat(freeDiskHourly, TimeUnit.MINUTES.toMillis(1));
            TestUtilityService.validateTimeSeriesStat(cpuUsageDaily, TimeUnit.HOURS.toMillis(1));
            TestUtilityService.validateTimeSeriesStat(cpuUsageHourly, TimeUnit.MINUTES.toMillis(1));
            TestUtilityService.validateTimeSeriesStat(threadCountDaily, TimeUnit.HOURS.toMillis(1));
            TestUtilityService.validateTimeSeriesStat(threadCountHourly,
                    TimeUnit.MINUTES.toMillis(1));
            return true;
        });
    }

    private boolean isTimeSeriesStatReady(ServiceStat st) {
        return st != null && st.timeSeriesStats != null;
    }

    @Test
    public void testCacheClearAndRefresh() throws Throwable {
        setUp(false);
        this.host.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS.toMicros(100));

        // no INSTRUMENTATION, as it prevents cache eviction
        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE, ServiceOption.FACTORY_ITEM);

        // Start the factory service. it will be needed to start services on-demand
        MinimalFactoryTestService factoryService = new MinimalFactoryTestService();
        factoryService.setChildServiceCaps(caps);
        this.host.startServiceAndWait(factoryService, "service", null);

        // Start some test services
        List<Service> services = this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class, this.host.buildMinimalTestState(), caps, null);

        // wait for some host maintenance intervals
        double maintCount = getHostMaintenanceCount();
        this.host.waitFor("wait for main.", () -> {
            double latestCount = getHostMaintenanceCount();
            return latestCount > maintCount + 1;
        });

        // verify services have stopped
        this.host.waitFor("wait for services to stop.", () -> {
            for (Service service : services) {
                if (this.host.getServiceStage(service.getSelfLink()) != null) {
                    return false;
                }
            }

            return true;
        });

        // reset cache clear delay to default value
        this.host.setServiceCacheClearDelayMicros(
                ServiceHostState.DEFAULT_SERVICE_CACHE_CLEAR_DELAY_MICROS);

        // Perform a GET on each service to repopulate the service state cache
        TestContext ctx = this.host.testCreate(services.size());
        for (Service service : services) {
            Operation get = Operation.createGet(service.getUri()).setCompletion(ctx.getCompletion());
            this.host.send(get);
        }
        this.host.testWait(ctx);

        // Now do many more overlapping gets -- since the operations above have returned, these
        // should all hit the cache.
        int requestCount = 10;
        ctx = this.host.testCreate(requestCount * services.size());
        for (Service service : services) {
            for (int i = 0; i < requestCount; i++) {
                Operation get = Operation.createGet(service.getUri()).setCompletion(ctx.getCompletion());
                this.host.send(get);
            }
        }
        this.host.testWait(ctx);

        Map<String, ServiceStat> mgmtStats = this.host.getServiceStats(this.host.getManagementServiceUri());

        // verify cache miss count
        ServiceStat cacheMissStat = mgmtStats.get(ServiceHostManagementService.STAT_NAME_SERVICE_CACHE_MISS_COUNT);
        assertNotNull(cacheMissStat);
        assertTrue(cacheMissStat.latestValue >= this.serviceCount);

        // verify cache hit count
        ServiceStat cacheHitStat = mgmtStats.get(ServiceHostManagementService.STAT_NAME_SERVICE_CACHE_HIT_COUNT);
        assertNotNull(cacheHitStat);
        assertTrue(cacheHitStat.latestValue >= requestCount * this.serviceCount);

        // now set host cache clear delay to a short value but the services' cached clear
        // delay to a long value, and verify that the services are stopped only after
        // the long value
        List<Service> cachedServices = this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class, this.host.buildMinimalTestState(), caps, null);
        for (Service service : cachedServices) {
            service.setCacheClearDelayMicros(TimeUnit.SECONDS.toMicros(5));
        }
        this.host.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS.toMicros(100));

        double newMaintCount = getHostMaintenanceCount();
        this.host.waitFor("wait for main.", () -> {
            double latestCount = getHostMaintenanceCount();
            return latestCount > newMaintCount + 1;
        });

        for (Service service : cachedServices) {
            assertEquals(ProcessingStage.AVAILABLE,
                    this.host.getServiceStage(service.getSelfLink()));
        }

        this.host.waitFor("wait for main.", () -> {
            double latestCount = getHostMaintenanceCount();
            return latestCount > newMaintCount + 5;
        });

        for (Service service : cachedServices) {
            ProcessingStage processingStage = this.host.getServiceStage(service.getSelfLink());
            assertTrue(processingStage == null || processingStage == ProcessingStage.STOPPED);
        }

    }

    @Test
    public void registerForServiceAvailabilityTimeout()
            throws Throwable {
        setUp(false);
        int c = 10;
        this.host.testStart(c);
        // issue requests to service paths we know do not exist, but induce the automatic
        // queuing behavior for service availability, by setting targetReplicated = true
        for (int i = 0; i < c; i++) {
            this.host.send(Operation
                    .createGet(UriUtils.buildUri(this.host, UUID.randomUUID().toString()))
                    .setExpiration(Utils.fromNowMicrosUtc(TimeUnit.SECONDS.toMicros(1)))
                    .setCompletion(this.host.getExpectedFailureCompletion()));
        }
        this.host.testWait();
    }

    @Test
    public void registerForFactoryServiceAvailability()
            throws Throwable {
        setUp(false);
        this.host.startFactoryServicesSynchronously(new TestFactoryService.SomeFactoryService(),
                SomeExampleService.createFactory());
        this.host.waitForServiceAvailable(SomeExampleService.FACTORY_LINK);
        this.host.waitForServiceAvailable(TestFactoryService.SomeFactoryService.SELF_LINK);
        try {
            // not a factory so will fail
            this.host.startFactoryServicesSynchronously(new ExampleService());
            throw new IllegalStateException("Should have failed");
        } catch (IllegalArgumentException e) {

        }

        try {
            // does not have SELF_LINK/FACTORY_LINK so will fail
            this.host.startFactoryServicesSynchronously(new MinimalFactoryTestService());
            throw new IllegalStateException("Should have failed");
        } catch (IllegalArgumentException e) {

        }
    }

    public static class SomeExampleService extends StatefulService {
        public static final String FACTORY_LINK = UUID.randomUUID().toString();

        public static Service createFactory() {
            return FactoryService.create(SomeExampleService.class, SomeExampleServiceState.class);
        }

        public SomeExampleService() {
            super(SomeExampleServiceState.class);
        }

        public static class SomeExampleServiceState extends ServiceDocument {
            public String name;
        }
    }

    @Test
    public void registerForServiceAvailabilityBeforeAndAfterMultiple()
            throws Throwable {
        setUp(false);
        int serviceCount = 100;
        this.host.testStart(serviceCount * 3);
        String[] links = new String[serviceCount];
        for (int i = 0; i < serviceCount; i++) {
            URI u = UriUtils.buildUri(this.host, UUID.randomUUID().toString());
            links[i] = u.getPath();
            this.host.registerForServiceAvailability(this.host.getCompletion(),
                    u.getPath());
            this.host.startService(Operation.createPost(u),
                    ExampleService.createFactory());
            this.host.registerForServiceAvailability(this.host.getCompletion(),
                    u.getPath());
        }
        this.host.registerForServiceAvailability(this.host.getCompletion(),
                links);

        this.host.testWait();
    }

    @Test
    public void registerForServiceAvailabilityWithReplicaBeforeAndAfterMultiple()
            throws Throwable {
        setUp(true);
        this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));

        String[] links = new String[]{
                ExampleService.FACTORY_LINK,
                ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                ServiceUriPaths.CORE_AUTHZ_ROLES,
                ServiceUriPaths.CORE_AUTHZ_USER_GROUPS};

        // register multiple factories, before host start
        TestContext ctx = this.host.testCreate(links.length * 10);
        for (int i = 0; i < 10; i++) {
            this.host.registerForServiceAvailability(ctx.getCompletion(), true, links);
        }
        this.host.start();
        this.host.testWait(ctx);

        // register multiple factories, after host start
        for (int i = 0; i < 10; i++) {
            ctx = this.host.testCreate(links.length);
            this.host.registerForServiceAvailability(ctx.getCompletion(), true, links);
            this.host.testWait(ctx);
        }

        // verify that the new replica aware service available works with child services
        int serviceCount = 10;
        ctx = this.host.testCreate(serviceCount * 3);
        links = new String[serviceCount];
        for (int i = 0; i < serviceCount; i++) {
            URI u = UriUtils.buildUri(this.host, UUID.randomUUID().toString());
            links[i] = u.getPath();
            this.host.registerForServiceAvailability(ctx.getCompletion(),
                    u.getPath());
            this.host.startService(Operation.createPost(u),
                    ExampleService.createFactory());
            this.host.registerForServiceAvailability(ctx.getCompletion(), true,
                    u.getPath());
        }
        this.host.registerForServiceAvailability(ctx.getCompletion(),
                links);

        this.host.testWait(ctx);
    }

    public static class ParentService extends StatefulService {

        public static final String FACTORY_LINK = "/test/parent";

        public static Service createFactory() {
            return FactoryService.create(ParentService.class);
        }

        public ParentService() {
            super(ExampleServiceState.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
        }
    }

    public static class ChildDependsOnParentService extends StatefulService {
        public static final String FACTORY_LINK = "/test/child-of-parent";

        public static Service createFactory() {
            return FactoryService.create(ChildDependsOnParentService.class);
        }

        public ChildDependsOnParentService() {
            super(ExampleServiceState.class);
            super.toggleOption(ServiceOption.PERSISTENCE, true);
        }

        @Override
        public void handleStart(Operation post) {
            // do not complete post for start, until we see a instance of the parent
            // being available. If there is an issue with factory start, this will
            // deadlock
            ExampleServiceState st = getBody(post);
            String id = Service.getId(st.documentSelfLink);
            String parentPath = UriUtils.buildUriPath(ParentService.FACTORY_LINK, id);
            post.nestCompletion((o, e) -> {
                if (e != null) {
                    post.fail(e);
                    return;
                }
                logInfo("Parent service started!");
                post.complete();
            });
            getHost().registerForServiceAvailability(post, parentPath);
        }
    }

    @Test
    public void registerForServiceAvailabilityWithCrossDependencies()
            throws Throwable {
        setUp(false);
        this.host.startFactoryServicesSynchronously(ParentService.createFactory(),
                ChildDependsOnParentService.createFactory());
        String id = UUID.randomUUID().toString();
        TestContext ctx = this.host.testCreate(2);
        // start a parent instance and a child instance.
        ExampleServiceState st = new ExampleServiceState();
        st.documentSelfLink = id;
        st.name = id;
        Operation post = Operation
                .createPost(UriUtils.buildUri(this.host, ParentService.FACTORY_LINK))
                .setCompletion(ctx.getCompletion())
                .setBody(st);
        this.host.send(post);
        post = Operation
                .createPost(UriUtils.buildUri(this.host, ChildDependsOnParentService.FACTORY_LINK))
                .setCompletion(ctx.getCompletion())
                .setBody(st);
        this.host.send(post);
        ctx.await();

        // we create the two persisted instances, and they started. Now stop the host and confirm restart occurs
        this.host.stop();
        this.host.setPort(0);
        if (!VerificationHost.restartStatefulHost(this.host, true)) {
            this.host.log("Failed restart of host, aborting");
            return;
        }
        this.host.startFactoryServicesSynchronously(ParentService.createFactory(),
                ChildDependsOnParentService.createFactory());

        // verify instance services started
        ctx = this.host.testCreate(1);
        String childPath = UriUtils.buildUriPath(ChildDependsOnParentService.FACTORY_LINK, id);
        Operation get = Operation.createGet(UriUtils.buildUri(this.host, childPath))
                .setCompletion(ctx.getCompletion());
        this.host.send(get);
        ctx.await();
    }

    @Test
    public void queueRequestForServiceWithNonFactoryParent() throws Throwable {
        setUp(false);
        class DelayedStartService extends StatelessService {
            @Override
            public void handleStart(Operation start) {
                getHost().schedule(() -> {
                    start.complete();
                }, 100, TimeUnit.MILLISECONDS);
            }

            @Override
            public void handleGet(Operation get) {
                get.complete();
            }
        }

        Operation startOp = Operation.createPost(UriUtils.buildUri(this.host, "/delayed"));
        this.host.startService(startOp, new DelayedStartService());

        // Don't wait for the service to be started, because it intentionally takes a while.
        // The GET operation below should be queued until the service's start completes.
        Operation getOp = Operation
                .createGet(UriUtils.buildUri(this.host, "/delayed"))
                .setCompletion(this.host.getCompletion());
        this.host.testStart(1);
        this.host.send(getOp);
        this.host.testWait();
    }

    @Test
    public void serviceStopDueToMemoryPressure() throws Throwable {
        setUp(true);
        this.host.setAuthorizationService(new AuthorizationContextService());
        this.host.setAuthorizationEnabled(true);

        if (this.serviceCount >= 1000) {
            this.host.setStressTest(true);
        }

        // Set the threshold low to induce it during this test, several times. This will
        // verify that refreshing the index writer does not break the index semantics
        LuceneDocumentIndexService
                .setIndexFileCountThresholdForWriterRefresh(this.indexFileThreshold);

        // set memory limit low to force service stop
        this.host.setServiceMemoryLimit(ServiceHost.ROOT_PATH, 0.00001);
        beforeHostStart(this.host);

        this.host.setPort(0);
        long delayMicros = TimeUnit.SECONDS
                .toMicros(this.serviceCacheClearDelaySeconds);
        this.host.setServiceCacheClearDelayMicros(delayMicros);

        // disable auto sync since it might cause a false negative (skipped pauses) when
        // it kicks in within a few milliseconds from host start, during induced pause
        this.host.setPeerSynchronizationEnabled(false);
        long delayMicrosAfter = this.host.getServiceCacheClearDelayMicros();
        assertTrue(delayMicros == delayMicrosAfter);
        this.host.start();

        this.host.setSystemAuthorizationContext();
        TestContext ctxQuery = this.host.testCreate(1);
        String user = "foo@bar.com";
        Query.Builder queryBuilder = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND, Utils.buildKind(ExampleServiceState.class));
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(user)
                .setUserSelfLink(user)
                .setUserPassword(user)
                .setResourceQuery(queryBuilder.build())
                .setCompletion((ex) -> {
                    if (ex != null) {
                        ctxQuery.failIteration(ex);
                        return;
                    }
                    ctxQuery.completeIteration();
                }).start();
        ctxQuery.await();
        String factoryLink = OnDemandLoadFactoryService.create(this.host);
        URI factoryURI = UriUtils.buildUri(this.host, factoryLink);
        this.host.resetSystemAuthorizationContext();

        AtomicLong selfLinkCounter = new AtomicLong();
        String prefix = "instance-";
        String name = UUID.randomUUID().toString();
        ExampleServiceState s = new ExampleServiceState();
        s.name = name;
        Consumer<Operation> bodySetter = (o) -> {
            s.documentSelfLink = prefix + selfLinkCounter.incrementAndGet();
            o.setBody(s);
        };

        // Create a number of child services.
        this.host.assumeIdentity(UriUtils.buildUriPath(UserService.FACTORY_LINK, user));

        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null,
                this.serviceCount,
                ExampleServiceState.class, bodySetter, factoryURI);

        // Wait for the next maintenance interval to trigger. This will stop all the services
        // we just created since the memory limit was set so low.
        long expectedStopTime = Utils.fromNowMicrosUtc(this.host
                .getMaintenanceIntervalMicros() * 5);
        while (this.host.getState().lastMaintenanceTimeUtcMicros < expectedStopTime) {
            // memory limits are applied during maintenance, so wait for a few intervals.
            Thread.sleep(this.host.getMaintenanceIntervalMicros() / 1000);
        }

        // Let's now issue some updates to verify stopped services get started.
        int updateCount = 100;
        if (this.testDurationSeconds > 0 || this.host.isStressTest()) {
            updateCount = 1;
        }
        patchExampleServices(states, updateCount);

        TestContext ctxGet = this.host.testCreate(states.size());
        for (ExampleServiceState st : states.values()) {
            Operation get = Operation.createGet(UriUtils.buildUri(this.host, st.documentSelfLink))
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    this.host.failIteration(e);
                                    return;
                                }

                                ExampleServiceState rsp = o.getBody(ExampleServiceState.class);
                                if (!rsp.name.startsWith("updated")) {
                                    ctxGet.fail(new IllegalStateException(Utils
                                            .toJsonHtml(rsp)));
                                    return;
                                }
                                ctxGet.complete();
                            });
            this.host.send(get);
        }
        this.host.testWait(ctxGet);

        // Let's set the service memory limit back to normal and issue more updates to ensure
        // that the services still continue to operate as expected.
        this.host
                .setServiceMemoryLimit(ServiceHost.ROOT_PATH, ServiceHost.DEFAULT_PCT_MEMORY_LIMIT);
        patchExampleServices(states, updateCount);

        states.clear();
        // Long running test. Keep adding services, expecting stop to occur and free up memory so the
        // number of service instances exceeds available memory.
        Date exp = new Date(TimeUnit.MICROSECONDS.toMillis(
                Utils.getSystemNowMicrosUtc())
                + TimeUnit.SECONDS.toMillis(this.testDurationSeconds));

        this.host.setOperationTimeOutMicros(
                TimeUnit.SECONDS.toMicros(this.host.getTimeoutSeconds()));

        while (new Date().before(exp)) {
            states = this.host.doFactoryChildServiceStart(null,
                    this.serviceCount,
                    ExampleServiceState.class, bodySetter, factoryURI);
            Thread.sleep(500);

            this.host.log("created %d services, created so far: %d, attached count: %d",
                    this.serviceCount,
                    selfLinkCounter.get(),
                    this.host.getState().serviceCount);
            Runtime.getRuntime().gc();
            this.host.logMemoryInfo();

            File f = new File(this.host.getStorageSandbox());
            this.host.log("Sandbox: %s, Disk: free %d, usable: %d, total: %d", f.toURI(),
                    f.getFreeSpace(),
                    f.getUsableSpace(),
                    f.getTotalSpace());

            // let a couple of maintenance intervals run
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(this.host.getMaintenanceIntervalMicros()) * 2);

            // ping every service we created to see if they can be started
            TestContext getCtx = this.host.testCreate(states.size());
            for (URI u : states.keySet()) {
                Operation get = Operation.createGet(u).setCompletion((o, e) -> {
                    if (e == null) {
                        getCtx.complete();
                        return;
                    }

                    if (o.getStatusCode() == Operation.STATUS_CODE_TIMEOUT) {
                        // check the document index, if we ever created this service
                        try {
                            this.host.createAndWaitSimpleDirectQuery(
                                    ServiceDocument.FIELD_NAME_SELF_LINK, o.getUri().getPath(), 1, 1);
                        } catch (Throwable e1) {
                            getCtx.fail(e1);
                            return;
                        }
                    }
                    getCtx.fail(e);
                });
                this.host.send(get);
            }
            this.host.testWait(getCtx);

            long limit = this.serviceCount * 30;
            if (selfLinkCounter.get() <= limit) {
                continue;
            }

            TestContext ctxDelete = this.host.testCreate(states.size());
            // periodically, delete services we created (and likely stopped) several passes ago
            for (int i = 0; i < states.size(); i++) {
                String childPath = UriUtils.buildUriPath(factoryURI.getPath(), prefix + ""
                        + (selfLinkCounter.get() - limit + i));
                Operation delete = Operation.createDelete(this.host, childPath);
                delete.setCompletion((o, e) -> {
                    ctxDelete.complete();
                });
                this.host.send(delete);
            }
            ctxDelete.await();
        }
    }

    @Test
    public void maintenanceForOnDemandLoadServices() throws Throwable {
        setUp(true);

        long maintenanceIntervalMillis = 100;
        long maintenanceIntervalMicros = TimeUnit.MILLISECONDS
                .toMicros(maintenanceIntervalMillis);

        // induce host to clear service state cache
        this.host.setMaintenanceIntervalMicros(maintenanceIntervalMicros);
        this.host.setServiceCacheClearDelayMicros(maintenanceIntervalMicros / 2);
        this.host.start();

        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.INSTRUMENTATION, ServiceOption.FACTORY_ITEM);

        // Start the factory service. it will be needed to start services on-demand
        MinimalFactoryTestService factoryService = new MinimalFactoryTestService();
        factoryService.setChildServiceCaps(caps);
        this.host.startServiceAndWait(factoryService, "service", null);

        // Start some test services
        this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class, this.host.buildMinimalTestState(), caps, null);

        // guarantee at least a few maintenance intervals have passed.
        Thread.sleep(maintenanceIntervalMillis * 10);

        // Let's verify now that all of the services have stopped by now.
        this.host.waitFor(
                "Service stats did not get updated",
                () -> {
                    Map<String, ServiceStat> stats = this.host.getServiceStats(this.host
                            .getManagementServiceUri());

                    ServiceStat cacheClears = stats
                            .get(ServiceHostManagementService.STAT_NAME_SERVICE_CACHE_CLEAR_COUNT);
                    if (cacheClears == null || cacheClears.latestValue < this.serviceCount) {
                        this.host.log(
                                "Service Cache Clears %s were less than expected %d",
                                cacheClears == null ? "null" : String
                                        .valueOf(cacheClears.latestValue),
                                this.serviceCount);
                        return false;
                    }

                    return true;
                });
    }

    private void patchExampleServices(Map<URI, ExampleServiceState> states, int count)
            throws Throwable {
        TestContext ctx = this.host.testCreate(states.size() * count);
        for (ExampleServiceState st : states.values()) {
            for (int i = 0; i < count; i++) {
                st.name = "updated" + Utils.getNowMicrosUtc() + "";
                Operation patch = Operation
                        .createPatch(UriUtils.buildUri(this.host, st.documentSelfLink))
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                ctx.fail(e);
                                return;
                            }
                            ctx.complete();
                        }).setBody(st);
                this.host.send(patch);
            }
        }
        this.host.testWait(ctx);
    }

    @Test
    public void onDemandServiceStopCheckWithReadAndWriteAccess() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            tearDown();
            doOnDemandServiceStopCheckWithReadAndWriteAccess();
        }
    }

    private void doOnDemandServiceStopCheckWithReadAndWriteAccess() throws Throwable {
        setUp(true);

        long maintenanceIntervalMicros = TimeUnit.MILLISECONDS.toMicros(100);

        // induce host to stop service more often by setting maintenance interval short
        this.host.setMaintenanceIntervalMicros(maintenanceIntervalMicros);
        this.host.setServiceCacheClearDelayMicros(maintenanceIntervalMicros / 2);
        this.host.start();

        // Start some test services
        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.FACTORY_ITEM);

        MinimalFactoryTestService factoryService = new MinimalFactoryTestService();
        factoryService.setChildServiceCaps(caps);
        this.host.startServiceAndWait(factoryService, "/service", null);

        // Test DELETE works on ODL service as it works on non-ODL service.
        // Delete on non-existent service should succeed, and should not leave any side effects behind.
        Operation deleteOp = Operation.createDelete(this.host, "/service/foo")
                .setBody(new ServiceDocument());
        this.host.sendAndWaitExpectSuccess(deleteOp);

        // create a service
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "foo";
        initialState.documentSelfLink = "/foo";
        Operation startPost = Operation
                .createPost(UriUtils.buildUri(this.host, "/service"))
                .setBody(initialState);
        this.host.sendAndWaitExpectSuccess(startPost);

        String servicePath = "/service/foo";

        // wait for the service to be stopped.
        // This verifies that a service will stop while it is idle for some duration
        this.host.waitFor("Waiting for service to be stopped",
                () -> this.host.getServiceStage(servicePath) == null
        );

        int requestCount = 10;
        int requestDelayMills = 40;

        // send 10 GET request 40ms apart to make service receive GET request during a couple
        // of maintenance windows
        TestContext testContextForGet = this.host.testCreate(requestCount);
        for (int i = 0; i < requestCount; i++) {
            Operation get = Operation
                    .createGet(this.host, servicePath)
                    .setCompletion(testContextForGet.getCompletion());
            this.host.send(get);
            Thread.sleep(requestDelayMills);
        }
        testContextForGet.await();

        // wait for the service to be stopped
        this.host.waitFor("Waiting for service to be stopped",
                () -> this.host.getServiceStage(servicePath) == null
        );

        // send 10 update request 40ms apart to make service receive PATCH request during a couple
        // of maintenance windows
        TestContext ctx = this.host.testCreate(requestCount);
        for (int i = 0; i < requestCount; i++) {
            Operation patch = createMinimalTestServicePatch(servicePath, ctx);
            this.host.send(patch);
            Thread.sleep(requestDelayMills);
        }
        ctx.await();

        // wait for the service to be stopped
        this.host.waitFor("Waiting for service to be stopped",
                () -> this.host.getServiceStage(servicePath) == null
        );

        double maintCount = getHostMaintenanceCount();
        // issue multiple PATCHs while directly stopping the service to induce collision
        // of stop with active requests. First prevent automatic stop by extending
        // cache clear time
        this.host.setServiceCacheClearDelayMicros(TimeUnit.DAYS.toMicros(1));
        this.host.waitFor("wait for main.", () -> {
            double latestCount = getHostMaintenanceCount();
            return latestCount > maintCount + 1;
        });


        // first cause a start
        Operation patch = createMinimalTestServicePatch(servicePath, null);
        this.host.sendAndWaitExpectSuccess(patch);

        assertEquals(ProcessingStage.AVAILABLE, this.host.getServiceStage(servicePath));

        requestCount = this.requestCount;
        // service is started. issue updates in parallel and then stop service while requests are
        // still being issued
        ctx = this.host.testCreate(requestCount);
        for (int i = 0; i < requestCount; i++) {
            patch = createMinimalTestServicePatch(servicePath, ctx);
            this.host.send(patch);
            if (i == Math.min(10, requestCount / 2)) {
                Operation deleteStop = Operation.createDelete(this.host, servicePath)
                        .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE);
                this.host.send(deleteStop);
            }
        }
        ctx.await();

        verifyOnDemandLoadUpdateDeleteContention();
    }

    void verifyOnDemandLoadUpdateDeleteContention() throws Throwable {
        Operation patch;
        Consumer<Operation> bodySetter = (o) -> {
            ExampleServiceState body = new ExampleServiceState();
            body.name = "prefix-" + UUID.randomUUID();
            o.setBody(body);
        };
        String factoryLink = OnDemandLoadFactoryService.create(this.host);

        // before we start service attempt a GET on a ODL service we know does not
        // exist. Make sure its handleStart is NOT called (we will fail the POST if handleStart
        // is called, with no body)

        Operation get = Operation.createGet(UriUtils.buildUri(
                this.host, UriUtils.buildUriPath(factoryLink, "does-not-exist")));
        this.host.sendAndWaitExpectFailure(get, Operation.STATUS_CODE_NOT_FOUND);

        // create another set of services
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(
                null,
                this.serviceCount,
                ExampleServiceState.class,
                bodySetter,
                UriUtils.buildUri(this.host, factoryLink));

        // set aggressive cache clear again so ODL services stop
        // temporarily disabled - https://jira-hzn.eng.vmware.com/browse/VRXEN-21
        /*
        double nowCount = getHostMaintenanceCount();
        this.host.setServiceCacheClearDelayMicros(this.host.getMaintenanceIntervalMicros() / 2);
        this.host.waitFor("wait for main.", () -> {
            double latestCount = getHostMaintenanceCount();
            return latestCount > nowCount + 1;
        });
        */

        // now patch these services, while we issue deletes. The PATCHs can fail, but not
        // the DELETEs
        TestContext patchAndDeleteCtx = this.host.testCreate(states.size() * 2);
        patchAndDeleteCtx.setTestName("Concurrent PATCH / DELETE on ODL").logBefore();
        for (Entry<URI, ExampleServiceState> e : states.entrySet()) {
            patch = Operation.createPatch(e.getKey())
                    .setBody(e.getValue())
                    .setCompletion((o, ex) -> {
                        patchAndDeleteCtx.complete();
                    });
            this.host.send(patch);
            // in parallel send a DELETE
            this.host.send(Operation.createDelete(e.getKey())
                    .setCompletion(patchAndDeleteCtx.getCompletion()));
        }
        patchAndDeleteCtx.await();
        patchAndDeleteCtx.logAfter();
    }

    double getHostMaintenanceCount() {
        Map<String, ServiceStat> hostStats = this.host.getServiceStats(
                UriUtils.buildUri(this.host, ServiceHostManagementService.SELF_LINK));
        ServiceStat stat = hostStats.get(Service.STAT_NAME_SERVICE_HOST_MAINTENANCE_COUNT);
        if (stat == null) {
            return 0.0;
        }
        return stat.latestValue;
    }

    Operation createMinimalTestServicePatch(String servicePath, TestContext ctx) {
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = Utils.buildUUID("foo");
        Operation patch = Operation
                .createPatch(UriUtils.buildUri(this.host, servicePath))
                .setBody(body);
        if (ctx != null) {
            patch.setCompletion(ctx.getCompletion());
        }
        return patch;
    }

    private ServiceStat getRateLimitOpCountStat() throws Throwable {
        URI managementServiceUri = this.host.getManagementServiceUri();
        return this.host.getServiceStats(managementServiceUri)
                .get(ServiceHostManagementService.STAT_NAME_RATE_LIMITED_OP_COUNT);
    }

    @Test
    public void thirdPartyClientPost() throws Throwable {
        setUp(false);
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        String name = UUID.randomUUID().toString();
        ExampleServiceState s = new ExampleServiceState();
        s.name = name;
        Consumer<Operation> bodySetter = (o) -> {
            o.setBody(s);
        };

        URI factoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);
        long c = 1;
        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null, c,
                ExampleServiceState.class, bodySetter, factoryURI);

        String contentType = Operation.MEDIA_TYPE_APPLICATION_JSON;
        for (ExampleServiceState initialState : states.values()) {
            String json = this.host.sendWithJavaClient(
                    UriUtils.buildUri(this.host, initialState.documentSelfLink), contentType, null);
            ExampleServiceState javaClientRsp = Utils.fromJson(json, ExampleServiceState.class);
            assertTrue(javaClientRsp.name.equals(initialState.name));
        }

        // Now issue POST with third party client
        s.name = UUID.randomUUID().toString();
        String body = Utils.toJson(s);
        // first use proper content type
        String json = this.host.sendWithJavaClient(factoryURI,
                Operation.MEDIA_TYPE_APPLICATION_JSON, body);
        ExampleServiceState javaClientRsp = Utils.fromJson(json, ExampleServiceState.class);
        assertTrue(javaClientRsp.name.equals(s.name));

        // POST to a service we know does not exist and verify our request did not get implicitly
        // queued, but failed instantly instead

        json = this.host.sendWithJavaClient(
                UriUtils.extendUri(factoryURI, UUID.randomUUID().toString()),
                Operation.MEDIA_TYPE_APPLICATION_JSON, null);

        ServiceErrorResponse r = Utils.fromJson(json, ServiceErrorResponse.class);
        assertEquals(Operation.STATUS_CODE_NOT_FOUND, r.statusCode);
    }

    private URI[] buildStatsUris(long serviceCount, List<Service> services) {
        URI[] statUris = new URI[(int) serviceCount];
        int i = 0;
        for (Service s : services) {
            statUris[i++] = UriUtils.extendUri(s.getUri(),
                    ServiceHost.SERVICE_URI_SUFFIX_STATS);
        }
        return statUris;
    }

    @Test
    public void queryServiceUris() throws Throwable {
        setUp(false);
        int serviceCount = 5;
        this.host.createExampleServices(this.host, serviceCount, Utils.getNowMicrosUtc());

        EnumSet<ServiceOption> options = EnumSet.of(ServiceOption.INSTRUMENTATION,
                ServiceOption.OWNER_SELECTION, ServiceOption.FACTORY_ITEM);

        Operation get = Operation.createGet(this.host.getUri());
        final ServiceDocumentQueryResult[] results = new ServiceDocumentQueryResult[1];

        get.setCompletion((o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }
            results[0] = o.getBody(ServiceDocumentQueryResult.class);
            this.host.completeIteration();
        });

        // use path prefix match
        this.host.testStart(1);
        this.host.queryServiceUris(ExampleService.FACTORY_LINK + "/*", get.clone());
        this.host.testWait();
        assertEquals(serviceCount, results[0].documentLinks.size());
        assertEquals((long) serviceCount, (long) results[0].documentCount);

        this.host.testStart(1);
        this.host.queryServiceUris(options, true, get.clone());
        this.host.testWait();
        assertEquals(serviceCount, results[0].documentLinks.size());
        assertEquals((long) serviceCount, (long) results[0].documentCount);
        this.host.testStart(1);
        this.host.queryServiceUris(options, false, get.clone());
        this.host.testWait();
        assertTrue(results[0].documentLinks.size() >= serviceCount);
        assertEquals((long) results[0].documentLinks.size(), (long) results[0].documentCount);
    }

    @Test
    public void queryServiceUrisWithAuth() throws Throwable {
        setUp(true);
        this.host.setAuthorizationService(new AuthorizationContextService());
        this.host.setAuthorizationEnabled(true);
        this.host.start();

        AuthTestUtils.setSystemAuthorizationContext(this.host);

        // Start Statefull with Non-Persisted service
        this.host.startFactory(new ExampleNonPersistedService());
        this.host.waitForServiceAvailable(ExampleNonPersistedService.FACTORY_LINK);

        TestRequestSender sender = this.host.getTestRequestSender();

        // create user foo@example.com who has access to ExampleServiceState with name="foo"
        TestContext createUserFoo = this.host.testCreate(1);
        String userFoo = "foo@example.com";
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(userFoo)
                .setUserSelfLink(userFoo)
                .setUserPassword("password")
                .setResourceQuery(Query.Builder.create()
                        .addFieldClause(ServiceDocument.FIELD_NAME_KIND, Utils.buildKind(ExampleServiceState.class))
                        .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, "foo")
                        .build())
                .setCompletion(createUserFoo.getCompletion())
                .start();
        createUserFoo.await();

        // create user bar@example.com who has access to ExampleServiceState with name="foo"
        TestContext createUserBar = this.host.testCreate(1);
        String userBar = "bar@example.com";
        AuthorizationSetupHelper.create()
                .setHost(this.host)
                .setUserEmail(userBar)
                .setUserSelfLink(userBar)
                .setUserPassword("password")
                .setResourceQuery(Query.Builder.create()
                        .addFieldClause(ServiceDocument.FIELD_NAME_KIND, Utils.buildKind(ExampleServiceState.class))
                        .addFieldClause(ExampleServiceState.FIELD_NAME_NAME, "bar")
                        .build())
                .setCompletion(createUserBar.getCompletion())
                .start();
        createUserBar.await();

        // create foo & bar documents
        ExampleServiceState exampleFoo = new ExampleServiceState();
        exampleFoo.name = "foo";
        exampleFoo.documentSelfLink = "foo";

        ExampleServiceState exampleBar = new ExampleServiceState();
        exampleBar.name = "bar";
        exampleBar.documentSelfLink = "bar";

        List<Operation> posts = new ArrayList<>();
        posts.add(Operation.createPost(this.host, ExampleNonPersistedService.FACTORY_LINK).setBody(exampleFoo));
        posts.add(Operation.createPost(this.host, ExampleNonPersistedService.FACTORY_LINK).setBody(exampleBar));
        sender.sendAndWait(posts);

        AuthTestUtils.resetAuthorizationContext(this.host);

        // login as foo
        AuthTestUtils.loginAndSetToken(this.host, "foo@example.com", "password");

        Operation factoryGetFoo = Operation.createGet(this.host, ExampleNonPersistedService.FACTORY_LINK);
        ServiceDocumentQueryResult factoryGetResultFoo = sender.sendAndWait(factoryGetFoo, ServiceDocumentQueryResult.class);
        assertEquals(1, factoryGetResultFoo.documentLinks.size());
        assertEquals("/core/nonpersist-examples/foo", factoryGetResultFoo.documentLinks.get(0));


        // login as bar
        AuthTestUtils.loginAndSetToken(this.host, "bar@example.com", "password");
        Operation factoryGetBar = Operation.createGet(this.host, ExampleNonPersistedService.FACTORY_LINK);
        ServiceDocumentQueryResult factoryGetResultBar = sender.sendAndWait(factoryGetBar, ServiceDocumentQueryResult.class);
        assertEquals(1, factoryGetResultBar.documentLinks.size());
        assertEquals("/core/nonpersist-examples/bar", factoryGetResultBar.documentLinks.get(0));
    }

    /**
     * This test verify the custom Ui path resource of service
     **/
    @Test
    public void testServiceCustomUIPath() throws Throwable {
        setUp(false);
        String resourcePath = "customUiPath";
        // Service with custom path
        class CustomUiPathService extends StatelessService {
            public static final String SELF_LINK = "/custom";

            public CustomUiPathService() {
                super();
                toggleOption(ServiceOption.HTML_USER_INTERFACE, true);
            }

            @Override
            public ServiceDocument getDocumentTemplate() {
                ServiceDocument serviceDocument = new ServiceDocument();
                serviceDocument.documentDescription = new ServiceDocumentDescription();
                serviceDocument.documentDescription.userInterfaceResourcePath = resourcePath;
                return serviceDocument;
            }
        }

        // Starting the  CustomUiPathService service
        this.host.startServiceAndWait(new CustomUiPathService(), CustomUiPathService.SELF_LINK, null);

        String htmlPath = "/user-interface/resources/" + resourcePath + "/custom.html";
        // Sending get request for html
        String htmlResponse = this.host.sendWithJavaClient(
                UriUtils.buildUri(this.host, htmlPath),
                Operation.MEDIA_TYPE_TEXT_HTML, null);

        assertEquals("<html>customHtml</html>", htmlResponse);
    }

    @Test
    public void testRootUiService() throws Throwable {
        setUp(false);

        // Stopping the RootNamespaceService
        this.host.waitForResponse(Operation
                .createDelete(UriUtils.buildUri(this.host, UriUtils.URI_PATH_CHAR)));

        class RootUiService extends UiFileContentService {
            public static final String SELF_LINK = UriUtils.URI_PATH_CHAR;
        }

        // Starting the CustomUiService service
        this.host.startServiceAndWait(new RootUiService(), RootUiService.SELF_LINK, null);

        // Loading the default page
        Operation result = this.host.waitForResponse(Operation
                .createGet(UriUtils.buildUri(this.host, RootUiService.SELF_LINK)));
        assertEquals("<html><title>Root</title></html>", result.getBodyRaw());
    }

    @Test
    public void testClientSideRouting() throws Throwable {
        setUp(false);

        class AppUiService extends UiFileContentService {
            public static final String SELF_LINK = "/app";
        }

        // Starting the AppUiService service
        AppUiService s = new AppUiService();
        this.host.startServiceAndWait(s, AppUiService.SELF_LINK, null);

        // Finding the default page file
        Path baseResourcePath = Utils.getServiceUiResourcePath(s);
        Path baseUriPath = Paths.get(AppUiService.SELF_LINK);
        String prefix = baseResourcePath.toString().replace('\\', '/');
        Map<Path, String> pathToURIPath = new HashMap<>();
        this.host.discoverJarResources(baseResourcePath, s, pathToURIPath, baseUriPath, prefix);
        File defaultFile = pathToURIPath.entrySet()
                .stream()
                .filter((entry) -> {
                    return entry.getValue().equals(AppUiService.SELF_LINK +
                            UriUtils.URI_PATH_CHAR + ServiceUriPaths.UI_RESOURCE_DEFAULT_FILE);
                })
                .map(Map.Entry::getKey)
                .findFirst()
                .get()
                .toFile();

        List<String> routes = Arrays.asList("/app/1", "/app/2");

        // Starting all route services
        for (String route : routes) {
            this.host.startServiceAndWait(new FileContentService(defaultFile), route, null);
        }

        // Loading routes
        for (String route : routes) {
            Operation result = this.host.waitForResponse(Operation
                    .createGet(UriUtils.buildUri(this.host, route)));
            assertEquals("<html><title>App</title></html>", result.getBodyRaw());
        }

        // Loading the about page
        Operation about = this.host.waitForResponse(Operation
                .createGet(UriUtils.buildUri(this.host, AppUiService.SELF_LINK + "/about.html")));
        assertEquals("<html><title>About</title></html>", about.getBodyRaw());
    }

    @Test
    public void httpScheme() throws Throwable {
        setUp(true);

        // SSL config for https
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        this.host.setCertificateFileReference(ssc.certificate().toURI());
        this.host.setPrivateKeyFileReference(ssc.privateKey().toURI());

        assertEquals("before starting, scheme is NONE", ServiceHost.HttpScheme.NONE,
                this.host.getCurrentHttpScheme());

        this.host.setPort(0);
        this.host.setSecurePort(0);
        this.host.start();

        ServiceRequestListener httpListener = this.host.getListener();
        ServiceRequestListener httpsListener = this.host.getSecureListener();

        assertTrue("http listener should be on", httpListener.isListening());
        assertTrue("https listener should be on", httpsListener.isListening());
        assertEquals(ServiceHost.HttpScheme.HTTP_AND_HTTPS, this.host.getCurrentHttpScheme());
        assertTrue("public uri scheme should be HTTP",
                this.host.getPublicUri().getScheme().equals("http"));

        httpsListener.stop();
        assertTrue("http listener should be on ", httpListener.isListening());
        assertFalse("https listener should be off", httpsListener.isListening());
        assertEquals(ServiceHost.HttpScheme.HTTP_ONLY, this.host.getCurrentHttpScheme());
        assertTrue("public uri scheme should be HTTP",
                this.host.getPublicUri().getScheme().equals("http"));

        httpListener.stop();
        assertFalse("http listener should be off", httpListener.isListening());
        assertFalse("https listener should be off", httpsListener.isListening());
        assertEquals(ServiceHost.HttpScheme.NONE, this.host.getCurrentHttpScheme());

        // re-start listener even host is stopped, verify getCurrentHttpScheme only
        httpsListener.start(0, ServiceHost.LOOPBACK_ADDRESS);
        assertFalse("http listener should be off", httpListener.isListening());
        assertTrue("https listener should be on", httpsListener.isListening());
        assertEquals(ServiceHost.HttpScheme.HTTPS_ONLY, this.host.getCurrentHttpScheme());
        httpsListener.stop();

        this.host.stop();
        // set HTTP port to disabled, restart host. Verify scheme is HTTPS only. We must
        // set both HTTP and secure port, to null out the listeners from the host instance.
        this.host.setPort(ServiceHost.PORT_VALUE_LISTENER_DISABLED);
        this.host.setSecurePort(0);
        VerificationHost.createAndAttachSSLClient(this.host);
        this.host.start();

        httpListener = this.host.getListener();
        httpsListener = this.host.getSecureListener();

        assertTrue("http listener should be null, default port value set to disabled",
                httpListener == null);
        assertTrue("https listener should be on", httpsListener.isListening());
        assertEquals(ServiceHost.HttpScheme.HTTPS_ONLY, this.host.getCurrentHttpScheme());
        assertTrue("public uri scheme should be HTTPS",
                this.host.getPublicUri().getScheme().equals("https"));
    }

    @Test
    public void create() throws Throwable {
        ServiceHost h = ServiceHost.create("--port=0");
        try {
            h.start();
            h.startDefaultCoreServicesSynchronously();

            // Start the example service factory
            h.startFactory(ExampleService.class, ExampleService::createFactory);

            boolean[] isReady = new boolean[1];
            h.registerForServiceAvailability((o, e) -> {
                isReady[0] = true;
            }, ExampleService.FACTORY_LINK);


            Duration timeout = Duration.of(ServiceHost.ServiceHostState.DEFAULT_MAINTENANCE_INTERVAL_MICROS * 5, ChronoUnit.MICROS);
            TestContext.waitFor(timeout, () -> {
                return isReady[0];
            }, "ExampleService did not start");

            // verify ExampleService exists
            TestRequestSender sender = new TestRequestSender(h);
            Operation get = Operation.createGet(h, ExampleService.FACTORY_LINK);
            sender.sendAndWait(get);
        } finally {
            if (h != null) {
                h.unregisterRuntimeShutdownHook();
                h.stop();
            }
        }
    }

    @Test
    public void findOwnerNode() throws Throwable {
        setUp(false);

        int nodeCount = 3;
        int pathVerificationCount = 3;
        this.host.setUpPeerHosts(nodeCount);
        this.host.joinNodesAndVerifyConvergence(nodeCount, true);
        this.host.setNodeGroupQuorum(nodeCount);

        // each host should say same owner for the path
        for (int i = 0; i < pathVerificationCount; i++) {
            String path = UUID.randomUUID().toString();

            Map<String, String> map = new HashMap<>();
            Set<String> ownerIds = new HashSet<>();
            for (VerificationHost h : this.host.getInProcessHostMap().values()) {
                String ownerId = h.findOwnerNode(null, path).ownerNodeId;
                map.put(h.getId(), ownerId);
                ownerIds.add(ownerId);
            }
            assertThat(ownerIds).as("all peers say same owner for %s. %s", path, map).hasSize(1);
        }

    }

    @Test
    public void restartAndVerifyManagementService() throws Throwable {
        setUp(false);

        // management service should be accessible
        Operation get = Operation.createGet(this.host, ServiceUriPaths.CORE_MANAGEMENT);
        this.host.getTestRequestSender().sendAndWait(get);

        // restart
        this.host.stop();
        this.host.setPort(0);
        this.host.start();

        // verify management service is accessible.
        get = Operation.createGet(this.host, ServiceUriPaths.CORE_MANAGEMENT);
        this.host.getTestRequestSender().sendAndWait(get);
    }

    @Test
    public void findLocalRootNamespaceServiceViaURI() throws Throwable {
        setUp(false);

        // full URI for the localhost (ex: http://127.0.0.1:50000)
        URI rootUri = this.host.getUri();

        // ex: http://127.0.0.1:50000/
        URI rootUriWithPath = UriUtils.buildUri(this.host.getUri(), UriUtils.URI_PATH_CHAR);

        // Accessing localhost with URI will short-circuit the call to direct method invocation. (No netty layer)
        // This should resolve the RootNamespaceService
        this.host.getTestRequestSender().sendAndWait(Operation.createGet(rootUri));

        // same for the URI with path-character
        this.host.getTestRequestSender().sendAndWait(Operation.createGet(rootUriWithPath));
    }

    @Test
    public void cors() throws Throwable {

        // CORS config for http://example.com
        CorsConfig corsConfig = CorsConfigBuilder.forOrigin("http://example.com")
                .allowedRequestMethods(HttpMethod.PUT)
                .allowedRequestHeaders("x-xenon")
                .build();

        this.host = new VerificationHost() {
            @Override
            protected void configureHttpListener(ServiceRequestListener httpListener) {
                // enable CORS
                ((NettyHttpListener) httpListener).setCorsConfig(corsConfig);
            }
        };

        VerificationHost.initialize(this.host, VerificationHost.buildDefaultServiceHostArguments(0));
        this.host.start();

        TestRequestSender sender = this.host.getTestRequestSender();

        Operation get;
        Operation preflight;
        Operation response;

        // Request from http://example.com
        get = Operation.createGet(this.host, "/")
                .addRequestHeader("origin", "http://example.com")
                .forceRemote();

        response = sender.sendAndWait(get);
        assertEquals("http://example.com", response.getResponseHeader("access-control-allow-origin"));

        // Request from http://not-example.com
        get = Operation.createGet(this.host, "/")
                .addRequestHeader("origin", "http://not-example.com")
                .forceRemote();

        response = sender.sendAndWait(get);
        assertNull(response.getResponseHeader("access-control-allow-origin"));


        // Preflight from http://example.com
        preflight = Operation.createOptions(this.host, "/")
                .addRequestHeader("origin", "http://example.com")
                .addRequestHeader("Access-Control-Request-Method", "POST")
                .forceRemote();

        response = sender.sendAndWait(preflight);
        assertEquals("http://example.com", response.getResponseHeader("access-control-allow-origin"));
        assertEquals("PUT", response.getResponseHeader("access-control-allow-methods"));
        assertEquals("x-xenon", response.getResponseHeader("access-control-allow-headers"));

        // Preflight from http://not-example.com
        preflight = Operation.createOptions(this.host, "/")
                .addRequestHeader("origin", "http://not-example.com")
                .addRequestHeader("Access-Control-Request-Method", "POST")
                .addRequestHeader(Operation.CONNECTION_HEADER, "close")
                .setKeepAlive(false)
                .forceRemote();

        response = sender.sendAndWait(preflight);
        assertNull(response.getResponseHeader("access-control-allow-origin"));
        assertNull(response.getResponseHeader("access-control-allow-methods"));
        assertNull(response.getResponseHeader("access-control-allow-headers"));
    }

    @After
    public void tearDown() {
        LuceneDocumentIndexService.setIndexFileCountThresholdForWriterRefresh(
                LuceneDocumentIndexService
                        .DEFAULT_INDEX_FILE_COUNT_THRESHOLD_FOR_WRITER_REFRESH);

        if (this.host == null) {
            return;
        }

        if (!this.host.isStopping()) {
            AuthTestUtils.logout(this.host);
        }
        this.host.tearDown();
        this.host = null;
    }

}
