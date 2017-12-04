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

package com.vmware.xenon.common.test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static javax.xml.bind.DatatypeConverter.printBase64Binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.xml.bind.DatatypeConverter;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import org.apache.lucene.store.LockObtainFailedException;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Claims;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceConfigUpdateRequest;
import com.vmware.xenon.common.ServiceConfiguration;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.common.serialization.KryoSerializers;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ConsistentHashingNodeSelectorService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleServiceHost;
import com.vmware.xenon.services.common.MinimalTestService.MinimalTestServiceErrorResponse;
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupConfig;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeGroupService.UpdateQuorumRequest;
import com.vmware.xenon.services.common.NodeGroupUtils;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.NodeState.NodeOption;
import com.vmware.xenon.services.common.NodeState.NodeStatus;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.QueryValidationTestService.NestedType;
import com.vmware.xenon.services.common.QueryValidationTestService.QueryValidationServiceState;
import com.vmware.xenon.services.common.ServiceHostLogService.LogServiceState;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TaskService;

public class VerificationHost extends ExampleServiceHost {

    public static final int FAST_MAINT_INTERVAL_MILLIS = 100;

    public static final String LOCATION1 = "L1";
    public static final String LOCATION2 = "L2";

    private volatile TestContext context;

    private int timeoutSeconds = 30;

    private long testStartMicros;

    private long testEndMicros;

    private long expectedCompletionCount;

    private Throwable failure;

    private URI referer;

    /**
     * Command line argument. Comma separated list of one or more peer nodes to join through Nodes
     * must be defined in URI form, e.g --peerNodes=http://192.168.1.59:8000,http://192.168.1.82
     */
    public String[] peerNodes;

    /**
     * Command line argument indicating this is a stress test
     */
    public boolean isStressTest;

    /**
     * Command line argument indicating this is a multi-location test
     */
    public boolean isMultiLocationTest;

    /**
     * Command line argument for test duration, set for long running tests
     */
    public long testDurationSeconds;

    /**
     * Command line argument
     */
    public long maintenanceIntervalMillis = FAST_MAINT_INTERVAL_MILLIS;

    /**
     * Command line argument
     */
    public String connectionTag;

    private String lastTestName;

    private TemporaryFolder temporaryFolder;

    private TestRequestSender sender;

    public static AtomicInteger hostNumber = new AtomicInteger();

    public static VerificationHost create() {
        return new VerificationHost();
    }

    public static VerificationHost create(Integer port) throws Exception {
        ServiceHost.Arguments args = buildDefaultServiceHostArguments(port);
        return initialize(new VerificationHost(), args);
    }

    public static ServiceHost.Arguments buildDefaultServiceHostArguments(Integer port) {
        ServiceHost.Arguments args = new ServiceHost.Arguments();
        args.id = "host-" + hostNumber.incrementAndGet();
        args.port = port;
        args.sandbox = null;
        args.bindAddress = ServiceHost.LOOPBACK_ADDRESS;
        return args;
    }

    public static VerificationHost create(ServiceHost.Arguments args)
            throws Exception {
        return initialize(new VerificationHost(), args);
    }

    public static VerificationHost initialize(VerificationHost h, ServiceHost.Arguments args)
            throws Exception {

        if (args.sandbox == null) {
            h.setTemporaryFolder(new TemporaryFolder());
            h.getTemporaryFolder().create();
            args.sandbox = h.getTemporaryFolder().getRoot().toPath();
        }

        try {
            h.initialize(args);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        h.sender = new TestRequestSender(h);
        return h;
    }

    public static void createAndAttachSSLClient(ServiceHost h) throws Throwable {
        // we create a random userAgent string to validate host to host communication when
        // the client appears to be from an external, non-Xenon source.
        ServiceClient client = NettyHttpServiceClient.create(UUID.randomUUID().toString(),
                null,
                h.getScheduledExecutor(), h);

        SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
        clientContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
        client.setSSLContext(clientContext);
        h.setClient(client);

        SelfSignedCertificate ssc = new SelfSignedCertificate();
        h.setCertificateFileReference(ssc.certificate().toURI());
        h.setPrivateKeyFileReference(ssc.privateKey().toURI());
    }

    @Override
    protected void configureLoggerFormatter(Logger logger) {
        super.configureLoggerFormatter(logger);

        // override with formatters for VerificationHost
        // if custom formatter has already set, do NOT replace it.
        for (Handler h : logger.getParent().getHandlers()) {
            if (Objects.equals(h.getFormatter(), LOG_FORMATTER)) {
                h.setFormatter(VerificationHostLogFormatter.NORMAL_FORMAT);
            } else if (Objects.equals(h.getFormatter(), COLOR_LOG_FORMATTER)) {
                h.setFormatter(VerificationHostLogFormatter.COLORED_FORMAT);
            }
        }
    }

    public void tearDown() {
        stop();
        TemporaryFolder tempFolder = this.getTemporaryFolder();
        if (tempFolder != null) {
            tempFolder.delete();
        }
    }

    public Operation createServiceStartPost(TestContext ctx) {
        Operation post = Operation.createPost(null);
        post.setUri(UriUtils.buildUri(this, "service/" + post.getId()));
        return post.setCompletion(ctx.getCompletion());
    }

    public CompletionHandler getCompletion() {
        return (o, e) -> {
            if (e != null) {
                failIteration(e);
                return;
            }
            completeIteration();
        };
    }

    public <T> BiConsumer<T, ? super Throwable> getCompletionDeferred() {
        return (ignore, e) -> {
            if (e != null) {
                if (e instanceof CompletionException) {
                    e = e.getCause();
                }
                failIteration(e);
                return;
            }
            completeIteration();
        };
    }

    public CompletionHandler getExpectedFailureCompletion() {
        return getExpectedFailureCompletion(null);
    }

    public CompletionHandler getExpectedFailureCompletion(Integer statusCode) {
        return (o, e) -> {
            if (e == null) {
                failIteration(new IllegalStateException("Failure expected"));
                return;
            }

            if (statusCode != null) {
                if (!statusCode.equals(o.getStatusCode())) {
                    failIteration(new IllegalStateException(
                            "Expected different status code "
                                    + statusCode + " got " + o.getStatusCode()));
                    return;
                }
            }

            if (e instanceof TimeoutException) {
                if (o.getStatusCode() != Operation.STATUS_CODE_TIMEOUT) {
                    failIteration(new IllegalArgumentException(
                            "TImeout exception did not have timeout status code"));
                    return;
                }
            }

            if (o.hasBody()) {
                ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                if (rsp.message != null && rsp.message.toLowerCase().contains("timeout")
                        && rsp.statusCode != Operation.STATUS_CODE_TIMEOUT) {
                    failIteration(new IllegalArgumentException(
                            "Service error response did not have timeout status code:"
                                    + Utils.toJsonHtml(rsp)));
                    return;
                }

            }

            completeIteration();
        };
    }

    public VerificationHost setTimeoutSeconds(int seconds) {
        this.timeoutSeconds = seconds;
        if (this.sender != null) {
            this.sender.setTimeout(Duration.ofSeconds(seconds));
        }
        return this;
    }

    public int getTimeoutSeconds() {
        return this.timeoutSeconds;
    }

    public void send(Operation op) {
        op.setReferer(getReferer());
        super.sendRequest(op);
    }

    @Override
    public DeferredResult<Operation> sendWithDeferredResult(Operation operation) {
        operation.setReferer(getReferer());
        return super.sendWithDeferredResult(operation);
    }

    @Override
    public <T> DeferredResult<T> sendWithDeferredResult(Operation operation, Class<T> resultType) {
        operation.setReferer(getReferer());
        return super.sendWithDeferredResult(operation, resultType);
    }

    /**
     * Creates a test wait context that can be nested and isolated from other wait contexts
     */
    public TestContext testCreate(int c) {
        return TestContext.create(c, TimeUnit.SECONDS.toMicros(this.timeoutSeconds));
    }

    /**
     * Creates a test wait context that can be nested and isolated from other wait contexts
     */
    public TestContext testCreate(long c) {
        return testCreate((int) c);
    }

    /**
     * Starts a test context used for a single synchronous test execution for the entire host
     * @param c Expected completions
     */
    public void testStart(long c) {
        if (this.isSingleton) {
            throw new IllegalStateException("Use testCreate on singleton, shared host instances");
        }
        String testName = buildTestNameFromStack();
        testStart(
                testName,
                EnumSet.noneOf(TestProperty.class), c);
    }

    public String buildTestNameFromStack() {
        StackTraceElement[] stack = new Exception().getStackTrace();
        String rootTestMethod = "";
        for (StackTraceElement s : stack) {
            if (s.getClassName().contains("vmware")) {
                rootTestMethod = s.getMethodName();
            }
        }
        String testName = rootTestMethod + ":" + stack[2].getMethodName();
        return testName;
    }

    public void testStart(String testName, EnumSet<TestProperty> properties, long c) {
        if (this.isSingleton) {
            throw new IllegalStateException("Use startTest on singleton, shared host instances");
        }
        if (this.context != null) {
            throw new IllegalStateException("A test is already started");
        }

        String negative = properties != null && properties.contains(TestProperty.FORCE_FAILURE)
                ? "(NEGATIVE)"
                : "";
        if (c > 1) {
            log("%sTest %s, iterations %d, started", negative, testName, c);
        }
        this.failure = null;
        this.expectedCompletionCount = c;
        this.testStartMicros = Utils.getNowMicrosUtc();
        this.context = TestContext.create((int) c, TimeUnit.SECONDS.toMicros(this.timeoutSeconds));
    }

    public void completeIteration() {
        if (this.isSingleton) {
            throw new IllegalStateException("Use startTest on singleton, shared host instances");
        }
        TestContext ctx = this.context;

        if (ctx == null) {
            String error = "testStart() and testWait() not paired properly" +
                    " or testStart(N) was called with N being less than actual completions";
            log(error);
            return;
        }
        ctx.completeIteration();
    }

    public void failIteration(Throwable e) {
        if (this.isSingleton) {
            throw new IllegalStateException("Use startTest on singleton, shared host instances");
        }
        if (isStopping()) {
            log("Received completion after stop");
            return;
        }

        TestContext ctx = this.context;

        if (ctx == null) {
            log("Test finished, ignoring completion. This might indicate wrong count was used in testStart(count)");
            return;
        }

        log("test failed: %s", e.toString());
        ctx.failIteration(e);
    }

    public void testWait(TestContext ctx) {
        ctx.await();
    }

    public void testWait() {
        testWait(new Exception().getStackTrace()[1].getMethodName(),
                this.timeoutSeconds);
    }

    public void testWait(int timeoutSeconds) {
        testWait(new Exception().getStackTrace()[1].getMethodName(), timeoutSeconds);
    }

    public void testWait(String testName, int timeoutSeconds) {
        if (this.isSingleton) {
            throw new IllegalStateException("Use startTest on singleton, shared host instances");
        }

        TestContext ctx = this.context;
        if (ctx == null) {
            throw new IllegalStateException("testStart() was not called before testWait()");
        }

        if (this.expectedCompletionCount > 1) {
            log("Test %s, iterations %d, waiting ...", testName,
                    this.expectedCompletionCount);
        }

        try {
            ctx.await();
            this.testEndMicros = Utils.getNowMicrosUtc();
            if (this.expectedCompletionCount > 1) {
                log("Test %s, iterations %d, complete!", testName,
                        this.expectedCompletionCount);
            }
        } finally {
            this.context = null;
            this.lastTestName = testName;
        }
    }

    public double calculateThroughput() {
        double t = this.testEndMicros - this.testStartMicros;
        t /= 1000000.0;
        t = this.expectedCompletionCount / t;
        return t;
    }

    public long computeIterationsFromMemory(int serviceCount) {
        return computeIterationsFromMemory(EnumSet.noneOf(TestProperty.class), serviceCount);
    }

    public long computeIterationsFromMemory(EnumSet<TestProperty> props, int serviceCount) {
        long total = Runtime.getRuntime().totalMemory();

        total /= 512;
        total /= serviceCount;
        if (props == null) {
            props = EnumSet.noneOf(TestProperty.class);
        }

        if (props.contains(TestProperty.FORCE_REMOTE)) {
            total /= 5;
        }

        if (props.contains(TestProperty.PERSISTED)) {
            total /= 5;
        }

        if (props.contains(TestProperty.FORCE_FAILURE)
                || props.contains(TestProperty.EXPECT_FAILURE)) {
            total = 10;
        }

        if (!this.isStressTest) {
            total /= 100;
            total = Math.max(Runtime.getRuntime().availableProcessors() * 16, total);
        }
        total = Math.max(1, total);

        if (props.contains(TestProperty.SINGLE_ITERATION)) {
            total = 1;
        }

        return total;
    }

    public void logThroughput() {
        log("Test %s iterations per second: %f", this.lastTestName, calculateThroughput());
        logMemoryInfo();
    }

    public void log(String fmt, Object... args) {
        super.log(Level.INFO, 3, fmt, args);
    }

    public ServiceDocument buildMinimalTestState() {
        return buildMinimalTestState(20);
    }

    public MinimalTestServiceState buildMinimalTestState(int bytes) {
        MinimalTestServiceState minState = new MinimalTestServiceState();
        minState.id = Utils.getNowMicrosUtc() + "";
        byte[] body = new byte[bytes];
        new Random().nextBytes(body);
        minState.stringValue = DatatypeConverter.printBase64Binary(body);
        return minState;
    }

    public CompletableFuture<Operation> sendWithFuture(Operation op) {
        if (op.getCompletion() != null) {
            throw new IllegalStateException("completion handler must not be set");
        }

        CompletableFuture<Operation> res = new CompletableFuture<>();
        op.setCompletion((o, e) -> {
            if (e != null) {
                res.completeExceptionally(e);
            } else {
                res.complete(o);
            }
        });

        this.send(op);

        return res;
    }

    /**
     * Use built in Java synchronous HTTP client to verify DCP HttpListener is compliant
     */
    public String sendWithJavaClient(URI serviceUri, String contentType, String body)
            throws IOException {
        URL url = serviceUri.toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoInput(true);

        connection.addRequestProperty(Operation.CONTENT_TYPE_HEADER, contentType);
        if (body != null) {
            connection.setDoOutput(true);
            connection.getOutputStream().write(body.getBytes(Utils.CHARSET));
        }

        BufferedReader in = null;
        try {
            try {
                in = new BufferedReader(
                        new InputStreamReader(
                                connection.getInputStream(), Utils.CHARSET));

            } catch (Throwable e) {
                InputStream errorStream = connection.getErrorStream();
                if (errorStream != null) {
                    in = new BufferedReader(
                            new InputStreamReader(errorStream, Utils.CHARSET));
                }
            }
            StringBuilder stringResponseBuilder = new StringBuilder();

            if (in == null) {
                return "";
            }
            do {
                String line = in.readLine();
                if (line == null) {
                    break;
                }
                stringResponseBuilder.append(line);
            } while (true);

            return stringResponseBuilder.toString();
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    public URI createQueryTaskService(QueryTask create) {
        return createQueryTaskService(create, false);
    }

    public URI createQueryTaskService(QueryTask create, boolean forceRemote) {
        return createQueryTaskService(create, forceRemote, false, null, null);
    }

    public URI createQueryTaskService(QueryTask create, boolean forceRemote, String sourceLink) {
        return createQueryTaskService(create, forceRemote, false, null, sourceLink);
    }

    public URI createQueryTaskService(QueryTask create, boolean forceRemote, boolean isDirect,
            QueryTask taskResult,
            String sourceLink) {
        return createQueryTaskService(null, create, forceRemote, isDirect, taskResult, sourceLink);
    }

    public URI createQueryTaskService(URI factoryUri, QueryTask create, boolean forceRemote,
            boolean isDirect,
            QueryTask taskResult,
            String sourceLink) {

        if (create.documentExpirationTimeMicros == 0) {
            create.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + this.getOperationTimeoutMicros();
        }

        if (factoryUri == null) {
            VerificationHost h = this;
            if (!getInProcessHostMap().isEmpty()) {
                // pick one host to create the query task
                h = getInProcessHostMap().values().iterator().next();
            }
            factoryUri = UriUtils.buildUri(h, ServiceUriPaths.CORE_QUERY_TASKS);
        }
        create.documentSelfLink = UUID.randomUUID().toString();
        create.documentSourceLink = sourceLink;
        create.taskInfo.isDirect = isDirect;
        Operation startPost = Operation.createPost(factoryUri).setBody(create);

        if (forceRemote) {
            startPost.forceRemote();
        }

        log("Starting query with options:%s, resultLimit: %d",
                create.querySpec.options,
                create.querySpec.resultLimit);

        QueryTask result;
        try {
            result = this.sender.sendAndWait(startPost, QueryTask.class);
        } catch (RuntimeException e) {
            // throw original exception
            throw ExceptionTestUtils.throwAsUnchecked(e.getSuppressed()[0]);
        }

        if (isDirect) {
            taskResult.results = result.results;
            taskResult.taskInfo.durationMicros = result.results.queryTimeMicros;
        }

        return UriUtils.extendUri(factoryUri, create.documentSelfLink);
    }

    public QueryTask waitForQueryTaskCompletion(QuerySpecification q, int totalDocuments,
            int versionCount, URI u, boolean forceRemote, boolean deleteOnFinish) {
        return waitForQueryTaskCompletion(q, totalDocuments, versionCount, u, forceRemote,
                deleteOnFinish, true);
    }

    public boolean isOwner(String documentSelfLink, String nodeSelector) {
        final boolean[] isOwner = new boolean[1];
        TestContext ctx = this.testCreate(1);
        Operation op = Operation
                .createPost(null)
                .setExpiration(Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(10))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }

                    NodeSelectorService.SelectOwnerResponse rsp =
                            o.getBody(NodeSelectorService.SelectOwnerResponse.class);
                    isOwner[0] = rsp.isLocalHostOwner;
                    ctx.completeIteration();
                });
        this.selectOwner(nodeSelector, documentSelfLink, op);
        ctx.await();

        return isOwner[0];
    }

    public QueryTask waitForQueryTaskCompletion(QuerySpecification q, int totalDocuments,
            int versionCount, URI u, boolean forceRemote, boolean deleteOnFinish,
            boolean throwOnFailure) {

        long startNanos = System.nanoTime();
        if (q.options == null) {
            q.options = EnumSet.noneOf(QueryOption.class);
        }

        EnumSet<TestProperty> props = EnumSet.noneOf(TestProperty.class);
        if (forceRemote) {
            props.add(TestProperty.FORCE_REMOTE);
        }
        waitFor("Query did not complete in time", () -> {
            QueryTask taskState = getServiceState(props, QueryTask.class, u);
            return taskState.taskInfo.stage == TaskState.TaskStage.FINISHED
                    || taskState.taskInfo.stage == TaskState.TaskStage.FAILED
                    || taskState.taskInfo.stage == TaskState.TaskStage.CANCELLED;
        });

        QueryTask latestTaskState = getServiceState(props, QueryTask.class, u);

        // Throw if task was expected to be successful
        if (throwOnFailure && (latestTaskState.taskInfo.stage == TaskState.TaskStage.FAILED)) {
            throw new IllegalStateException(Utils.toJsonHtml(latestTaskState.taskInfo.failure));
        }

        if (totalDocuments * versionCount > 1) {
            long endNanos = System.nanoTime();
            double deltaSeconds = endNanos - startNanos;
            deltaSeconds /= TimeUnit.SECONDS.toNanos(1);
            double thpt = totalDocuments / deltaSeconds;
            log("Options: %s.  Throughput (documents / sec): %f", q.options.toString(), thpt);
        }

        // Delete task, if not direct
        if (latestTaskState.taskInfo.isDirect) {
            return latestTaskState;
        }

        if (deleteOnFinish) {
            send(Operation.createDelete(u).setBody(new ServiceDocument()));
        }

        return latestTaskState;
    }

    public ServiceDocumentQueryResult createAndWaitSimpleDirectQuery(
            String fieldName, String fieldValue, long documentCount, long expectedResultCount) {
        return createAndWaitSimpleDirectQuery(this.getUri(), fieldName, fieldValue, documentCount,
                expectedResultCount);
    }

    public ServiceDocumentQueryResult createAndWaitSimpleDirectQuery(URI hostUri,
            String fieldName, String fieldValue, long documentCount, long expectedResultCount) {
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(fieldName).setTermMatchValue(fieldValue);
        return createAndWaitSimpleDirectQuery(hostUri, q,
                documentCount, expectedResultCount);
    }

    public ServiceDocumentQueryResult createAndWaitSimpleDirectQuery(
            QueryTask.QuerySpecification spec,
            long documentCount, long expectedResultCount) {
        return createAndWaitSimpleDirectQuery(this.getUri(), spec,
                documentCount, expectedResultCount);
    }

    public ServiceDocumentQueryResult createAndWaitSimpleDirectQuery(URI hostUri,
            QueryTask.QuerySpecification spec, long documentCount, long expectedResultCount) {
        long start = Utils.getNowMicrosUtc();

        QueryTask[] tasks = new QueryTask[1];
        waitFor("", () -> {
            QueryTask task = QueryTask.create(spec).setDirect(true);
            createQueryTaskService(UriUtils.buildUri(hostUri, ServiceUriPaths.CORE_QUERY_TASKS),
                    task, false, true, task, null);
            if (task.results.documentLinks.size() == expectedResultCount) {
                tasks[0] = task;
                return true;
            }
            log("Expected %d, got %d, Query task: %s", expectedResultCount,
                    task.results.documentLinks.size(), task);
            return false;
        });

        QueryTask resultTask = tasks[0];

        assertTrue(
                String.format("Got %d links, expected %d", resultTask.results.documentLinks.size(),
                        expectedResultCount),
                resultTask.results.documentLinks.size() == expectedResultCount);
        long end = Utils.getNowMicrosUtc();
        double delta = (end - start) / 1000000.0;
        double thpt = documentCount / delta;
        log("Document count: %d, Expected match count: %d, Documents / sec: %f",
                documentCount, expectedResultCount, thpt);
        return resultTask.results;
    }

    public void validatePermanentServiceDocumentDeletion(String linkPrefix, long count,
            boolean failOnMismatch)
            throws Throwable {
        long start = Utils.getNowMicrosUtc();

        while (Utils.getNowMicrosUtc() - start < this.getOperationTimeoutMicros()) {
            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            q.query = new QueryTask.Query()
                    .setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                    .setTermMatchType(MatchType.WILDCARD)
                    .setTermMatchValue(linkPrefix + UriUtils.URI_WILDCARD_CHAR);

            URI u = createQueryTaskService(QueryTask.create(q), false);
            QueryTask finishedTaskState = waitForQueryTaskCompletion(q,
                    (int) count, (int) count, u, false, true);
            if (finishedTaskState.results.documentLinks.size() == count) {
                return;
            }
            log("got %d links back, expected %d: %s",
                    finishedTaskState.results.documentLinks.size(), count,
                    Utils.toJsonHtml(finishedTaskState));

            if (!failOnMismatch) {
                return;
            }
            Thread.sleep(100);
        }
        if (failOnMismatch) {
            throw new TimeoutException();
        }
    }

    public String sendHttpRequest(ServiceClient client, String uri, String requestBody, int count) {

        Object[] rspBody = new Object[1];
        TestContext ctx = testCreate(count);
        Operation op = Operation.createGet(URI.create(uri)).setCompletion(
                (o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    rspBody[0] = o.getBodyRaw();
                    ctx.completeIteration();
                });

        if (requestBody != null) {
            op.setAction(Action.POST).setBody(requestBody);
        }

        op.setExpiration(Utils.getNowMicrosUtc() + getOperationTimeoutMicros());
        op.setReferer(getReferer());
        ServiceClient c = client != null ? client : getClient();
        for (int i = 0; i < count; i++) {
            c.send(op);
        }
        ctx.await();

        String htmlResponse = (String) rspBody[0];
        return htmlResponse;
    }

    public Operation sendUIHttpRequest(String uri, String requestBody, int count) {
        Operation op = Operation.createGet(URI.create(uri));
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ops.add(op);
        }
        List<Operation> responses = this.sender.sendAndWait(ops);
        return responses.get(0);
    }

    public <T extends ServiceDocument> T getServiceState(EnumSet<TestProperty> props, Class<T> type,
            URI uri) {
        Map<URI, T> r = getServiceState(props, type, new URI[] { uri });
        return r.values().iterator().next();
    }

    public <T extends ServiceDocument> Map<URI, T> getServiceState(EnumSet<TestProperty> props,
            Class<T> type,
            Collection<URI> uris) {
        URI[] array = new URI[uris.size()];
        int i = 0;
        for (URI u : uris) {
            array[i++] = u;
        }
        return getServiceState(props, type, array);
    }

    public <T extends TaskService.TaskServiceState> T getServiceStateUsingQueryTask(
            Class<T> type, String uri) {
        QueryTask.Query q = QueryTask.Query.Builder.create()
                .setTerm(ServiceDocument.FIELD_NAME_SELF_LINK, uri)
                .build();

        QueryTask queryTask = new QueryTask();
        queryTask.querySpec = new QueryTask.QuerySpecification();
        queryTask.querySpec.query = q;
        queryTask.querySpec.options.add(QueryOption.EXPAND_CONTENT);

        this.createQueryTaskService(null, queryTask, false, true, queryTask, null);
        return Utils.fromJson(queryTask.results.documents.get(uri), type);
    }

    /**
     * Retrieve in parallel, state from N services. This method will block execution until responses
     * are received or a failure occurs. It is not optimized for throughput measurements
     *
     * @param type
     * @param uris
     */
    public <T extends ServiceDocument> Map<URI, T> getServiceState(EnumSet<TestProperty> props,
            Class<T> type, URI... uris) {

        if (type == null) {
            throw new IllegalArgumentException("type is required");
        }

        if (uris == null || uris.length == 0) {
            throw new IllegalArgumentException("uris are required");
        }

        List<Operation> ops = new ArrayList<>();
        for (URI u : uris) {
            Operation get = Operation.createGet(u).setReferer(getReferer());
            if (props != null && props.contains(TestProperty.FORCE_REMOTE)) {
                get.forceRemote();
            }
            if (props != null && props.contains(TestProperty.HTTP2)) {
                get.setConnectionSharing(true);
            }

            if (props != null && props.contains(TestProperty.DISABLE_CONTEXT_ID_VALIDATION)) {
                get.setContextId(TestProperty.DISABLE_CONTEXT_ID_VALIDATION.toString());
            }

            ops.add(get);
        }

        Map<URI, T> results = new HashMap<>();

        List<Operation> responses = this.sender.sendAndWait(ops);
        for (Operation response : responses) {
            T doc = response.getBody(type);
            results.put(UriUtils.buildUri(response.getUri(), doc.documentSelfLink), doc);
        }

        return results;
    }

    /**
     * Retrieve in parallel, state from N services. This method will block execution until responses
     * are received or a failure occurs. It is not optimized for throughput measurements
     */
    public <T extends ServiceDocument> Map<URI, T> getServiceState(EnumSet<TestProperty> props,
            Class<T> type,
            List<Service> services) {
        URI[] uris = new URI[services.size()];
        int i = 0;
        for (Service s : services) {
            uris[i++] = s.getUri();
        }
        return this.getServiceState(props, type, uris);
    }

    public ServiceDocumentQueryResult getFactoryState(URI factoryUri) {
        return this.getServiceState(null, ServiceDocumentQueryResult.class, factoryUri);
    }

    public ServiceDocumentQueryResult getExpandedFactoryState(URI factoryUri) {
        factoryUri = UriUtils.buildExpandLinksQueryUri(factoryUri);
        return this.getServiceState(null, ServiceDocumentQueryResult.class, factoryUri);
    }

    public Map<String, ServiceStat> getServiceStats(URI serviceUri) {
        AuthorizationContext ctx = null;
        if (this.isAuthorizationEnabled()) {
            ctx = OperationContext.getAuthorizationContext();
            this.setSystemAuthorizationContext();
        }
        ServiceStats stats = this.getServiceState(
                null, ServiceStats.class, UriUtils.buildStatsUri(serviceUri));
        if (this.isAuthorizationEnabled()) {
            this.setAuthorizationContext(ctx);
        }
        return stats.entries;
    }

    public void doExampleServiceUpdateAndQueryByVersion(URI hostUri, int serviceCount) {
        Consumer<Operation> bodySetter = (o) -> {
            ExampleServiceState s = new ExampleServiceState();
            s.name = UUID.randomUUID().toString();
            o.setBody(s);
        };
        Map<URI, ExampleServiceState> services = doFactoryChildServiceStart(null,
                serviceCount,
                ExampleServiceState.class, bodySetter,
                UriUtils.buildUri(hostUri, ExampleService.FACTORY_LINK));

        Map<URI, ExampleServiceState> statesBeforeUpdate = getServiceState(null,
                ExampleServiceState.class, services.keySet());

        for (ExampleServiceState state : statesBeforeUpdate.values()) {
            assertEquals(state.documentVersion, 0);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.POST, 0L,
                    0L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.POST, null,
                    0L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.POST, 1L,
                    null);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.POST, 10L,
                    null);
        }

        ExampleServiceState body = new ExampleServiceState();
        body.name = UUID.randomUUID().toString();
        doServiceUpdates(services.keySet(), Action.PUT, body);
        Map<URI, ExampleServiceState> statesAfterPut = getServiceState(null,
                ExampleServiceState.class, services.keySet());

        for (ExampleServiceState state : statesAfterPut.values()) {
            assertEquals(state.documentVersion, 1);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.POST, 0L,
                    0L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.PUT, 1L,
                    1L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.PUT, null,
                    1L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.PUT, 10L,
                    null);
        }

        doServiceUpdates(services.keySet(), Action.DELETE, body);

        for (ExampleServiceState state : statesAfterPut.values()) {
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.POST, 0L,
                    0L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.PUT, 1L,
                    1L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.DELETE, 2L,
                    2L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.DELETE,
                    null, 2L);
            queryDocumentIndexByVersionAndVerify(hostUri, state.documentSelfLink, Action.DELETE,
                    10L, null);
        }
    }

    private void doServiceUpdates(Collection<URI> serviceUris, Action action,
            ServiceDocument body) {
        List<Operation> ops = new ArrayList<>();
        for (URI u : serviceUris) {
            Operation update = Operation.createPost(u)
                    .setAction(action)
                    .setBody(body);
            ops.add(update);
        }
        this.sender.sendAndWait(ops);
    }

    private void queryDocumentIndexByVersionAndVerify(URI hostUri, String selfLink,
            Action expectedAction,
            Long version,
            Long latestVersion) {

        URI localQueryUri = UriUtils.buildDefaultDocumentQueryUri(
                hostUri,
                selfLink,
                false,
                true,
                ServiceOption.PERSISTENCE);

        if (version != null) {
            localQueryUri = UriUtils.appendQueryParam(localQueryUri,
                    ServiceDocument.FIELD_NAME_VERSION,
                    Long.toString(version));
        }

        Operation remoteGet = Operation.createGet(localQueryUri);
        Operation result = this.sender.sendAndWait(remoteGet);
        if (latestVersion == null) {
            assertFalse("Document not expected", result.hasBody());
            return;
        }

        ServiceDocument doc = result.getBody(ServiceDocument.class);
        int expectedVersion = version == null ? latestVersion.intValue() : version.intValue();
        assertEquals("Invalid document version returned", doc.documentVersion, expectedVersion);

        String action = doc.documentUpdateAction;
        assertEquals("Invalid document update action returned:" + action, expectedAction.name(),
                action);

    }

    public <T> void doPutPerService(List<Service> services)
            throws Throwable {
        doPutPerService(EnumSet.noneOf(TestProperty.class), services);
    }

    public <T> void doPutPerService(EnumSet<TestProperty> properties,
            List<Service> services) throws Throwable {
        doPutPerService(computeIterationsFromMemory(properties, services.size()),
                properties,
                services);
    }

    public <T> void doPatchPerService(long count,
            EnumSet<TestProperty> properties,
            List<Service> services) throws Throwable {
        doServiceUpdates(Action.PATCH, count, properties, services);
    }

    public <T> void doPutPerService(long count, EnumSet<TestProperty> properties,
            List<Service> services) throws Throwable {
        doServiceUpdates(Action.PUT, count, properties, services);
    }

    public void doServiceUpdates(Action action, long count,
            EnumSet<TestProperty> properties,
            List<Service> services) throws Throwable {

        if (properties == null) {
            properties = EnumSet.noneOf(TestProperty.class);
        }

        logMemoryInfo();
        StackTraceElement[] e = new Exception().getStackTrace();
        String testName = String.format(
                "Parent: %s, %s test with properties %s, service caps: %s",
                e[1].getMethodName(),
                action, properties.toString(), services.get(0).getOptions());

        Map<URI, MinimalTestServiceState> statesBeforeUpdate = getServiceState(properties,
                MinimalTestServiceState.class, services);


        long startTimeMicros = System.nanoTime() / 1000;
        TestContext ctx = testCreate(count * services.size());
        ctx.setTestName(testName);
        ctx.logBefore();

        // create a template PUT. Each operation instance is cloned on send, so
        // we can re-use across services
        Operation updateOp = Operation.createPut(null).setCompletion(ctx.getCompletion());

        updateOp.setAction(action);

        if (properties.contains(TestProperty.FORCE_REMOTE)) {
            updateOp.forceRemote();
        }
        MinimalTestServiceState body = (MinimalTestServiceState) buildMinimalTestState();
        byte[] binaryBody = null;

        // put random values in core document properties to verify they are
        // ignored
        if (!this.isStressTest()) {
            body.documentSelfLink = UUID.randomUUID().toString();
            body.documentKind = UUID.randomUUID().toString();
        } else {
            body.stringValue = UUID.randomUUID().toString();
            body.id = UUID.randomUUID().toString();
            body.responseDelay = 10;
            body.documentVersion = 10;
            body.documentEpoch = 10L;
            body.documentOwner = UUID.randomUUID().toString();
        }

        if (properties.contains(TestProperty.SET_EXPIRATION)) {
            // set expiration to the maintenance interval, which should already be very small
            // when the caller sets this test property
            body.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                    + this.getMaintenanceIntervalMicros();
        }

        final int maxByteCount = 256 * 1024;
        if (properties.contains(TestProperty.LARGE_PAYLOAD)) {
            Random r = new Random();
            int byteCount = getClient().getRequestPayloadSizeLimit() / 4;
            if (properties.contains(TestProperty.BINARY_PAYLOAD)) {
                if (properties.contains(TestProperty.FORCE_FAILURE)) {
                    byteCount = getClient().getRequestPayloadSizeLimit() * 2;
                } else {
                    // make sure we do not blow memory if max request size is high
                    byteCount = Math.min(maxByteCount, byteCount);
                }
            } else {
                byteCount = maxByteCount;
            }
            byte[] data = new byte[byteCount];
            r.nextBytes(data);
            if (properties.contains(TestProperty.BINARY_PAYLOAD)) {
                binaryBody = data;
            } else {
                body.stringValue = printBase64Binary(data);
            }
        }

        if (properties.contains(TestProperty.HTTP2)) {
            updateOp.setConnectionSharing(true);
        }

        if (properties.contains(TestProperty.BINARY_PAYLOAD)) {
            updateOp.setContentType(Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM);
            updateOp.setCompletion((o, eb) -> {
                if (eb != null) {
                    ctx.fail(eb);
                    return;
                }

                if (!Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM.equals(o.getContentType())) {
                    ctx.fail(new IllegalArgumentException("unexpected content type: "
                            + o.getContentType()));
                    return;
                }
                ctx.complete();
            });

        }

        boolean isFailureExpected = false;
        if (properties.contains(TestProperty.FORCE_FAILURE)
                || properties.contains(TestProperty.EXPECT_FAILURE)) {
            toggleNegativeTestMode(true);
            isFailureExpected = true;

            if (properties.contains(TestProperty.LARGE_PAYLOAD)) {
                updateOp.setCompletion((o, ex) -> {
                    if (ex == null) {
                        ctx.fail(new IllegalStateException("expected failure"));
                    } else {
                        ctx.complete();
                    }
                });
            } else {
                updateOp.setCompletion((o, ex) -> {
                    if (ex == null) {
                        ctx.fail(new IllegalStateException("failure expected"));
                        return;
                    }

                    MinimalTestServiceErrorResponse rsp = o
                            .getBody(MinimalTestServiceErrorResponse.class);
                    if (!MinimalTestServiceErrorResponse.KIND.equals(rsp.documentKind)) {
                        ctx.fail(new IllegalStateException("Response not expected:"
                                + Utils.toJson(rsp)));
                        return;
                    }
                    ctx.complete();
                });
            }
        }

        int byteCount = Utils.toJson(body).getBytes(Utils.CHARSET).length;
        if (properties.contains(TestProperty.BINARY_SERIALIZATION)) {
            long c = KryoSerializers.serializeDocument(body, 4096).position();
            byteCount = (int) c;
        }
        log("Bytes per payload %s", byteCount);

        boolean isConcurrentSend = properties.contains(TestProperty.CONCURRENT_SEND);
        final boolean isFailureExpectedFinal = isFailureExpected;

        for (Service s : services) {
            if (properties.contains(TestProperty.FORCE_REMOTE)) {
                updateOp.setConnectionTag(this.connectionTag);
            }

            long[] expectedVersion = new long[1];
            if (s.hasOption(ServiceOption.STRICT_UPDATE_CHECKING)) {
                // we have to serialize requests and properly set version to match expected current
                // version
                MinimalTestServiceState initialState = statesBeforeUpdate.get(s.getUri());
                expectedVersion[0] = isFailureExpected ? Integer.MAX_VALUE
                        : initialState.documentVersion;
            }

            URI sUri = s.getUri();
            updateOp.setUri(sUri).setReferer(getReferer());

            for (int i = 0; i < count; i++) {
                if (!isFailureExpected) {
                    body.id = "" + i;
                } else if (!properties.contains(TestProperty.LARGE_PAYLOAD)) {
                    body.id = null;
                }

                CountDownLatch[] l = new CountDownLatch[1];
                if (s.hasOption(ServiceOption.STRICT_UPDATE_CHECKING)) {
                    // only used for strict update checking, serialized requests
                    l[0] = new CountDownLatch(1);
                    // we have to serialize requests and properly set version
                    body.documentVersion = expectedVersion[0];
                    updateOp.setCompletion((o, ex) -> {
                        if (ex == null || isFailureExpectedFinal) {
                            MinimalTestServiceState rsp = o.getBody(MinimalTestServiceState.class);
                            expectedVersion[0] = rsp.documentVersion;
                            ctx.complete();
                            l[0].countDown();
                            return;
                        }
                        ctx.fail(ex);
                        l[0].countDown();
                    });
                }

                Object b = binaryBody != null ? binaryBody : body;
                if (properties.contains(TestProperty.BINARY_SERIALIZATION)) {
                    // provide hints to runtime on how to serialize the body,
                    // using binary serialization and a buffer size equal to content length
                    updateOp.setContentLength(byteCount);
                    updateOp.setContentType(Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM);
                }

                if (isConcurrentSend) {
                    Operation putClone = updateOp.clone();
                    putClone.setBody(b).setUri(sUri);
                    run(() -> {
                        send(putClone);
                    });
                } else if (properties.contains(TestProperty.CALLBACK_SEND)) {
                    updateOp.toggleOption(OperationOption.SEND_WITH_CALLBACK, true);
                    send(updateOp.setBody(b));
                } else {
                    send(updateOp.setBody(b));
                }
                if (s.hasOption(ServiceOption.STRICT_UPDATE_CHECKING)) {
                    // we have to serialize requests and properly set version
                    if (!isFailureExpected) {
                        l[0].await();
                    }
                    if (this.failure != null) {
                        throw this.failure;
                    }

                }
            }
        }

        testWait(ctx);
        ctx.logAfter();

        if (isFailureExpected) {
            this.toggleNegativeTestMode(false);
            return;
        }

        if (properties.contains(TestProperty.BINARY_PAYLOAD)) {
            return;
        }

        List<URI> getUris = new ArrayList<>();
        if (services.get(0).hasOption(ServiceOption.PERSISTENCE)) {
            for (Service s : services) {
                // bypass the services, which rely on caching, and go straight to the index
                URI u = UriUtils.buildDocumentQueryUri(this, s.getSelfLink(), true, false,
                        ServiceOption.PERSISTENCE);
                getUris.add(u);

            }
        } else {
            for (Service s : services) {
                getUris.add(s.getUri());
            }
        }

        Map<URI, MinimalTestServiceState> statesAfterUpdate = getServiceState(
                properties,
                MinimalTestServiceState.class, getUris);

        for (MinimalTestServiceState st : statesAfterUpdate.values()) {
            URI serviceUri = UriUtils.buildUri(this, st.documentSelfLink);
            ServiceDocument beforeSt = statesBeforeUpdate.get(serviceUri);
            long expectedVersion = beforeSt.documentVersion + count;
            if (st.documentVersion != expectedVersion) {
                QueryTestUtils.logVersionInfoForService(this.sender, serviceUri, expectedVersion);
                throw new IllegalStateException("got " + st.documentVersion + ", expected "
                        + (beforeSt.documentVersion + count));
            }
            assertTrue(st.documentVersion == beforeSt.documentVersion + count);
            assertTrue(st.id != null);
            assertTrue(st.documentSelfLink != null
                    && st.documentSelfLink.equals(beforeSt.documentSelfLink));
            assertTrue(st.documentKind != null
                    && st.documentKind.equals(Utils.buildKind(MinimalTestServiceState.class)));
            assertTrue(st.documentUpdateTimeMicros > startTimeMicros);
            assertTrue(st.documentUpdateAction != null);
            assertTrue(st.documentUpdateAction.equals(action.toString()));
        }

        logMemoryInfo();

    }

    public void logMemoryInfo() {
        log("Memory free:%d, available:%s, total:%s", Runtime.getRuntime().freeMemory(),
                Runtime.getRuntime().totalMemory(),
                Runtime.getRuntime().maxMemory());
    }

    public URI getReferer() {
        if (this.referer == null) {
            this.referer = getUri();
        }
        return this.referer;
    }

    public void waitForServiceAvailable(String... links) {
        for (String link : links) {
            TestContext ctx = testCreate(1);
            this.registerForServiceAvailability(ctx.getCompletion(), link);
            ctx.await();
        }
    }

    public void waitForReplicatedFactoryServiceAvailable(URI u) {
        waitForReplicatedFactoryServiceAvailable(u, ServiceUriPaths.DEFAULT_NODE_SELECTOR);
    }

    public void waitForReplicatedFactoryServiceAvailable(URI u, String nodeSelectorPath) {
        waitFor("replicated available check time out for " + u, () -> {
            boolean[] isReady = new boolean[1];
            TestContext ctx = testCreate(1);
            NodeGroupUtils.checkServiceAvailability((o, e) -> {
                if (e != null) {
                    isReady[0] = false;
                    ctx.completeIteration();
                    return;
                }

                isReady[0] = true;
                ctx.completeIteration();
            }, this, u, nodeSelectorPath);
            ctx.await();
            return isReady[0];
        });
    }

    public void waitForServiceAvailable(URI u) {
        boolean[] isReady = new boolean[1];
        log("Starting /available check on %s", u);
        waitFor("available check timeout for " + u, () -> {
            TestContext ctx = testCreate(1);
            URI available = UriUtils.buildAvailableUri(u);
            Operation get = Operation.createGet(available).setCompletion((o, e) -> {
                if (e != null) {
                    // not ready
                    isReady[0] = false;
                    ctx.completeIteration();
                    return;
                }
                isReady[0] = true;
                ctx.completeIteration();
                return;
            });
            send(get);
            ctx.await();

            if (isReady[0]) {
                log("%s /available returned success", get.getUri());
                return true;
            }
            return false;
        });
    }

    public <T extends ServiceDocument> Map<URI, T> doFactoryChildServiceStart(
            EnumSet<TestProperty> props,
            long c,
            Class<T> bodyType,
            Consumer<Operation> setInitialStateConsumer,
            URI factoryURI) {
        Map<URI, T> initialStates = new HashMap<>();
        if (props == null) {
            props = EnumSet.noneOf(TestProperty.class);
        }

        log("Sending %d POST requests to %s", c, factoryURI);

        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < c; i++) {
            Operation createPost = Operation.createPost(factoryURI);
            // call callback to set the body
            setInitialStateConsumer.accept(createPost);
            if (props.contains(TestProperty.FORCE_REMOTE)) {
                createPost.forceRemote();
            }

            ops.add(createPost);
        }

        List<T> responses = this.sender.sendAndWait(ops, bodyType);
        Map<URI, T> docByChildURI = responses.stream().collect(
                toMap(doc -> UriUtils.buildUri(factoryURI, doc.documentSelfLink), identity()));
        initialStates.putAll(docByChildURI);
        log("Done with %d POST requests to %s", c, factoryURI);
        return initialStates;
    }

    public List<Service> doThroughputServiceStart(long c, Class<? extends Service> type,
            ServiceDocument initialState,
            EnumSet<Service.ServiceOption> options,
            EnumSet<Service.ServiceOption> optionsToRemove) throws Throwable {
        return doThroughputServiceStart(EnumSet.noneOf(TestProperty.class), c, type, initialState,
                options, null);
    }

    public List<Service> doThroughputServiceStart(
            EnumSet<TestProperty> props,
            long c, Class<? extends Service> type,
            ServiceDocument initialState,
            EnumSet<Service.ServiceOption> options,
            EnumSet<Service.ServiceOption> optionsToRemove) throws Throwable {
        return doThroughputServiceStart(props, c, type, initialState,
                options, optionsToRemove, null);
    }

    public List<Service> doThroughputServiceStart(
            EnumSet<TestProperty> props,
            long c, Class<? extends Service> type,
            ServiceDocument initialState,
            EnumSet<Service.ServiceOption> options,
            EnumSet<Service.ServiceOption> optionsToRemove,
            Long maintIntervalMicros) throws Throwable {

        List<Service> services = new ArrayList<>();

        TestContext ctx = testCreate((int) c);
        for (int i = 0; i < c; i++) {
            Service e = type.newInstance();
            if (options != null) {
                for (Service.ServiceOption cap : options) {
                    e.toggleOption(cap, true);
                }
            }
            if (optionsToRemove != null) {
                for (ServiceOption opt : optionsToRemove) {
                    e.toggleOption(opt, false);
                }
            }

            Operation post = createServiceStartPost(ctx);
            if (initialState != null) {
                post.setBody(initialState);
            }

            if (props != null && props.contains(TestProperty.SET_CONTEXT_ID)) {
                post.setContextId(TestProperty.SET_CONTEXT_ID.toString());
            }

            if (maintIntervalMicros != null) {
                e.setMaintenanceIntervalMicros(maintIntervalMicros);
            }

            startService(post, e);
            services.add(e);
        }
        ctx.await();
        logThroughput();
        return services;
    }

    public Service startServiceAndWait(Class<? extends Service> serviceType,
            String uriPath)
            throws Throwable {
        return startServiceAndWait(serviceType.newInstance(), uriPath, null);
    }

    public Service startServiceAndWait(Service s,
            String uriPath,
            ServiceDocument body)
            throws Throwable {
        TestContext ctx = testCreate(1);
        URI u = null;
        if (uriPath != null) {
            u = UriUtils.buildUri(this, uriPath);
        }

        Operation post = Operation
                .createPost(u)
                .setBody(body)
                .setCompletion(ctx.getCompletion());

        startService(post, s);
        ctx.await();
        return s;
    }

    public <T extends ServiceDocument> void doServiceRestart(List<Service> services,
            Class<T> stateType,
            EnumSet<Service.ServiceOption> caps)
            throws Throwable {
        ServiceDocumentDescription sdd = buildDescription(stateType);
        // first collect service state before shutdown so we can compare after
        // they restart
        Map<URI, T> statesBeforeRestart = getServiceState(null, stateType, services);

        List<Service> freshServices = new ArrayList<>();
        List<Operation> ops = new ArrayList<>();
        for (Service s : services) {
            // delete with no body means stop the service
            Operation delete = Operation.createDelete(s.getUri());
            ops.add(delete);
        }
        this.sender.sendAndWait(ops);

        // restart services
        TestContext ctx = testCreate(services.size());
        for (Service oldInstance : services) {
            Service e = oldInstance.getClass().newInstance();

            for (Service.ServiceOption cap : caps) {
                e.toggleOption(cap, true);
            }

            // use the same exact URI so the document index can find the service
            // state by self link
            startService(
                    Operation.createPost(oldInstance.getUri()).setCompletion(ctx.getCompletion()),
                    e);
            freshServices.add(e);
        }
        ctx.await();
        services = null;

        Map<URI, T> statesAfterRestart = getServiceState(null, stateType, freshServices);

        for (Entry<URI, T> e : statesAfterRestart.entrySet()) {
            T stateAfter = e.getValue();
            if (stateAfter.documentSelfLink == null) {
                throw new IllegalStateException("missing selflink");
            }
            if (stateAfter.documentKind == null) {
                throw new IllegalStateException("missing kind");
            }

            T stateBefore = statesBeforeRestart.get(e.getKey());
            if (stateBefore == null) {
                throw new IllegalStateException(
                        "New service has new self link, not in previous service instances");
            }

            if (!stateBefore.documentKind.equals(stateAfter.documentKind)) {
                throw new IllegalStateException("kind mismatch");
            }

            if (!caps.contains(Service.ServiceOption.PERSISTENCE)) {
                continue;
            }

            if (stateBefore.documentVersion != stateAfter.documentVersion) {
                String error = String.format(
                        "Version mismatch. Before State: %s%n%n After state:%s",
                        Utils.toJson(stateBefore),
                        Utils.toJson(stateAfter));
                throw new IllegalStateException(error);
            }

            if (stateBefore.documentUpdateTimeMicros != stateAfter.documentUpdateTimeMicros) {
                throw new IllegalStateException("update time mismatch");
            }

            if (stateBefore.documentVersion == 0) {
                throw new IllegalStateException("PUT did not appear to take place before restart");
            }
            if (!ServiceDocument.equals(sdd, stateBefore, stateAfter)) {
                throw new IllegalStateException("content signature mismatch");
            }
        }

    }

    private Map<String, NodeState> peerHostIdToNodeState = new ConcurrentHashMap<>();
    private Map<URI, URI> peerNodeGroups = new ConcurrentHashMap<>();
    private Map<URI, VerificationHost> localPeerHosts = new ConcurrentHashMap<>();

    private boolean isRemotePeerTest;

    private boolean isSingleton;

    public Map<URI, VerificationHost> getInProcessHostMap() {
        return new HashMap<>(this.localPeerHosts);
    }

    public Map<URI, URI> getNodeGroupMap() {
        return new HashMap<>(this.peerNodeGroups);
    }

    public Map<String, NodeState> getNodeStateMap() {
        return new HashMap<>(this.peerHostIdToNodeState);
    }

    public void scheduleSynchronizationIfAutoSyncDisabled(String selectorPath) {
        if (this.isPeerSynchronizationEnabled()) {
            return;
        }
        for (VerificationHost peerHost : getInProcessHostMap().values()) {
            peerHost.scheduleNodeGroupChangeMaintenance(selectorPath);
            ServiceStats selectorStats = getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(peerHost, selectorPath));
            ServiceStat synchStat = selectorStats.entries
                    .get(ConsistentHashingNodeSelectorService.STAT_NAME_SYNCHRONIZATION_COUNT);
            if (synchStat != null && synchStat.latestValue > 0) {
                throw new IllegalStateException("Automatic synchronization was triggered");
            }
        }
    }

    public void setUpPeerHosts(int localHostCount) {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.peerNodes == null) {
            this.setUpLocalPeersHosts(localHostCount, null);
        } else {
            this.setUpWithRemotePeers(this.peerNodes);
        }
    }

    public void setUpLocalPeersHosts(int localHostCount, Long maintIntervalMillis) {
        testStart(localHostCount);
        if (maintIntervalMillis == null) {
            maintIntervalMillis = this.maintenanceIntervalMillis;
        }
        final long intervalMicros = TimeUnit.MILLISECONDS.toMicros(maintIntervalMillis);
        for (int i = 0; i < localHostCount; i++) {
            String location = this.isMultiLocationTest
                    ? ((i < localHostCount / 2) ? LOCATION1 : LOCATION2)
                    : null;
            run(() -> {
                try {
                    this.setUpLocalPeerHost(null, intervalMicros, location);
                } catch (Throwable e) {
                    failIteration(e);
                }
            });
        }
        testWait();
    }

    public Map<URI, URI> getNodeGroupToFactoryMap(String factoryLink) {
        Map<URI, URI> nodeGroupToFactoryMap = new HashMap<>();
        for (URI nodeGroup : this.peerNodeGroups.values()) {
            nodeGroupToFactoryMap.put(nodeGroup,
                    UriUtils.buildUri(nodeGroup.getScheme(), nodeGroup.getHost(),
                            nodeGroup.getPort(), factoryLink, null));
        }
        return nodeGroupToFactoryMap;
    }

    public VerificationHost setUpLocalPeerHost(Collection<ServiceHost> hosts,
            long maintIntervalMicros) throws Throwable {
        return setUpLocalPeerHost(0, maintIntervalMicros, hosts);
    }

    public VerificationHost setUpLocalPeerHost(int port, long maintIntervalMicros,
            Collection<ServiceHost> hosts)
            throws Throwable {
        return setUpLocalPeerHost(port, maintIntervalMicros, hosts, null);
    }

    public VerificationHost setUpLocalPeerHost(Collection<ServiceHost> hosts,
            long maintIntervalMicros, String location) throws Throwable {
        return setUpLocalPeerHost(0, maintIntervalMicros, hosts, location);
    }

    public VerificationHost setUpLocalPeerHost(int port, long maintIntervalMicros,
            Collection<ServiceHost> hosts, String location)
            throws Throwable {

        VerificationHost h = VerificationHost.create(port);

        h.setPeerSynchronizationEnabled(this.isPeerSynchronizationEnabled());
        h.setAuthorizationEnabled(this.isAuthorizationEnabled());

        if (this.getCurrentHttpScheme() == HttpScheme.HTTPS_ONLY) {
            // disable HTTP on new peer host
            h.setPort(ServiceHost.PORT_VALUE_LISTENER_DISABLED);
            // request a random HTTPS port
            h.setSecurePort(0);
        }

        if (this.isAuthorizationEnabled()) {
            h.setAuthorizationService(new AuthorizationContextService());
        }
        try {
            VerificationHost.createAndAttachSSLClient(h);

            // override with parent cert info.
            // Within same node group, all hosts are required to use same cert, private key, and
            // passphrase for now.
            h.setCertificateFileReference(this.getState().certificateFileReference);
            h.setPrivateKeyFileReference(this.getState().privateKeyFileReference);
            h.setPrivateKeyPassphrase(this.getState().privateKeyPassphrase);
            if (location != null) {
                h.setLocation(location);
            }

            h.start();
            h.setMaintenanceIntervalMicros(maintIntervalMicros);
        } catch (Throwable e) {
            throw new Exception(e);
        }

        addPeerNode(h);
        if (hosts != null) {
            hosts.add(h);
        }
        this.completeIteration();
        return h;
    }

    public void setUpWithRemotePeers(String[] peerNodes) {
        this.isRemotePeerTest = true;

        this.peerNodeGroups.clear();
        for (String remoteNode : peerNodes) {
            URI remoteHostBaseURI = URI.create(remoteNode);
            if (remoteHostBaseURI.getPort() == 80 || remoteHostBaseURI.getPort() == -1) {
                remoteHostBaseURI = UriUtils.buildUri(remoteNode, ServiceHost.DEFAULT_PORT, "",
                        null);
            }

            URI remoteNodeGroup = UriUtils.extendUri(remoteHostBaseURI,
                    ServiceUriPaths.DEFAULT_NODE_GROUP);
            this.peerNodeGroups.put(remoteHostBaseURI, remoteNodeGroup);
        }

    }

    public void joinNodesAndVerifyConvergence(int nodeCount) throws Throwable {
        joinNodesAndVerifyConvergence(null, nodeCount, nodeCount, null);
    }

    public boolean isRemotePeerTest() {
        return this.isRemotePeerTest;
    }

    public int getPeerCount() {
        return this.peerNodeGroups.size();
    }

    public URI getPeerHostUri() {
        return getPeerServiceUri("");
    }

    public URI getPeerNodeGroupUri() {
        return getPeerServiceUri(ServiceUriPaths.DEFAULT_NODE_GROUP);
    }

    /**
     * Randomly returns one of peer hosts.
     */
    public VerificationHost getPeerHost() {
        URI hostUri = getPeerServiceUri(null);
        if (hostUri != null) {
            return this.localPeerHosts.get(hostUri);
        }
        return null;
    }

    public URI getPeerServiceUri(String link) {
        if (!this.localPeerHosts.isEmpty()) {
            List<URI> localPeerList = new ArrayList<>();
            for (VerificationHost h : this.localPeerHosts.values()) {
                if (h.isStopping() || !h.isStarted()) {
                    continue;
                }
                localPeerList.add(h.getUri());
            }
            return getUriFromList(link, localPeerList);
        } else {
            List<URI> peerList = new ArrayList<>(this.peerNodeGroups.keySet());
            return getUriFromList(link, peerList);
        }
    }

    /**
     * Randomly choose one uri from uriList and extend with the link
     */
    private URI getUriFromList(String link, List<URI> uriList) {
        if (!uriList.isEmpty()) {
            Collections.shuffle(uriList, new Random(System.nanoTime()));
            URI baseUri = uriList.iterator().next();
            return UriUtils.extendUri(baseUri, link);
        }
        return null;
    }

    public void createCustomNodeGroupOnPeers(String customGroupName) {
        createCustomNodeGroupOnPeers(customGroupName, null);
    }

    public void createCustomNodeGroupOnPeers(String customGroupName,
            Map<URI, NodeState> selfState) {
        if (selfState == null) {
            selfState = new HashMap<>();
        }
        // create a custom node group on all peer nodes
        List<Operation> ops = new ArrayList<>();
        for (URI peerHostBaseUri : getNodeGroupMap().keySet()) {
            URI nodeGroupFactoryUri = UriUtils.buildUri(peerHostBaseUri,
                    ServiceUriPaths.NODE_GROUP_FACTORY);
            Operation op = getCreateCustomNodeGroupOperation(customGroupName, nodeGroupFactoryUri,
                    selfState.get(peerHostBaseUri));
            ops.add(op);
        }
        this.sender.sendAndWait(ops);
    }

    private Operation getCreateCustomNodeGroupOperation(String customGroupName,
            URI nodeGroupFactoryUri,
            NodeState selfState) {
        NodeGroupState body = new NodeGroupState();
        body.documentSelfLink = customGroupName;
        if (selfState != null) {
            body.nodes.put(selfState.id, selfState);
        }
        return Operation.createPost(nodeGroupFactoryUri).setBody(body);
    }

    public void joinNodesAndVerifyConvergence(String customGroupPath, int hostCount,
            int memberCount,
            Map<URI, EnumSet<NodeOption>> expectedOptionsPerNode)
            throws Throwable {
        joinNodesAndVerifyConvergence(customGroupPath, hostCount, memberCount,
                expectedOptionsPerNode, true);
    }

    public void joinNodesAndVerifyConvergence(int hostCount, boolean waitForTimeSync)
            throws Throwable {
        joinNodesAndVerifyConvergence(hostCount, hostCount, waitForTimeSync);
    }

    public void joinNodesAndVerifyConvergence(int hostCount, int memberCount,
            boolean waitForTimeSync) throws Throwable {
        joinNodesAndVerifyConvergence(null, hostCount, memberCount, null, waitForTimeSync);
    }

    public void joinNodesAndVerifyConvergence(String customGroupPath, int hostCount,
            int memberCount,
            Map<URI, EnumSet<NodeOption>> expectedOptionsPerNode,
            boolean waitForTimeSync) throws Throwable {

        // invoke op as system user
        setAuthorizationContext(getSystemAuthorizationContext());
        if (hostCount == 0) {
            return;
        }

        Map<URI, URI> nodeGroupPerHost = new HashMap<>();
        if (customGroupPath != null) {
            for (Entry<URI, URI> e : this.peerNodeGroups.entrySet()) {
                URI ngUri = UriUtils.buildUri(e.getKey(), customGroupPath);
                nodeGroupPerHost.put(e.getKey(), ngUri);
            }
        } else {
            nodeGroupPerHost = this.peerNodeGroups;
        }

        if (this.isRemotePeerTest()) {
            memberCount = getPeerCount();
        } else {
            for (URI initialNodeGroupService : this.peerNodeGroups.values()) {
                if (customGroupPath != null) {
                    initialNodeGroupService = UriUtils.buildUri(initialNodeGroupService,
                            customGroupPath);
                }

                for (URI nodeGroup : this.peerNodeGroups.values()) {
                    if (customGroupPath != null) {
                        nodeGroup = UriUtils.buildUri(nodeGroup, customGroupPath);
                    }

                    if (initialNodeGroupService.equals(nodeGroup)) {
                        continue;
                    }

                    testStart(1);
                    joinNodeGroup(nodeGroup, initialNodeGroupService, memberCount);
                    testWait();
                }
            }

        }

        // for local or remote tests, we still want to wait for convergence
        waitForNodeGroupConvergence(nodeGroupPerHost.values(), memberCount, null,
                expectedOptionsPerNode, waitForTimeSync);

        waitForNodeGroupIsAvailableConvergence(customGroupPath);

        //reset auth context
        setAuthorizationContext(null);
    }

    public void joinNodeGroup(URI newNodeGroupService,
            URI nodeGroup, Integer quorum) {
        if (nodeGroup.equals(newNodeGroupService)) {
            return;
        }

        // to become member of a group of nodes, you send a POST to self
        // (the local node group service) with the URI of the remote node
        // group you wish to join
        JoinPeerRequest joinBody = JoinPeerRequest.create(nodeGroup, quorum);

        log("Joining %s through %s", newNodeGroupService, nodeGroup);
        // send the request to the node group instance we have picked as the
        // "initial" one
        send(Operation.createPost(newNodeGroupService)
                .setBody(joinBody)
                .setCompletion(getCompletion()));
    }

    public void joinNodeGroup(URI newNodeGroupService, URI nodeGroup) {
        joinNodeGroup(newNodeGroupService, nodeGroup, null);
    }

    public void subscribeForNodeGroupConvergence(URI nodeGroup, int expectedAvailableCount,
            CompletionHandler convergedCompletion) {

        TestContext ctx = testCreate(1);
        Operation subscribeToNodeGroup = Operation.createPost(
                UriUtils.buildSubscriptionUri(nodeGroup))
                .setCompletion(ctx.getCompletion())
                .setReferer(getUri());
        startSubscriptionService(subscribeToNodeGroup, (op) -> {
            op.complete();
            if (op.getAction() != Action.PATCH) {
                return;
            }

            NodeGroupState ngs = op.getBody(NodeGroupState.class);
            if (ngs.nodes == null && ngs.nodes.isEmpty()) {
                return;
            }

            int c = 0;
            for (NodeState ns : ngs.nodes.values()) {
                if (ns.status == NodeStatus.AVAILABLE) {
                    c++;
                }
            }

            if (c != expectedAvailableCount) {
                return;
            }
            convergedCompletion.handle(op, null);
        });
        ctx.await();
    }

    public void waitForNodeGroupIsAvailableConvergence() {
        waitForNodeGroupIsAvailableConvergence(ServiceUriPaths.DEFAULT_NODE_GROUP);
    }

    public void waitForNodeGroupIsAvailableConvergence(String nodeGroupPath) {
        waitForNodeGroupIsAvailableConvergence(nodeGroupPath, this.peerNodeGroups.values());
    }

    public void waitForNodeGroupIsAvailableConvergence(String nodeGroupPath,
            Collection<URI> nodeGroupUris) {
        if (nodeGroupPath == null) {
            nodeGroupPath = ServiceUriPaths.DEFAULT_NODE_GROUP;
        }
        String finalNodeGroupPath = nodeGroupPath;

        waitFor("Node group is not available for convergence", () -> {
            boolean isConverged = true;
            for (URI nodeGroupUri : nodeGroupUris) {
                URI u = UriUtils.buildUri(nodeGroupUri, finalNodeGroupPath);
                URI statsUri = UriUtils.buildStatsUri(u);
                ServiceStats stats = getServiceState(null, ServiceStats.class, statsUri);
                ServiceStat availableStat = stats.entries.get(Service.STAT_NAME_AVAILABLE);
                if (availableStat == null || availableStat.latestValue != Service.STAT_VALUE_TRUE) {
                    log("Service stat available is missing or not 1.0");
                    isConverged = false;
                    break;
                }
            }
            return isConverged;
        });

    }

    public void waitForNodeGroupConvergence(int memberCount) {
        waitForNodeGroupConvergence(memberCount, null);
    }

    public void waitForNodeGroupConvergence(int healthyMemberCount, Integer totalMemberCount) {
        waitForNodeGroupConvergence(this.peerNodeGroups.values(), healthyMemberCount,
                totalMemberCount, true);
    }

    public void waitForNodeGroupConvergence(Collection<URI> nodeGroupUris, int healthyMemberCount,
            Integer totalMemberCount,
            boolean waitForTimeSync) {
        waitForNodeGroupConvergence(nodeGroupUris, healthyMemberCount, totalMemberCount,
                new HashMap<>(), waitForTimeSync);
    }

    /**
     * Check node group convergence.
     *
     * Due to the implementation of {@link NodeGroupUtils#isNodeGroupAvailable}, quorum needs to
     * be set less than the available node counts.
     *
     * Since {@link TestNodeGroupManager} requires all passing nodes to be in a same nodegroup,
     * hosts in in-memory host map({@code this.localPeerHosts}) that do not match with the given
     * nodegroup will be skipped for check.
     *
     * For existing API compatibility, keeping unused variables in signature.
     * Only {@code nodeGroupUris} parameter is used.
     *
     * Sample node group URI: http://127.0.0.1:8000/core/node-groups/default
     *
     * @see TestNodeGroupManager#waitForConvergence()
     */
    public void waitForNodeGroupConvergence(Collection<URI> nodeGroupUris,
            int healthyMemberCount,
            Integer totalMemberCount,
            Map<URI, EnumSet<NodeOption>> expectedOptionsPerNodeGroupUri,
            boolean waitForTimeSync) {

        Set<String> nodeGroupNames = nodeGroupUris.stream()
                .map(URI::getPath)
                .map(UriUtils::getLastPathSegment)
                .collect(toSet());
        if (nodeGroupNames.size() != 1) {
            throw new RuntimeException("Multiple nodegroups are not supported. " + nodeGroupNames);
        }
        String nodeGroupName = nodeGroupNames.iterator().next();

        Date exp = getTestExpiration();
        Duration timeout = Duration.between(Instant.now(), exp.toInstant());

        // Convert "http://127.0.0.1:1234/core/node-groups/default" to "http://127.0.0.1:1234"
        Set<URI> baseUris = nodeGroupUris.stream()
                .map(uri -> uri.toString().replace(uri.getPath(), ""))
                .map(URI::create)
                .collect(toSet());

        // pick up hosts that match with the base uris of given node group uris
        Set<ServiceHost> hosts = getInProcessHostMap().values().stream()
                .filter(host -> baseUris.contains(host.getPublicUri()))
                .collect(toSet());

        // perform "waitForConvergence()"
        if (hosts != null && !hosts.isEmpty()) {
            TestNodeGroupManager manager = new TestNodeGroupManager(nodeGroupName);
            manager.addHosts(hosts);
            manager.setTimeout(timeout);
            manager.waitForConvergence();
        } else {
            this.waitFor("Node group did not converge", () -> {
                String nodeGroupPath = ServiceUriPaths.NODE_GROUP_FACTORY + "/" + nodeGroupName;
                List<Operation> nodeGroupOps = baseUris.stream()
                        .map(u -> UriUtils.buildUri(u, nodeGroupPath))
                        .map(Operation::createGet)
                        .collect(toList());
                List<NodeGroupState> nodeGroupStates = getTestRequestSender()
                        .sendAndWait(nodeGroupOps, NodeGroupState.class);

                for (NodeGroupState nodeGroupState : nodeGroupStates) {
                    TestContext testContext = this.testCreate(1);
                    // placeholder operation
                    Operation parentOp = Operation.createGet(null)
                            .setReferer(this.getUri())
                            .setCompletion(testContext.getCompletion());
                    try {
                        NodeGroupUtils.checkConvergenceFromAnyHost(this, nodeGroupState, parentOp);
                        testContext.await();
                    } catch (Exception e) {
                        return false;
                    }
                }
                return true;
            });
        }

        // To be compatible with old behavior, populate peerHostIdToNodeState same way as before
        List<Operation> nodeGroupGetOps = nodeGroupUris.stream()
                .map(UriUtils::buildExpandLinksQueryUri)
                .map(Operation::createGet)
                .collect(toList());
        List<NodeGroupState> nodeGroupStats = this.sender.sendAndWait(nodeGroupGetOps, NodeGroupState.class);

        for (NodeGroupState nodeGroupStat : nodeGroupStats) {
            for (NodeState nodeState : nodeGroupStat.nodes.values()) {
                if (nodeState.status == NodeStatus.AVAILABLE) {
                    this.peerHostIdToNodeState.put(nodeState.id, nodeState);
                }
            }
        }
    }

    public int calculateHealthyNodeCount(NodeGroupState r) {
        int healthyNodeCount = 0;
        for (NodeState ns : r.nodes.values()) {
            if (ns.status == NodeStatus.AVAILABLE) {
                healthyNodeCount++;
            }
        }
        return healthyNodeCount;
    }

    public void getNodeState(URI nodeGroup, Map<URI, NodeGroupState> nodesPerHost) {
        getNodeState(nodeGroup, nodesPerHost, null);
    }

    public void getNodeState(URI nodeGroup, Map<URI, NodeGroupState> nodesPerHost,
            TestContext ctx) {
        URI u = UriUtils.buildExpandLinksQueryUri(nodeGroup);
        Operation get = Operation.createGet(u).setCompletion((o, e) -> {
            NodeGroupState ngs = null;
            if (e != null) {
                // failure is OK, since we might have just stopped a host
                log("Host %s failed GET with %s", nodeGroup, e.getMessage());
                ngs = new NodeGroupState();
            } else {
                ngs = o.getBody(NodeGroupState.class);
            }
            synchronized (nodesPerHost) {
                nodesPerHost.put(nodeGroup, ngs);
            }
            if (ctx == null) {
                completeIteration();
            } else {
                ctx.completeIteration();
            }
        });
        send(get);
    }

    public void validateNodes(NodeGroupState r, int expectedNodesPerGroup,
            Map<URI, EnumSet<NodeOption>> expectedOptionsPerNode) {

        int healthyNodes = 0;
        NodeState localNode = null;
        for (NodeState ns : r.nodes.values()) {
            if (ns.status == NodeStatus.AVAILABLE) {
                healthyNodes++;
            }
            assertTrue(ns.documentKind.equals(Utils.buildKind(NodeState.class)));
            if (ns.documentSelfLink.endsWith(r.documentOwner)) {
                localNode = ns;
            }

            assertTrue(ns.options != null);
            EnumSet<NodeOption> expectedOptions = expectedOptionsPerNode.get(ns.groupReference);
            if (expectedOptions == null) {
                expectedOptions = NodeState.DEFAULT_OPTIONS;
            }

            for (NodeOption eo : expectedOptions) {
                assertTrue(ns.options.contains(eo));
            }

            assertTrue(ns.id != null);
            assertTrue(ns.groupReference != null);
            assertTrue(ns.documentSelfLink.startsWith(ns.groupReference.getPath()));
        }

        assertTrue(healthyNodes >= expectedNodesPerGroup);
        assertTrue(localNode != null);
    }

    public void doNodeGroupStatsVerification(Map<URI, URI> defaultNodeGroupsPerHost) {
        List<Operation> ops = new ArrayList<>();
        for (URI nodeGroup : defaultNodeGroupsPerHost.values()) {
            Operation get = Operation.createGet(UriUtils.extendUri(nodeGroup,
                    ServiceHost.SERVICE_URI_SUFFIX_STATS));
            ops.add(get);
        }
        List<Operation> results = this.sender.sendAndWait(ops);
        for (Operation result : results) {
            ServiceStats stats = result.getBody(ServiceStats.class);
            assertTrue(!stats.entries.isEmpty());
        }
    }

    public void setNodeGroupConfig(NodeGroupConfig config) {
        setSystemAuthorizationContext();
        List<Operation> ops = new ArrayList<>();
        for (URI nodeGroup : getNodeGroupMap().values()) {
            NodeGroupState body = new NodeGroupState();
            body.config = config;
            body.nodes = null;
            ops.add(Operation.createPatch(nodeGroup).setBody(body));
        }
        this.sender.sendAndWait(ops);
        resetAuthorizationContext();
    }

    public void setNodeGroupQuorum(Integer quorum)
            throws Throwable {
        // we can issue the update to any one node and it will update
        // everyone in the group

        setSystemAuthorizationContext();

        for (URI nodeGroup : getNodeGroupMap().values()) {
            log("Changing quorum to %d on group %s", quorum, nodeGroup);
            setNodeGroupQuorum(quorum, nodeGroup);
            // nodes might not be joined, so we need to ask each node to set quorum
        }

        Date exp = getTestExpiration();
        while (new Date().before(exp)) {
            boolean isConverged = true;
            setSystemAuthorizationContext();
            for (URI n : this.peerNodeGroups.values()) {
                NodeGroupState s = getServiceState(null, NodeGroupState.class, n);
                for (NodeState ns : s.nodes.values()) {
                    if (quorum != ns.membershipQuorum) {
                        isConverged = false;
                    }
                }
            }
            resetAuthorizationContext();
            if (isConverged) {

                log("converged");
                return;
            }
            Thread.sleep(500);
        }
        waitForNodeSelectorQuorumConvergence(ServiceUriPaths.DEFAULT_NODE_SELECTOR, quorum);
        resetAuthorizationContext();

        throw new TimeoutException();
    }

    public void waitForNodeSelectorQuorumConvergence(String nodeSelectorPath, int quorum) {
        waitFor("quorum not updated", () -> {
            for (URI peerHostUri : getNodeGroupMap().keySet()) {
                URI nodeSelectorUri = UriUtils.buildUri(peerHostUri, nodeSelectorPath);
                NodeSelectorState nss = getServiceState(null, NodeSelectorState.class,
                        nodeSelectorUri);
                if (nss.membershipQuorum != quorum) {
                    return false;
                }
            }
            return true;
        });
    }

    public void setNodeGroupQuorum(Integer quorum, URI nodeGroup) {
        UpdateQuorumRequest body = UpdateQuorumRequest.create(true);

        if (quorum != null) {
            body.setMembershipQuorum(quorum);
        }

        this.sender.sendAndWait(Operation.createPatch(nodeGroup).setBody(body));
    }

    public <T extends ServiceDocument> void validateDocumentPartitioning(
            Map<URI, T> provisioningTasks,
            Class<T> type) {
        Map<String, Map<String, Long>> taskToOwnerCount = new HashMap<>();

        for (URI baseHostURI : getNodeGroupMap().keySet()) {
            List<URI> documentsPerDcpHost = new ArrayList<>();
            for (URI serviceUri : provisioningTasks.keySet()) {
                URI u = UriUtils.extendUri(baseHostURI, serviceUri.getPath());
                documentsPerDcpHost.add(u);
            }

            Map<URI, T> tasksOnThisHost = getServiceState(
                    null,
                    type, documentsPerDcpHost);

            for (T task : tasksOnThisHost.values()) {
                Map<String, Long> ownerCount = taskToOwnerCount.get(task.documentSelfLink);
                if (ownerCount == null) {
                    ownerCount = new HashMap<>();
                    taskToOwnerCount.put(task.documentSelfLink, ownerCount);
                }

                Long count = ownerCount.get(task.documentOwner);
                if (count == null) {
                    count = 0L;
                }
                count++;
                ownerCount.put(task.documentOwner, count);
            }
        }

        // now verify that each task had a single owner assigned to it
        for (Entry<String, Map<String, Long>> e : taskToOwnerCount.entrySet()) {
            Map<String, Long> owners = e.getValue();
            if (owners.size() > 1) {
                throw new IllegalStateException("Multiple owners assigned on task " + e.getKey());
            }
        }

    }

    public void createExampleServices(ServiceHost h, long serviceCount, List<URI> exampleURIs,
            Long expiration) {
        waitForServiceAvailable(ExampleService.FACTORY_LINK);
        ExampleServiceState initialState = new ExampleServiceState();
        URI exampleFactoryUri = UriUtils.buildFactoryUri(h,
                ExampleService.class);

        // create example services
        List<Operation> ops = new ArrayList<>();
        for (int i = 0; i < serviceCount; i++) {
            initialState.counter = 123L;
            if (expiration != null) {
                initialState.documentExpirationTimeMicros = expiration;
            }
            initialState.name = initialState.documentSelfLink = UUID.randomUUID().toString();
            exampleURIs.add(UriUtils.extendUri(exampleFactoryUri, initialState.documentSelfLink));

            Operation createPost = Operation.createPost(exampleFactoryUri).setBody(initialState);
            ops.add(createPost);
        }
        this.sender.sendAndWait(ops);
    }

    public Date getTestExpiration() {
        long duration = this.timeoutSeconds + this.testDurationSeconds;
        return new Date(new Date().getTime()
                + TimeUnit.SECONDS.toMillis(duration));
    }

    public boolean isStressTest() {
        return this.isStressTest;
    }

    public void setStressTest(boolean isStressTest) {
        this.isStressTest = isStressTest;
        if (isStressTest) {
            this.timeoutSeconds = 600;
            this.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(this.timeoutSeconds));
        } else {
            this.timeoutSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(
                    ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS);
        }
    }

    public boolean isMultiLocationTest() {
        return this.isMultiLocationTest;
    }

    public void setMultiLocationTest(boolean isMultiLocationTest) {
        this.isMultiLocationTest = isMultiLocationTest;
    }

    public void toggleServiceOptions(URI serviceUri, EnumSet<ServiceOption> optionsToEnable,
            EnumSet<ServiceOption> optionsToDisable) {

        ServiceConfigUpdateRequest updateBody = ServiceConfigUpdateRequest.create();
        updateBody.removeOptions = optionsToDisable;
        updateBody.addOptions = optionsToEnable;

        URI configUri = UriUtils.buildConfigUri(serviceUri);
        this.sender.sendAndWait(Operation.createPatch(configUri).setBody(updateBody));
    }

    public void setOperationQueueLimit(URI serviceUri, int limit) {
        // send a set limit configuration request
        ServiceConfigUpdateRequest body = ServiceConfigUpdateRequest.create();
        body.operationQueueLimit = limit;
        URI configUri = UriUtils.buildConfigUri(serviceUri);
        this.sender.sendAndWait(Operation.createPatch(configUri).setBody(body));

        // verify new operation limit is set
        ServiceConfiguration config = this.sender.sendAndWait(Operation.createGet(configUri),
                ServiceConfiguration.class);
        assertEquals("Invalid queue limit", body.operationQueueLimit,
                (Integer) config.operationQueueLimit);
    }

    public void toggleNegativeTestMode(boolean enable) {
        log("++++++ Negative test mode %s, failure logs expected: %s", enable, enable);
    }

    public void logNodeProcessLogs(Set<URI> keySet, String logSuffix) {
        List<URI> logServices = new ArrayList<>();
        for (URI host : keySet) {
            logServices.add(UriUtils.extendUri(host, logSuffix));
        }

        Map<URI, LogServiceState> states = this.getServiceState(null, LogServiceState.class,
                logServices);
        for (Entry<URI, LogServiceState> entry : states.entrySet()) {
            log("Process log for node %s\n\n%s", entry.getKey(),
                    Utils.toJsonHtml(entry.getValue()));
        }
    }

    public void logNodeManagementState(Set<URI> keySet) {
        List<URI> services = new ArrayList<>();
        for (URI host : keySet) {
            services.add(UriUtils.extendUri(host, ServiceUriPaths.CORE_MANAGEMENT));
        }

        Map<URI, ServiceHostState> states = this.getServiceState(null, ServiceHostState.class,
                services);
        for (Entry<URI, ServiceHostState> entry : states.entrySet()) {
            log("Management state for node %s\n\n%s", entry.getKey(),
                    Utils.toJsonHtml(entry.getValue()));
        }
    }

    public void tearDownInProcessPeers() {
        for (VerificationHost h : this.localPeerHosts.values()) {
            if (h == null) {
                continue;
            }
            stopHost(h);
        }
    }

    public void stopHost(VerificationHost host) {
        log("Stopping host %s (%s)", host.getUri(), host.getId());
        host.tearDown();
        this.peerHostIdToNodeState.remove(host.getId());
        this.peerNodeGroups.remove(host.getUri());
        this.localPeerHosts.remove(host.getUri());
    }

    public void stopHostAndPreserveState(ServiceHost host) {
        log("Stopping host %s", host.getUri());
        // Do not delete the temporary directory with the lucene index. Notice that
        // we do not call host.tearDown(), which will delete disk state, we simply
        // stop the host and remove it from the peer node tracking tables
        host.stop();
        this.peerHostIdToNodeState.remove(host.getId());
        this.peerNodeGroups.remove(host.getUri());
        this.localPeerHosts.remove(host.getUri());
    }

    public boolean isLongDurationTest() {
        return this.testDurationSeconds > 0;
    }

    public void logServiceStats(URI uri) {
        ServiceStats stats = getServiceState(null, ServiceStats.class, UriUtils.buildStatsUri(uri));
        if (stats == null || stats.entries == null) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Stats for %s%n", uri));
        sb.append(String.format("\tCount\t\tAvg\t\tTotal\t\t\tName%n"));
        for (ServiceStat st : stats.entries.values()) {
            logStat(uri, st, sb);
        }
        log(sb.toString());
    }

    private void logStat(URI serviceUri, ServiceStat st, StringBuilder sb) {
        ServiceStatLogHistogram hist = st.logHistogram;
        st.logHistogram = null;

        double total = st.accumulatedValue != 0 ? st.accumulatedValue : st.latestValue;
        double avg = total / st.version;
        sb.append(
                String.format("\t%08d\t\t%08.2f\t%010.2f\t%s%n", st.version, avg, total, st.name));
        if (hist == null) {
            return;
        }
    }

    /**
     * Retrieves node group service state from all peers and logs it in JSON format
     */
    public void logNodeGroupState() {
        List<Operation> ops = new ArrayList<>();
        for (URI nodeGroup : getNodeGroupMap().values()) {
            ops.add(Operation.createGet(nodeGroup));
        }
        List<NodeGroupState> stats = this.sender.sendAndWait(ops, NodeGroupState.class);
        for (NodeGroupState stat : stats) {
            log("%s", Utils.toJsonHtml(stat));
        }
    }

    public void setServiceMaintenanceIntervalMicros(String path, long micros) {
        setServiceMaintenanceIntervalMicros(UriUtils.buildUri(this, path), micros);
    }

    public void setServiceMaintenanceIntervalMicros(URI u, long micros) {
        ServiceConfigUpdateRequest updateBody = ServiceConfigUpdateRequest.create();
        updateBody.maintenanceIntervalMicros = micros;
        URI configUri = UriUtils.extendUri(u, ServiceHost.SERVICE_URI_SUFFIX_CONFIG);
        this.sender.sendAndWait(Operation.createPatch(configUri).setBody(updateBody));
    }

    /**
     * Toggles the operation tracing service
     *
     * @param baseHostURI  the uri of the tracing service
     * @param enable state to toggle to
     */
    public void toggleOperationTracing(URI baseHostURI, boolean enable) {
        ServiceHostManagementService.ConfigureOperationTracingRequest r = new ServiceHostManagementService.ConfigureOperationTracingRequest();
        r.enable = enable ? ServiceHostManagementService.OperationTracingEnable.START
                : ServiceHostManagementService.OperationTracingEnable.STOP;
        r.kind = ServiceHostManagementService.ConfigureOperationTracingRequest.KIND;

        this.setSystemAuthorizationContext();
        this.sender.sendAndWait(Operation.createPatch(
                UriUtils.extendUri(baseHostURI, ServiceHostManagementService.SELF_LINK))
                .setBody(r));
        this.resetAuthorizationContext();
    }

    public CompletionHandler getSuccessOrFailureCompletion() {
        return (o, e) -> {
            completeIteration();
        };
    }

    public static QueryValidationServiceState buildQueryValidationState() {
        QueryValidationServiceState newState = new QueryValidationServiceState();

        newState.ignoredStringValue = "should be ignored by index";
        newState.exampleValue = new ExampleServiceState();
        newState.exampleValue.counter = 10L;
        newState.exampleValue.name = "example name";

        newState.nestedComplexValue = new NestedType();
        newState.nestedComplexValue.id = UUID.randomUUID().toString();
        newState.nestedComplexValue.longValue = Long.MIN_VALUE;

        newState.listOfExampleValues = new ArrayList<>();
        ExampleServiceState exampleItem = new ExampleServiceState();
        exampleItem.name = "nested name";
        newState.listOfExampleValues.add(exampleItem);

        newState.listOfStrings = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            newState.listOfStrings.add(UUID.randomUUID().toString());
        }

        newState.arrayOfExampleValues = new ExampleServiceState[2];
        newState.arrayOfExampleValues[0] = new ExampleServiceState();
        newState.arrayOfExampleValues[0].name = UUID.randomUUID().toString();

        newState.arrayOfStrings = new String[2];
        newState.arrayOfStrings[0] = UUID.randomUUID().toString();
        newState.arrayOfStrings[1] = UUID.randomUUID().toString();

        newState.mapOfStrings = new HashMap<>();
        String keyOne = "keyOne";
        String keyTwo = "keyTwo";
        String valueOne = UUID.randomUUID().toString();
        String valueTwo = UUID.randomUUID().toString();
        newState.mapOfStrings.put(keyOne, valueOne);
        newState.mapOfStrings.put(keyTwo, valueTwo);

        newState.mapOfBooleans = new HashMap<>();
        newState.mapOfBooleans.put("trueKey", true);
        newState.mapOfBooleans.put("falseKey", false);

        newState.mapOfBytesArrays = new HashMap<>();
        newState.mapOfBytesArrays.put("bytes", new byte[] { 0x01, 0x02 });

        newState.mapOfDoubles = new HashMap<>();
        newState.mapOfDoubles.put("one", 1.0);
        newState.mapOfDoubles.put("minusOne", -1.0);

        newState.mapOfEnums = new HashMap<>();
        newState.mapOfEnums.put("GET", Service.Action.GET);

        newState.mapOfLongs = new HashMap<>();
        newState.mapOfLongs.put("one", 1L);
        newState.mapOfLongs.put("two", 2L);

        newState.mapOfNestedTypes = new HashMap<>();
        newState.mapOfNestedTypes.put("nested", newState.nestedComplexValue);

        newState.mapOfUris = new HashMap<>();
        newState.mapOfUris.put("uri", UriUtils.buildUri("/foo/bar"));

        newState.ignoredArrayOfStrings = new String[2];
        newState.ignoredArrayOfStrings[0] = UUID.randomUUID().toString();
        newState.ignoredArrayOfStrings[1] = UUID.randomUUID().toString();

        newState.binaryContent = UUID.randomUUID().toString().getBytes();
        return newState;
    }

    public void updateServiceOptions(Collection<String> selfLinks,
            ServiceConfigUpdateRequest cfgBody) {

        List<Operation> ops = new ArrayList<>();
        for (String link : selfLinks) {
            URI bUri = UriUtils.buildUri(getUri(), link,
                    ServiceHost.SERVICE_URI_SUFFIX_CONFIG);

            ops.add(Operation.createPatch(bUri).setBody(cfgBody));
        }
        this.sender.sendAndWait(ops);
    }

    public void addPeerNode(VerificationHost h) {
        URI localBaseURI = h.getPublicUri();
        URI nodeGroup = UriUtils.buildUri(h.getPublicUri(), ServiceUriPaths.DEFAULT_NODE_GROUP);
        this.peerNodeGroups.put(localBaseURI, nodeGroup);
        this.localPeerHosts.put(localBaseURI, h);
    }

    public void addPeerNode(URI ngUri) {
        URI hostUri = UriUtils.buildUri(ngUri.getScheme(), ngUri.getHost(), ngUri.getPort(), null,
                null);
        this.peerNodeGroups.put(hostUri, ngUri);
    }

    public ServiceDocumentDescription buildDescription(Class<? extends ServiceDocument> type) {
        EnumSet<ServiceOption> options = EnumSet.noneOf(ServiceOption.class);
        return Builder.create().buildDescription(type, options);
    }

    public void logAllDocuments(Set<URI> baseHostUris) {
        QueryTask task = new QueryTask();
        task.setDirect(true);
        task.querySpec = new QuerySpecification();
        task.querySpec.query.setTermPropertyName("documentSelfLink").setTermMatchValue("*");
        task.querySpec.query.setTermMatchType(MatchType.WILDCARD);
        task.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT);

        List<Operation> ops = new ArrayList<>();
        for (URI baseHost : baseHostUris) {
            Operation queryPost = Operation
                    .createPost(UriUtils.buildUri(baseHost, ServiceUriPaths.CORE_QUERY_TASKS))
                    .setBody(task);
            ops.add(queryPost);
        }
        List<QueryTask> queryTasks = this.sender.sendAndWait(ops, QueryTask.class);
        for (QueryTask queryTask : queryTasks) {
            log(Utils.toJsonHtml(queryTask));
        }
    }

    public void setSystemAuthorizationContext() {
        setAuthorizationContext(getSystemAuthorizationContext());
    }

    public void resetSystemAuthorizationContext() {
        super.setAuthorizationContext(null);
    }

    @Override
    public void addPrivilegedService(Class<? extends Service> serviceType) {
        // Overriding just for test cases
        super.addPrivilegedService(serviceType);
    }

    @Override
    public void setAuthorizationContext(AuthorizationContext context) {
        super.setAuthorizationContext(context);
    }

    public void resetAuthorizationContext() {
        super.setAuthorizationContext(null);
    }

    /**
     * Inject user identity into operation context.
     *
     * @param userServicePath user document link
     */
    public AuthorizationContext assumeIdentity(String userServicePath)
            throws GeneralSecurityException {
        return assumeIdentity(userServicePath, null);
    }

    /**
     * Inject user identity into operation context.
     *
     * @param userServicePath user document link
     * @param properties custom properties in claims
     * @throws GeneralSecurityException any generic security exception
     */
    public AuthorizationContext assumeIdentity(String userServicePath,
            Map<String, String> properties) throws GeneralSecurityException {
        Claims.Builder builder = new Claims.Builder();
        builder.setSubject(userServicePath);
        builder.setProperties(properties);
        Claims claims = builder.getResult();
        String token = getTokenSigner().sign(claims);

        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(token);

        // Associate resulting authorization context with this thread
        AuthorizationContext authContext = ab.getResult();
        setAuthorizationContext(authContext);
        return authContext;
    }

    public void deleteAllChildServices(URI factoryURI) {
        deleteOrStopAllChildServices(factoryURI, false);
    }

    public void deleteOrStopAllChildServices(URI factoryURI, boolean stopOnly) {
        ServiceDocumentQueryResult res = getFactoryState(factoryURI);
        if (res.documentLinks.isEmpty()) {
            return;
        }
        List<Operation> ops = new ArrayList<>();
        for (String link : res.documentLinks) {
            Operation op = Operation.createDelete(UriUtils.buildUri(factoryURI, link));
            if (stopOnly) {
                op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE);
            } else {
                op.addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                        Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL);
            }
            ops.add(op);
        }
        this.sender.sendAndWait(ops);
    }

    public <T extends ServiceDocument> ServiceDocument verifyPost(Class<T> documentType,
            String factoryLink,
            T state,
            int expectedStatusCode) {
        URI uri = UriUtils.buildUri(this, factoryLink);

        Operation op = Operation.createPost(uri).setBody(state);
        Operation response = this.sender.sendAndWait(op);
        String message = String.format("Status code expected: %s, actual: %s", expectedStatusCode,
                response.getStatusCode());
        assertEquals(message, expectedStatusCode, response.getStatusCode());

        return response.getBody(documentType);
    }

    protected TemporaryFolder getTemporaryFolder() {
        return this.temporaryFolder;
    }

    public void setTemporaryFolder(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    /**
     * Sends an operation and waits for completion. CompletionHandler on passed operation will be cleared.
     */
    public void sendAndWaitExpectSuccess(Operation op) {
        // to be compatible with old behavior, clear the completion handler
        op.setCompletion(null);

        this.sender.sendAndWait(op);
    }

    public void sendAndWaitExpectFailure(Operation op) {
        sendAndWaitExpectFailure(op, null);
    }

    public void sendAndWaitExpectFailure(Operation op, Integer expectedFailureCode) {

        // to be compatible with old behavior, clear the completion handler
        op.setCompletion(null);

        FailureResponse resposne = this.sender.sendAndWaitFailure(op);

        if (expectedFailureCode == null) {
            return;
        }
        String msg = "got unexpected status: " + expectedFailureCode;
        assertEquals(msg, (int) expectedFailureCode, resposne.op.getStatusCode());
    }

    /**
     * Sends an operation and waits for completion.
     */
    public void sendAndWait(Operation op) {
        // assume completion is attached, using our getCompletion() or
        // getExpectedFailureCompletion()
        testStart(1);
        send(op);
        testWait();
    }

    /**
     * Sends an operation, waits for completion and return the response representation.
     */
    public Operation waitForResponse(Operation op) {
        final Operation[] result = new Operation[1];
        op.nestCompletion((o, e) -> {
            result[0] = o;
            completeIteration();
        });

        sendAndWait(op);

        return result[0];
    }

    /**
     * Decorates a {@link CompletionHandler} with a try/catch-all
     * and fails the current iteration on exception. Allow for calling
     * Assert.assert* directly in a handler.
     *
     * A safe handler will call completeIteration or failIteration exactly once.
     *
     * @param handler
     * @return
     */
    public CompletionHandler getSafeHandler(CompletionHandler handler) {
        return (o, e) -> {
            try {
                handler.handle(o, e);
                completeIteration();
            } catch (Throwable t) {
                failIteration(t);
            }
        };
    }

    public CompletionHandler getSafeHandler(TestContext ctx, CompletionHandler handler) {
        return (o, e) -> {
            try {
                handler.handle(o, e);
                ctx.completeIteration();
            } catch (Throwable t) {
                ctx.failIteration(t);
            }
        };
    }

    /**
     * Creates a new service instance of type {@code service} via a {@code HTTP POST} to the service
     * factory URI (which is discovered automatically based on {@code service}). It passes {@code
     * state} as the body of the {@code POST}.
     * <p/>
     * See javadoc for <i>handler</i> param for important details on how to properly use this
     * method. If your test expects the service instance to be created successfully, you might use:
     * <pre>
     * String[] taskUri = new String[1];
     * CompletionHandler successHandler = getCompletionWithUri(taskUri);
     * sendFactoryPost(ExampleTaskService.class, new ExampleTaskServiceState(), successHandler);
     * </pre>
     *
     * @param service the type of service to create
     * @param state   the body of the {@code POST} to use to create the service instance
     * @param handler the completion handler to use when creating the service instance.
     *                <b>IMPORTANT</b>: This handler must properly call {@code host.failIteration()}
     *                or {@code host.completeIteration()}.
     * @param <T>     the state that represents the service instance
     */
    public <T extends ServiceDocument> void sendFactoryPost(Class<? extends Service> service,
            T state, CompletionHandler handler) {
        URI factoryURI = UriUtils.buildFactoryUri(this, service);
        log(Level.INFO, "Creating POST for [uri=%s] [body=%s]", factoryURI, state);
        Operation createPost = Operation.createPost(factoryURI)
                .setBody(state)
                .setCompletion(handler);

        this.sender.sendAndWait(createPost);
    }

    /**
     * Helper completion handler that:
     * <ul>
     * <li>Expects valid response to be returned; no exceptions when processing the operation</li>
     * <li>Expects a {@code ServiceDocument} to be returned in the response body. The response's
     * {@link ServiceDocument#documentSelfLink} will be stored in {@code storeUri[0]} so it can be
     * used for test assertions and logic</li>
     * </ul>
     *
     * @param storedLink The {@code documentSelfLink} of the created {@code ServiceDocument} will be
     *                 stored in {@code storedLink[0]} so it can be used for test assertions and
     *                 logic. This must be non-null and its length cannot be zero
     * @return a completion handler, handy for using in methods like {@link
     * #sendFactoryPost(Class, ServiceDocument, CompletionHandler)}
     */
    public CompletionHandler getCompletionWithSelflink(String[] storedLink) {
        if (storedLink == null || storedLink.length == 0) {
            throw new IllegalArgumentException(
                    "storeUri must be initialized and have room for at least one item");
        }

        return (op, ex) -> {
            if (ex != null) {
                failIteration(ex);
                return;
            }

            ServiceDocument response = op.getBody(ServiceDocument.class);
            if (response == null) {
                failIteration(new IllegalStateException(
                        "Expected non-null ServiceDocument in response body"));
                return;
            }

            log(Level.INFO, "Created service instance. [selfLink=%s] [kind=%s]",
                    response.documentSelfLink, response.documentKind);
            storedLink[0] = response.documentSelfLink;
            completeIteration();
        };
    }

    /**
     * Helper completion handler that:
     * <ul>
     * <li>Expects an exception when processing the handler; it is a {@code failIteration} if an
     * exception is <b>not</b> thrown.</li>
     * <li>The exception will be stored in {@code storeException[0]} so it can be used for test
     * assertions and logic.</li>
     * </ul>
     *
     * @param storeException the exception that occurred in completion handler will be stored in
     *                       {@code storeException[0]} so it can be used for test assertions and
     *                       logic. This must be non-null and its length cannot be zero.
     * @return a completion handler, handy for using in methods like {@link
     * #sendFactoryPost(Class, ServiceDocument, CompletionHandler)}
     */
    public CompletionHandler getExpectedFailureCompletionReturningThrowable(
            Throwable[] storeException) {
        if (storeException == null || storeException.length == 0) {
            throw new IllegalArgumentException(
                    "storeException must be initialized and have room for at least one item");
        }

        return (op, ex) -> {
            if (ex == null) {
                failIteration(new IllegalStateException("Failure expected"));
            }
            storeException[0] = ex;
            completeIteration();
        };
    }

    /**
     * Helper method that waits for a query task to reach the expected stage
     */
    public QueryTask waitForQueryTask(URI uri, TaskState.TaskStage expectedStage) {

        // If the task's state ever reaches one of these "final" stages, we can stop waiting...
        List<TaskState.TaskStage> finalTaskStages = Arrays
                .asList(TaskState.TaskStage.CANCELLED, TaskState.TaskStage.FAILED,
                        TaskState.TaskStage.FINISHED, expectedStage);

        String error = String.format("Task did not reach expected state %s", expectedStage);
        Object[] r = new Object[1];
        final URI finalUri = uri;
        waitFor(error, () -> {
            QueryTask state = this.getServiceState(null, QueryTask.class, finalUri);
            r[0] = state;
            if (state.taskInfo != null) {
                if (finalTaskStages.contains(state.taskInfo.stage)) {
                    return true;
                }
            }
            return false;
        });
        return (QueryTask) r[0];
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskState.TaskStage} == {@code
     * TaskStage.FINISHED}.
     *
     * @param type    The class type that represent's the task's state
     * @param taskUri the URI of the task to wait for
     * @param <T>     the type that represent's the task's state
     * @return the state of the task once's it's {@code FINISHED}
     */
    public <T extends TaskService.TaskServiceState> T waitForFinishedTask(Class<T> type,
            String taskUri) {
        return waitForTask(type, taskUri, TaskState.TaskStage.FINISHED);
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskState.TaskStage} == {@code
     * TaskStage.FINISHED}.
     *
     * @param type    The class type that represent's the task's state
     * @param taskUri the URI of the task to wait for
     * @param <T>     the type that represent's the task's state
     * @return the state of the task once's it's {@code FINISHED}
     */
    public <T extends TaskService.TaskServiceState> T waitForFinishedTask(Class<T> type,
            URI taskUri) {
        return waitForTask(type, taskUri.toString(), TaskState.TaskStage.FINISHED);
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskState.TaskStage} == {@code
     * TaskStage.FAILED}.
     *
     * @param type    The class type that represent's the task's state
     * @param taskUri the URI of the task to wait for
     * @param <T>     the type that represent's the task's state
     * @return the state of the task once's it s {@code FAILED}
     */
    public <T extends TaskService.TaskServiceState> T waitForFailedTask(Class<T> type,
            String taskUri) {
        return waitForTask(type, taskUri, TaskState.TaskStage.FAILED);
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskState.TaskStage} == {@code
     * expectedStage}.
     *
     * @param type          The class type of that represents the task's state
     * @param taskUri       the URI of the task to wait for
     * @param expectedStage the stage we expect the task to eventually get to
     * @param <T>           the type that represents the task's state
     * @return the state of the task once it's {@link TaskState.TaskStage} == {@code expectedStage}
     */
    public <T extends TaskService.TaskServiceState> T waitForTask(Class<T> type, String taskUri,
            TaskState.TaskStage expectedStage) {
        return waitForTask(type, taskUri, expectedStage, false);
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskState.TaskStage} == {@code
     * expectedStage}.
     *
     * @param type          The class type of that represents the task's state
     * @param taskUri       the URI of the task to wait for
     * @param expectedStage the stage we expect the task to eventually get to
     * @param useQueryTask  Uses {@link QueryTask} to retrieve the current stage of the Task
     * @param <T>           the type that represents the task's state
     * @return the state of the task once it's {@link TaskState.TaskStage} == {@code expectedStage}
     */
    @SuppressWarnings("unchecked")
    public <T extends TaskService.TaskServiceState> T waitForTask(Class<T> type, String taskUri,
            TaskState.TaskStage expectedStage, boolean useQueryTask) {
        URI uri = UriUtils.buildUri(taskUri);

        if (!uri.isAbsolute()) {
            uri = UriUtils.buildUri(this, taskUri);
        }

        List<TaskState.TaskStage> finalTaskStages = Arrays
                .asList(TaskState.TaskStage.CANCELLED, TaskState.TaskStage.FAILED,
                        TaskState.TaskStage.FINISHED);

        String error = String.format("Task did not reach expected state %s", expectedStage);
        Object[] r = new Object[1];
        final URI finalUri = uri;
        waitFor(error, () -> {
            T state = (useQueryTask)
                    ? this.getServiceStateUsingQueryTask(type, taskUri)
                    : this.getServiceState(null, type, finalUri);

            r[0] = state;
            if (state.taskInfo != null) {
                if (expectedStage == state.taskInfo.stage) {
                    return true;
                }
                if (finalTaskStages.contains(state.taskInfo.stage)) {
                    fail(String.format(
                            "Task was expected to reach stage %s but reached a final stage %s",
                            expectedStage, state.taskInfo.stage));
                }
            }
            return false;
        });
        return (T) r[0];
    }

    @FunctionalInterface
    public interface WaitHandler {
        boolean isReady() throws Throwable;
    }

    public void waitFor(String timeoutMsg, WaitHandler wh) {
        ExceptionTestUtils.executeSafely(() -> {
            Date exp = getTestExpiration();
            while (new Date().before(exp)) {
                if (wh.isReady()) {
                    return;
                }
                // sleep for a tenth of the maintenance interval
                Thread.sleep(TimeUnit.MICROSECONDS.toMillis(getMaintenanceIntervalMicros()) / 10);
            }
            throw new TimeoutException(timeoutMsg);
        });
    }

    public void setSingleton(boolean enable) {
        this.isSingleton = enable;
    }

    /*
    * Running restart tests in VMs, in over provisioned CI will cause a restart using the same
    * index sand box to fail, due to a file system LockHeldException.
    * The sleep just reduces the false negative test failure rate, but it can still happen.
    * Not much else we can do other adding some weird polling on all the index files.
    *
    * Returns true of host restarted, false if retry attempts expired or other exceptions where thrown
     */
    public static boolean restartStatefulHost(ServiceHost host) throws Throwable {
        long exp = Utils.getNowMicrosUtc() + host.getOperationTimeoutMicros();

        do {
            Thread.sleep(2000);
            try {
                host.start();
                return true;
            } catch (Throwable e) {
                Logger.getAnonymousLogger().warning(String
                        .format("exception on host restart: %s", e.getMessage()));
                try {
                    host.stop();
                } catch (Throwable e1) {
                    return false;
                }
                if (e instanceof LockObtainFailedException) {
                    Logger.getAnonymousLogger()
                            .warning("Lock held exception on host restart, retrying");
                    continue;
                }
                return false;
            }
        } while (Utils.getNowMicrosUtc() < exp);
        return false;
    }

    public void waitForGC() {
        if (!isStressTest()) {
            return;
        }
        for (int k = 0; k < 10; k++) {
            Runtime.getRuntime().gc();
            Runtime.getRuntime().runFinalization();
        }
    }

    public TestRequestSender getTestRequestSender() {
        return this.sender;
    }
}
