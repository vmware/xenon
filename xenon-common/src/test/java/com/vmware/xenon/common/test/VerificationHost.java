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

import static javax.xml.bind.DatatypeConverter.printBase64Binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.OperationOption;
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

    public static class ProcessInfo {
        public Long parentPid;
        public String name;
        public Long pid;

        @Override
        public String toString() {
            return "ProcessInfo{" +
                    "this.parentPid=" + this.parentPid +
                    ", this.name='" + this.name + '\'' +
                    ", this.pid=" + this.pid +
                    '}';
        }
    }

    public static List<ProcessInfo> findUnixProcessInfoByName(String name) {
        return findUnixProcessInfo("-e", name);
    }

    public static ProcessInfo findUnixProcessInfoByPid(Long pid) {
        List<ProcessInfo> parent = findUnixProcessInfo(String.format("-p %d", pid), null);
        if (parent.size() != 1) {
            return null;
        }
        return parent.get(0);
    }

    private static List<ProcessInfo> findUnixProcessInfo(String filter, String name) {
        List<ProcessInfo> processes = new ArrayList<>();

        try {
            String line;
            String cmd = String.format("ps -o ppid,pid,ucomm %s", filter);
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), Utils.CHARSET));
            input.readLine(); // skip header
            while ((line = input.readLine()) != null) {
                String[] columns = line.trim().split("\\s+", 3);
                String ucomm = columns[2].trim();
                if (name != null && !ucomm.equalsIgnoreCase(name)) {
                    continue;
                }

                ProcessInfo info = new ProcessInfo();
                try {
                    info.parentPid = Long.parseLong(columns[0].trim());
                    info.pid = Long.parseLong(columns[1].trim());
                    info.name = ucomm;
                    processes.add(info);
                } catch (Throwable e) {
                    continue;
                }
            }
            input.close();
        } catch (Throwable err) {
            // ignore
        }

        return processes;
    }

    public static void killUnixProcess(Long pid) {
        try {
            Runtime.getRuntime().exec("kill " + pid);
        } catch (Throwable e) {

        }
    }

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
        return h;
    }

    public static void createAndAttachSSLClient(ServiceHost h) throws Throwable {
        // we create a random userAgent string to validate host to host communication when
        // the client appears to be from an external, non-Xenon source.
        ServiceClient client = NettyHttpServiceClient.create(UUID.randomUUID().toString(),
                h.getExecutor(),
                h.getScheduledExecutor(), h);

        SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
        clientContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
        client.setSSLContext(clientContext);
        h.setClient(client);

        SelfSignedCertificate ssc = new SelfSignedCertificate();
        h.setCertificateFileReference(ssc.certificate().toURI());
        h.setPrivateKeyFileReference(ssc.privateKey().toURI());
    }

    public void tearDown() {
        stop();
        this.getTemporaryFolder().delete();
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
        return this;
    }

    public int getTimeoutSeconds() {
        return this.timeoutSeconds;
    }

    public void send(Operation op) {
        op.setReferer(getReferer());
        super.sendRequest(op);
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

    public void testWait(TestContext ctx) throws Throwable {
        ctx.await();
    }

    public void testWait() throws Throwable {
        testWait(new Exception().getStackTrace()[1].getMethodName(),
                this.timeoutSeconds);
    }

    public void testWait(int timeoutSeconds) throws Throwable {
        testWait(new Exception().getStackTrace()[1].getMethodName(), timeoutSeconds);
    }

    public void testWait(String testName, int timeoutSeconds) throws Throwable {
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
        return;

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

    public URI createQueryTaskService(QueryTask create) throws Throwable {
        return createQueryTaskService(create, false);
    }

    public URI createQueryTaskService(QueryTask create, boolean forceRemote) throws Throwable {
        return createQueryTaskService(create, forceRemote, false, null, null);
    }

    public URI createQueryTaskService(QueryTask create, boolean forceRemote, String sourceLink)
            throws Throwable {
        return createQueryTaskService(create, forceRemote, false, null, sourceLink);
    }

    public URI createQueryTaskService(QueryTask create, boolean forceRemote, boolean isDirect,
            QueryTask taskResult,
            String sourceLink) throws Throwable {
        return createQueryTaskService(null, create, forceRemote, isDirect, taskResult, sourceLink);
    }

    public URI createQueryTaskService(URI factoryUri, QueryTask create, boolean forceRemote,
            boolean isDirect,
            QueryTask taskResult,
            String sourceLink) throws Throwable {

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
        TestContext ctx = testCreate(1);
        create.documentSelfLink = UUID.randomUUID().toString();
        create.documentSourceLink = sourceLink;
        create.taskInfo.isDirect = isDirect;
        Operation startPost = Operation.createPost(factoryUri).setBody(create)
                .setCompletion(ctx.getCompletion());

        if (forceRemote) {
            startPost.forceRemote();
        }

        if (isDirect) {
            startPost.setCompletion((o, e) -> {
                if (e != null) {
                    ctx.failIteration(e);
                    return;
                }

                QueryTask rsp = o.getBody(QueryTask.class);
                taskResult.results = rsp.results;
                taskResult.taskInfo.durationMicros = rsp.results.queryTimeMicros;
                ctx.completeIteration();
            });
        }

        send(startPost);
        ctx.await();
        return UriUtils.extendUri(factoryUri, create.documentSelfLink);
    }

    public QueryTask waitForQueryTaskCompletion(QuerySpecification q, int totalDocuments,
            int versionCount, URI u, boolean forceRemote, boolean deleteOnFinish) throws Throwable {
        return waitForQueryTaskCompletion(q, totalDocuments, versionCount, u, forceRemote,
                deleteOnFinish, true);
    }

    public QueryTask waitForQueryTaskCompletion(QuerySpecification q, int totalDocuments,
            int versionCount, URI u, boolean forceRemote, boolean deleteOnFinish,
            boolean throwOnFailure) throws Throwable {
        Date expiration = getTestExpiration();
        long startNanos = System.nanoTime();
        QueryTask latestTaskState = null;
        if (q.options == null) {
            q.options = EnumSet.noneOf(QueryOption.class);
        }

        do {
            EnumSet<TestProperty> props = EnumSet.noneOf(TestProperty.class);
            if (forceRemote) {
                props.add(TestProperty.FORCE_REMOTE);
            }

            latestTaskState = getServiceState(props, QueryTask.class, u);
            if (latestTaskState.taskInfo.stage == TaskState.TaskStage.FINISHED ||
                    latestTaskState.taskInfo.stage == TaskState.TaskStage.FAILED ||
                    latestTaskState.taskInfo.stage == TaskState.TaskStage.CANCELLED) {
                break;
            }

            Date now = new Date();
            if (now.after(expiration)) {
                throw new TimeoutException("Query did not complete in time");
            }
            Thread.sleep(100);
        } while (true);

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
            String fieldName, String fieldValue, long documentCount, long expectedResultCount)
            throws Throwable {
        return createAndWaitSimpleDirectQuery(this.getUri(), fieldName, fieldValue, documentCount,
                expectedResultCount);
    }

    public ServiceDocumentQueryResult createAndWaitSimpleDirectQuery(URI hostUri,
            String fieldName, String fieldValue, long documentCount, long expectedResultCount)
            throws Throwable {
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.query.setTermPropertyName(fieldName).setTermMatchValue(fieldValue);
        return createAndWaitSimpleDirectQuery(hostUri, q,
                documentCount, expectedResultCount);
    }

    public ServiceDocumentQueryResult createAndWaitSimpleDirectQuery(
            QueryTask.QuerySpecification spec,
            long documentCount, long expectedResultCount)
            throws Throwable {
        return createAndWaitSimpleDirectQuery(this.getUri(), spec,
                documentCount, expectedResultCount);
    }

    public ServiceDocumentQueryResult createAndWaitSimpleDirectQuery(URI hostUri,
            QueryTask.QuerySpecification spec, long documentCount, long expectedResultCount)
            throws Throwable {
        long start = Utils.getNowMicrosUtc();

        QueryTask task = null;
        Date exp = getTestExpiration();
        while (new Date().before(exp)) {
            task = QueryTask.create(spec).setDirect(true);
            createQueryTaskService(UriUtils.buildUri(hostUri, ServiceUriPaths.CORE_QUERY_TASKS),
                    task, false, true, task, null);
            if (task.results.documentLinks.size() == expectedResultCount) {
                break;
            }
            log("Expected %d, got %d, Query task: %s", expectedResultCount,
                    task.results.documentLinks.size(), task);
            Thread.sleep(1000);
        }

        assertTrue(String.format("Got %d links, expected %d", task.results.documentLinks.size(),
                expectedResultCount),
                task.results.documentLinks.size() == expectedResultCount);
        long end = Utils.getNowMicrosUtc();
        double delta = (end - start) / 1000000.0;
        double thpt = documentCount / delta;
        log("Document count: %d, Expected match count: %d, Documents / sec: %f",
                documentCount, expectedResultCount, thpt);
        return task.results;
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

    public String sendHttpRequest(ServiceClient client, String uri, String requestBody,
            int count) throws Throwable {

        Object[] rspBody = new Object[1];
        TestContext ctx = testCreate(count);
        Operation op = Operation.createGet(new URI(uri)).setCompletion(
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

    public Operation sendUIHttpRequest(String uri, String requestBody, int count) throws Throwable {

        final Operation[] result = new Operation[1];
        TestContext ctx = testCreate(count);
        Operation op = Operation.createGet(new URI(uri)).setCompletion(
                (o, e) -> {
                    if (e != null) {
                        ctx.failIteration(e);
                        return;
                    }
                    result[0] = o;
                    ctx.completeIteration();
                });

        for (int i = 0; i < count; i++) {
            send(op);
        }
        testWait(ctx);

        return result[0];
    }

    public <T> T getServiceState(EnumSet<TestProperty> props, Class<T> type, URI uri)
            throws Throwable {
        Map<URI, T> r = getServiceState(props, type, new URI[] { uri });
        return r.values().iterator().next();
    }

    public <T extends ServiceDocument> Map<URI, T> getServiceState(EnumSet<TestProperty> props,
            Class<T> type,
            Collection<URI> uris)
            throws Throwable {
        URI[] array = new URI[uris.size()];
        int i = 0;
        for (URI u : uris) {
            array[i++] = u;
        }
        return getServiceState(props, type, array);
    }

    /**
     * Retrieve in parallel, state from N services. This method will block execution until responses
     * are received or a failure occurs. It is not optimized for throughput measurements
     *
     * @param type
     * @param uris
     * @return
     * @throws Throwable
     */
    @SuppressWarnings("unchecked")
    public <T> Map<URI, T> getServiceState(EnumSet<TestProperty> props,
            Class<T> type,
            URI... uris) throws Throwable {

        if (type == null) {
            throw new IllegalArgumentException("type is required");
        }

        if (uris == null || uris.length == 0) {
            throw new IllegalArgumentException("uris are required");
        }

        Map<URI, T> results = new HashMap<>();
        TestContext ctx = testCreate(uris.length);
        Object[] state = new Object[1];

        for (URI u : uris) {
            Operation get = Operation
                    .createGet(u)
                    .setReferer(getReferer())
                    .setCompletion(
                            (o, e) -> {
                                try {
                                    if (e != null) {
                                        ctx.failIteration(e);
                                        return;
                                    }
                                    if (uris.length == 1) {
                                        state[0] = o.getBody(type);
                                    } else {
                                        synchronized (state) {
                                            ServiceDocument d = (ServiceDocument) o.getBody(type);
                                            results.put(
                                                    UriUtils.buildUri(o.getUri(),
                                                            d.documentSelfLink),
                                                    o.getBody(type));
                                        }
                                    }
                                    ctx.completeIteration();
                                } catch (Throwable ex) {
                                    log("Exception parsing state for %s: %s", o.getUri(),
                                            ex.toString());
                                    ctx.failIteration(ex);
                                }
                            });
            if (props != null && props.contains(TestProperty.FORCE_REMOTE)) {
                get.forceRemote();
            }
            if (props != null && props.contains(TestProperty.HTTP2)) {
                get.setConnectionSharing(true);
            }

            if (props != null && props.contains(TestProperty.DISABLE_CONTEXT_ID_VALIDATION)) {
                get.setContextId(TestProperty.DISABLE_CONTEXT_ID_VALIDATION.toString());
            }

            send(get);
        }

        testWait(ctx);
        if (uris.length == 1) {
            results.put(uris[0], (T) state[0]);
        }

        return results;
    }

    /**
     * Retrieve in parallel, state from N services. This method will block execution until responses
     * are received or a failure occurs. It is not optimized for throughput measurements
     */
    public <T> Map<URI, T> getServiceState(EnumSet<TestProperty> props, Class<T> type,
            List<Service> services) throws Throwable {
        URI[] uris = new URI[services.size()];
        int i = 0;
        for (Service s : services) {
            uris[i++] = s.getUri();
        }
        return this.getServiceState(props, type, uris);
    }

    public ServiceDocumentQueryResult getFactoryState(URI factoryUri) throws Throwable {
        return this.getServiceState(null, ServiceDocumentQueryResult.class, factoryUri);
    }

    public ServiceDocumentQueryResult getExpandedFactoryState(URI factoryUri) throws Throwable {
        factoryUri = UriUtils.buildExpandLinksQueryUri(factoryUri);
        return this.getServiceState(null, ServiceDocumentQueryResult.class, factoryUri);
    }

    public void doExampleServiceUpdateAndQueryByVersion(URI hostUri, int serviceCount)
            throws Throwable {
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

    private void doServiceUpdates(Collection<URI> serviceUris, Action action, ServiceDocument body)
            throws Throwable {
        TestContext ctx = testCreate(serviceUris.size());
        for (URI u : serviceUris) {
            Operation update = Operation.createPost(u)
                    .setAction(action)
                    .setBody(body)
                    .setCompletion(ctx.getCompletion());
            send(update);
        }
        testWait(ctx);
    }

    private void queryDocumentIndexByVersionAndVerify(URI hostUri, String selfLink,
            Action expectedAction,
            Long version,
            Long latestVersion)
            throws Throwable {

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

        TestContext ctx = testCreate(1);
        Operation remoteGet = Operation
                .createGet(localQueryUri)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ctx.failIteration(
                                new IllegalStateException("Could not query document-index"));
                        return;
                    }

                    if (latestVersion == null) {
                        if (o.hasBody()) {
                            ctx.failIteration(new IllegalStateException(
                                    "Document not expected"));
                            return;
                        }
                        ctx.completeIteration();
                        return;
                    }

                    ServiceDocument result = o.getBody(ServiceDocument.class);
                    Long expectedVersion = version;

                    if (version == null) {
                        expectedVersion = latestVersion;
                    }

                    if (result.documentVersion != expectedVersion.intValue()) {
                        ctx.failIteration(new IllegalStateException(
                                "Invalid document version returned"));
                        return;
                    }

                    if (!expectedAction.name().equals(result.documentUpdateAction)) {
                        ctx.failIteration(new IllegalStateException(
                                "Invalid document update action returned:"
                                        + result.documentUpdateAction));
                        return;
                    }

                    ctx.completeIteration();
                });

        send(remoteGet);
        testWait(ctx);
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
        log("starting %s test with properties %s, service caps: %s",
                action, properties.toString(), services.get(0).getOptions());

        Map<URI, MinimalTestServiceState> statesBeforeUpdate = getServiceState(properties,
                MinimalTestServiceState.class, services);

        StackTraceElement[] e = new Exception().getStackTrace();
        String testName = e[1].getMethodName() + ":" + e[0].getMethodName();

        // create a template PUT. Each operation instance is cloned on send, so
        // we can re-use across services
        Operation updateOp = Operation.createPut(null).setCompletion(
                getCompletion());

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
                    failIteration(eb);
                    return;
                }

                if (!Operation.MEDIA_TYPE_APPLICATION_OCTET_STREAM.equals(o.getContentType())) {
                    failIteration(new IllegalArgumentException("unexpected content type: "
                            + o.getContentType()));
                    return;
                }

                completeIteration();
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
                        failIteration(new IllegalStateException("expected failure"));
                    } else {
                        completeIteration();
                    }
                });
            } else {
                updateOp.setCompletion((o, ex) -> {
                    if (ex == null) {
                        failIteration(new IllegalStateException("failure expected"));
                        return;
                    }

                    MinimalTestServiceErrorResponse rsp = o
                            .getBody(MinimalTestServiceErrorResponse.class);
                    if (!MinimalTestServiceErrorResponse.KIND.equals(rsp.documentKind)) {
                        failIteration(new IllegalStateException("Response not expected:"
                                + Utils.toJson(rsp)));
                        return;
                    }
                    completeIteration();
                });
            }
        }

        int byteCount = Utils.toJson(body).getBytes(Utils.CHARSET).length;
        if (properties.contains(TestProperty.BINARY_SERIALIZATION)) {
            byte[] buffer = new byte[4096];
            long c = Utils.toDocumentBytes(body, buffer, 0);
            byteCount = (int) c;
        }
        log("Bytes per payload %s", byteCount);

        long startTimeMicros = System.nanoTime() / 1000;
        testStart(testName, properties, count * services.size());

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
                            this.completeIteration();
                            l[0].countDown();
                            return;
                        }
                        this.failIteration(ex);
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
        testWait();

        if (isFailureExpected) {
            this.toggleNegativeTestMode(false);
            return;
        }

        long endTimeMicros = System.nanoTime() / 1000;
        double deltaSeconds = (endTimeMicros - startTimeMicros) / 1000000.0;
        double ioCount = count * services.size();
        double throughput = ioCount / deltaSeconds;
        log("Operation count: %f, throughput(ops/sec): %f", ioCount, throughput);

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
            ServiceDocument beforeSt = statesBeforeUpdate
                    .get(UriUtils.buildUri(this, st.documentSelfLink));

            if (st.documentVersion != beforeSt.documentVersion + count) {
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

    public void waitForServiceAvailable(String... links) throws Throwable {
        for (String link : links) {
            TestContext ctx = testCreate(1);
            this.registerForServiceAvailability(ctx.getCompletion(), link);
            ctx.await();
        }
    }

    public void waitForReplicatedFactoryServiceAvailable(URI u) throws Throwable {
        waitForReplicatedFactoryServiceAvailable(u, ServiceUriPaths.DEFAULT_NODE_SELECTOR);
    }

    public void waitForReplicatedFactoryServiceAvailable(URI u, String nodeSelectorPath)
            throws Throwable {
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

    public void waitForServiceAvailable(URI u) throws Throwable {
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

    public <T> Map<URI, T> doFactoryChildServiceStart(
            EnumSet<TestProperty> props,
            long c,
            Class<T> bodyType,
            Consumer<Operation> setInitialStateConsumer,
            URI factoryURI) throws Throwable {
        Map<URI, T> initialStates = new HashMap<>();
        if (props == null) {
            props = EnumSet.noneOf(TestProperty.class);
        }

        log("Sending %d POST requests to %s", c, factoryURI);

        TestContext ctx = testCreate((int) c);
        for (int i = 0; i < c; i++) {
            Operation createPost = Operation.createPost(factoryURI);
            // call callback to set the body
            setInitialStateConsumer.accept(createPost);

            // create a start service POST with an initial state
            createPost.setCompletion(
                    (o, e) -> {
                        if (e != null) {
                            ctx.failIteration(e);
                            return;
                        }
                        try {
                            T body = o.getBody(bodyType);
                            ServiceDocument rsp = (ServiceDocument) body;
                            URI childURI = UriUtils.buildUri(factoryURI, rsp.documentSelfLink);
                            synchronized (initialStates) {
                                initialStates.put(childURI, body);
                            }
                            ctx.completeIteration();
                        } catch (Throwable e1) {
                            ctx.failIteration(e1);
                        }
                    });
            if (props.contains(TestProperty.FORCE_REMOTE)) {
                createPost.forceRemote();
            }
            send(createPost);
        }

        ctx.await();
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
        TestContext ctx = testCreate(services.size());
        for (Service s : services) {
            Operation delete = Operation.createDelete(s.getUri())
                    .setCompletion(ctx.getCompletion());
            // delete with no body means stop the service
            send(delete);
        }

        ctx.await();

        // restart services
        ctx = testCreate(services.size());
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

    public Map<URI, URI> getNodeGroupMap() {
        return this.peerNodeGroups;
    }

    public Map<String, NodeState> getNodeStateMap() {
        return this.peerHostIdToNodeState;
    }

    public void scheduleSynchronizationIfAutoSyncDisabled(String selectorPath) throws Throwable {
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

    public void setUpPeerHosts(int localHostCount) throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        if (this.peerNodes == null) {
            this.setUpLocalPeersHosts(localHostCount, null);
        } else {
            this.setUpWithRemotePeers(this.peerNodes);
        }
    }

    public void setUpLocalPeersHosts(int localHostCount, Long maintIntervalMillis)
            throws Throwable {
        testStart(localHostCount);
        if (maintIntervalMillis == null) {
            maintIntervalMillis = this.maintenanceIntervalMillis;
        }
        final long intervalMicros = TimeUnit.MILLISECONDS.toMicros(maintIntervalMillis);
        for (int i = 0; i < localHostCount; i++) {
            run(() -> {
                try {
                    this.setUpLocalPeerHost(null, intervalMicros);
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

    public void setUpWithRemotePeers(String[] peerNodes) throws URISyntaxException {
        this.isRemotePeerTest = true;

        this.peerNodeGroups.clear();
        for (String remoteNode : peerNodes) {
            URI remoteHostBaseURI = new URI(remoteNode);
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
        if (!this.peerNodeGroups.isEmpty()) {
            List<URI> peerNodeGroupList = new ArrayList<URI>(this.peerNodeGroups.keySet());
            return getUriFromList(null, peerNodeGroupList);
        }
        return null;
    }

    public URI getPeerNodeGroupUri() {
        URI hostUri = getPeerHostUri();
        if (hostUri != null) {
            return this.peerNodeGroups.get(hostUri);
        }
        return null;
    }

    public VerificationHost getPeerHost() {
        URI hostUri = getPeerServiceUri(null);
        if (hostUri != null) {
            return this.localPeerHosts.get(hostUri);
        }
        return null;
    }

    public URI getPeerServiceUri(String link) {
        if (!this.localPeerHosts.isEmpty()) {
            List<URI> localPeerList = new ArrayList<URI>(this.localPeerHosts.keySet());
            return getUriFromList(link, localPeerList);
        } else {
            List<URI> peerList = new ArrayList<URI>(this.peerNodeGroups.keySet());
            return getUriFromList(link, peerList);
        }
    }

    private URI getUriFromList(String link, List<URI> uriList) {
        if (!uriList.isEmpty()) {
            Collections.shuffle(uriList, new Random(System.nanoTime()));
            URI baseUri = uriList.iterator().next();
            return UriUtils.extendUri(baseUri, link);
        }
        return null;
    }

    public void createCustomNodeGroupOnPeers(String customGroupName) throws Throwable {
        createCustomNodeGroupOnPeers(customGroupName, null);
    }

    public void createCustomNodeGroupOnPeers(String customGroupName, Map<URI, NodeState> selfState)
            throws Throwable {
        if (selfState == null) {
            selfState = new HashMap<>();
        }
        // create a custom node group on all peer nodes
        testStart(getNodeGroupMap().size());
        for (URI peerHostBaseUri : getNodeGroupMap().keySet()) {
            URI nodeGroupFactoryUri = UriUtils.buildUri(peerHostBaseUri,
                    ServiceUriPaths.NODE_GROUP_FACTORY);
            createCustomNodeGroup(customGroupName, nodeGroupFactoryUri,
                    selfState.get(peerHostBaseUri));
        }
        testWait();
    }

    private void createCustomNodeGroup(String customGroupName, URI nodeGroupFactoryUri,
            NodeState selfState) {
        NodeGroupState body = new NodeGroupState();
        body.documentSelfLink = customGroupName;
        if (selfState != null) {
            body.nodes.put(selfState.id, selfState);
        }
        Operation postNodeGroup = Operation.createPost(nodeGroupFactoryUri)
                .setBody(body)
                .setCompletion(getCompletion());
        send(postNodeGroup);
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
            CompletionHandler convergedCompletion) throws Throwable {

        testStart(1);
        Operation subscribeToNodeGroup = Operation.createPost(
                UriUtils.buildSubscriptionUri(nodeGroup))
                .setCompletion(getCompletion())
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
        testWait();
    }

    public void waitForNodeGroupIsAvailableConvergence() throws Throwable {
        waitForNodeGroupIsAvailableConvergence(ServiceUriPaths.DEFAULT_NODE_GROUP);
    }

    public void waitForNodeGroupIsAvailableConvergence(String nodeGroupPath) throws Throwable {
        waitForNodeGroupIsAvailableConvergence(nodeGroupPath, this.peerNodeGroups.values());
    }

    public void waitForNodeGroupIsAvailableConvergence(String nodeGroupPath,
            Collection<URI> nodeGroupUris) throws Throwable {
        if (nodeGroupPath == null) {
            nodeGroupPath = ServiceUriPaths.DEFAULT_NODE_GROUP;
        }
        Date expiration = getTestExpiration();
        while (new Date().before(expiration)) {
            boolean isConverged = true;
            for (URI nodeGroupUri : nodeGroupUris) {
                URI u = UriUtils.buildUri(nodeGroupUri, nodeGroupPath);
                URI statsUri = UriUtils.buildStatsUri(u);
                ServiceStats stats = getServiceState(null, ServiceStats.class, statsUri);
                ServiceStat availableStat = stats.entries.get(Service.STAT_NAME_AVAILABLE);
                if (availableStat == null || availableStat.latestValue != Service.STAT_VALUE_TRUE) {
                    log("Service stat available is missing or not 1.0");
                    isConverged = false;
                    break;
                }
            }

            if (!isConverged) {
                Thread.sleep(getMaintenanceIntervalMicros() / 1000);
                continue;
            }
            break;
        }

        if (new Date().after(expiration)) {
            throw new TimeoutException();
        }
    }

    public void waitForNodeGroupConvergence(int memberCount)
            throws Throwable {
        waitForNodeGroupConvergence(memberCount, null);
    }

    public void waitForNodeGroupConvergence(int healthyMemberCount, Integer totalMemberCount)
            throws Throwable {
        waitForNodeGroupConvergence(this.peerNodeGroups.values(), healthyMemberCount,
                totalMemberCount, true);
    }

    public void waitForNodeGroupConvergence(Collection<URI> nodeGroupUris, int healthyMemberCount,
            Integer totalMemberCount,
            boolean waitForTimeSync)
            throws Throwable {
        waitForNodeGroupConvergence(nodeGroupUris, healthyMemberCount, totalMemberCount,
                new HashMap<>(), waitForTimeSync);
    }

    public void waitForNodeGroupConvergence(Collection<URI> nodeGroupUris, int healthyMemberCount,
            Integer totalMemberCount,
            Map<URI, EnumSet<NodeOption>> expectedOptionsPerNodeGroupUri,
            boolean waitForTimeSync)
            throws Throwable {

        if (expectedOptionsPerNodeGroupUri == null) {
            expectedOptionsPerNodeGroupUri = new HashMap<>();
        }

        final int sleepTimeMillis = FAST_MAINT_INTERVAL_MILLIS * 2;
        Date now = null;
        Date expiration = getTestExpiration();
        assertTrue(!nodeGroupUris.isEmpty());
        Map<URI, NodeGroupState> nodesPerHost = new HashMap<>();
        Set<Long> updateTime = new HashSet<>();
        do {
            nodesPerHost.clear();
            updateTime.clear();
            TestContext ctx = testCreate(nodeGroupUris.size());
            for (URI nodeGroup : nodeGroupUris) {
                getNodeState(nodeGroup, nodesPerHost, ctx);
                EnumSet<NodeOption> expectedOptions = expectedOptionsPerNodeGroupUri.get(nodeGroup);
                if (expectedOptions == null) {
                    expectedOptionsPerNodeGroupUri.put(nodeGroup, NodeState.DEFAULT_OPTIONS);
                }
            }
            testWait(ctx);

            boolean isConverged = true;
            for (Entry<URI, NodeGroupState> entry : nodesPerHost
                    .entrySet()) {

                NodeGroupState nodeGroupState = entry.getValue();
                updateTime.add(nodeGroupState.membershipUpdateTimeMicros);
                int healthyNodeCount = calculateHealthyNodeCount(nodeGroupState);

                if (totalMemberCount != null
                        && nodeGroupState.nodes.size() != totalMemberCount.intValue()) {
                    log("Host %s is reporting %d healthy members %d total, expected %d total",
                            entry.getKey(), healthyNodeCount, nodesPerHost.size(),
                            healthyMemberCount, totalMemberCount);
                    isConverged = false;
                    break;
                }

                if (healthyNodeCount != healthyMemberCount) {
                    log("Host %s is reporting %d healthy members, expected %d",
                            entry.getKey(), healthyNodeCount, healthyMemberCount);
                    isConverged = false;
                    break;
                }

                validateNodes(entry.getValue(), healthyMemberCount, expectedOptionsPerNodeGroupUri);
            }

            now = new Date();

            if (waitForTimeSync && updateTime.size() != 1) {
                log("Update times did not converge: %s", updateTime.toString());
                isConverged = false;
            }

            if (isConverged) {
                break;
            }

            Thread.sleep(sleepTimeMillis);
        } while (now.before(expiration));

        boolean log = true;
        updateTime.clear();
        for (Entry<URI, NodeGroupState> entry : nodesPerHost
                .entrySet()) {
            updateTime.add(entry.getValue().membershipUpdateTimeMicros);
            for (NodeState n : entry.getValue().nodes.values()) {
                if (log) {
                    log("%s:%s %s, (time) %d, (version) %d", n.groupReference, n.id, n.status,
                            n.documentUpdateTimeMicros, n.documentVersion);
                    log = false;
                }
                if (n.status == NodeStatus.AVAILABLE) {
                    this.peerHostIdToNodeState.put(n.id, n);
                }
            }
        }

        try {
            if (waitForTimeSync && updateTime.size() != 1) {
                throw new IllegalStateException("Update time did not converge");
            }

            if (now.after(expiration)) {
                throw new TimeoutException();
            }
        } catch (Throwable e) {
            for (Entry<URI, NodeGroupState> entry : nodesPerHost
                    .entrySet()) {
                log("%s reports %s", entry.getKey(), Utils.toJsonHtml(entry.getValue()));
            }
            throw e;
        }

        if (!waitForTimeSync) {
            return;
        }

        // additional check using convergence utility
        Date exp = getTestExpiration();
        while (new Date().before(exp)) {
            boolean[] isConverged = new boolean[1];
            NodeGroupState ngs = nodesPerHost.values().iterator().next();

            testStart(1);
            Operation op = Operation.createPost(null)
                    .setReferer(getReferer())
                    .setExpiration(Utils.getNowMicrosUtc() + getOperationTimeoutMicros());
            NodeGroupUtils.checkConvergenceFromAnyHost(this, ngs, op.setCompletion((o, e) -> {
                if (e != null && waitForTimeSync) {
                    log(Level.INFO, "Convergence failure, will retry: %s", e.getMessage());
                    isConverged[0] = false;
                } else {
                    isConverged[0] = true;
                }
                completeIteration();
            }));
            testWait();
            if (!isConverged[0]) {
                Thread.sleep(sleepTimeMillis);
                continue;
            }
            break;
        }

        if (new Date().after(exp)) {
            throw new TimeoutException();
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

    public void doNodeGroupStatsVerification(Map<URI, URI> defaultNodeGroupsPerHost)
            throws Throwable {
        TestContext ctx = testCreate(defaultNodeGroupsPerHost.size());
        for (URI nodeGroup : defaultNodeGroupsPerHost.values()) {
            Operation get = Operation.createGet(UriUtils.extendUri(nodeGroup,
                    ServiceHost.SERVICE_URI_SUFFIX_STATS));
            get.setCompletion((o, e) -> {
                if (e != null) {
                    ctx.failIteration(e);
                    return;
                }
                try {
                    ServiceStats stats = o.getBody(ServiceStats.class);
                    assertTrue(!stats.entries.isEmpty());
                    ctx.completeIteration();
                } catch (Throwable ex) {
                    ctx.failIteration(ex);
                }
            });
            send(get);
        }
        ctx.await();
    }

    public void setNodeGroupConfig(NodeGroupConfig config)
            throws Throwable {
        setSystemAuthorizationContext();
        TestContext ctx = testCreate(getNodeGroupMap().size());
        for (URI nodeGroup : getNodeGroupMap().values()) {
            NodeGroupState body = new NodeGroupState();
            body.config = config;
            body.nodes = null;
            send(Operation.createPatch(nodeGroup)
                    .setCompletion(ctx.getCompletion())
                    .setBody(body));
        }
        resetAuthorizationContext();
        ctx.await();
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

    public void waitForNodeSelectorQuorumConvergence(String nodeSelectorPath, int quorum)
            throws Throwable {
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

    public void setNodeGroupQuorum(Integer quorum, URI nodeGroup)
            throws Throwable {
        TestContext ctx = testCreate(1);
        UpdateQuorumRequest body = UpdateQuorumRequest.create(true);

        if (quorum != null) {
            body.setMembershipQuorum(quorum);
        }

        send(Operation.createPatch(nodeGroup)
                .setCompletion(ctx.getCompletion())
                .setBody(body));
        ctx.await();
    }

    public <T extends ServiceDocument> void validateDocumentPartitioning(
            Map<URI, T> provisioningTasks,
            Class<T> type) throws Throwable {
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
            Long expiration)
            throws Throwable {
        waitForServiceAvailable(ExampleService.FACTORY_LINK);
        TestContext ctx = testCreate(serviceCount);
        ExampleServiceState initialState = new ExampleServiceState();
        URI exampleFactoryUri = UriUtils.buildFactoryUri(h,
                ExampleService.class);

        // create example services
        for (int i = 0; i < serviceCount; i++) {
            initialState.counter = 123L;
            if (expiration != null) {
                initialState.documentExpirationTimeMicros = expiration;
            }
            initialState.name = initialState.documentSelfLink = UUID.randomUUID().toString();
            Operation createPost = Operation
                    .createPost(exampleFactoryUri)
                    .setBody(initialState).setCompletion(ctx.getCompletion());
            send(createPost);
            exampleURIs.add(UriUtils.extendUri(exampleFactoryUri, initialState.documentSelfLink));
        }

        ctx.await();
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

    public void toggleServiceOptions(URI serviceUri, EnumSet<ServiceOption> optionsToEnable,
            EnumSet<ServiceOption> optionsToDisable) throws Throwable {

        ServiceConfigUpdateRequest updateBody = ServiceConfigUpdateRequest.create();
        updateBody.removeOptions = optionsToDisable;
        updateBody.addOptions = optionsToEnable;

        TestContext ctx = testCreate(1);
        URI configUri = UriUtils.buildConfigUri(serviceUri);
        send(Operation.createPatch(configUri).setBody(updateBody)
                .setCompletion(ctx.getCompletion()));
        testWait(ctx);
    }

    public void setOperationQueueLimit(URI serviceUri, int limit) throws Throwable {
        // send a set limit configuration request
        ServiceConfigUpdateRequest body = ServiceConfigUpdateRequest.create();
        body.operationQueueLimit = limit;
        URI configUri = UriUtils.buildConfigUri(serviceUri);
        TestContext ctx = testCreate(1);
        send(Operation.createPatch(configUri).setBody(body)
                .setCompletion(ctx.getCompletion()));
        testWait(ctx);

        // verify new operation limit is set
        TestContext ctxFinal = testCreate(1);
        send(Operation.createGet(configUri).setCompletion((o, e) -> {
            if (e != null) {
                ctxFinal.failIteration(e);
                return;
            }
            ServiceConfiguration cfg = o.getBody(ServiceConfiguration.class);
            if (cfg.operationQueueLimit != body.operationQueueLimit) {
                ctxFinal.failIteration(new IllegalStateException("Invalid queue limit"));
                return;
            }

            ctxFinal.completeIteration();
        }));
        testWait(ctxFinal);
    }

    public void toggleNegativeTestMode(boolean enable) {
        log("++++++ Negative test mode %s, failure logs expected: %s", enable, enable);
    }

    public void logNodeProcessLogs(Set<URI> keySet, String logSuffix) throws Throwable {
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

    public void logNodeManagementState(Set<URI> keySet) throws Throwable {
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

    public Map<URI, VerificationHost> getInProcessHostMap() {
        return this.localPeerHosts;
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

    public void logServiceStats(URI uri) throws Throwable {
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
    public void logNodeGroupState() throws Throwable {
        TestContext ctx = testCreate(getNodeGroupMap().size());
        for (URI nodeGroup : getNodeGroupMap().values()) {
            send(Operation.createGet(nodeGroup).setCompletion((o, e) -> {
                if (e != null) {
                    ctx.failIteration(e);
                    return;
                }
                NodeGroupState ngs = o.getBody(NodeGroupState.class);
                log("%s", Utils.toJsonHtml(ngs));
                ctx.completeIteration();
            }));
        }
        testWait(ctx);
    }

    public void setServiceMaintenanceIntervalMicros(String path, long micros) throws Throwable {
        setServiceMaintenanceIntervalMicros(UriUtils.buildUri(this, path), micros);
    }

    public void setServiceMaintenanceIntervalMicros(URI u, long micros) throws Throwable {
        ServiceConfigUpdateRequest updateBody = ServiceConfigUpdateRequest.create();
        updateBody.maintenanceIntervalMicros = micros;
        TestContext ctx = testCreate(1);
        URI configUri = UriUtils.extendUri(u, ServiceHost.SERVICE_URI_SUFFIX_CONFIG);
        send(Operation.createPatch(configUri).setBody(updateBody)
                .setCompletion(ctx.getCompletion()));

        testWait(ctx);
    }

    /**
     * Toggles the operation tracing service
     *
     * @param baseHostURI  the uri of the tracing service
     * @param enable state to toggle to
     */
    public void toggleOperationTracing(URI baseHostURI, boolean enable) throws Throwable {
        ServiceHostManagementService.ConfigureOperationTracingRequest r = new ServiceHostManagementService.ConfigureOperationTracingRequest();
        r.enable = enable ? ServiceHostManagementService.OperationTracingEnable.START
                : ServiceHostManagementService.OperationTracingEnable.STOP;
        r.kind = ServiceHostManagementService.ConfigureOperationTracingRequest.KIND;

        this.setSystemAuthorizationContext();
        TestContext ctx = testCreate(1);
        this.send(Operation
                .createPatch(
                        UriUtils.extendUri(baseHostURI, ServiceHostManagementService.SELF_LINK))
                .setBody(r)
                .setCompletion(ctx.getCompletion()));
        testWait(ctx);
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
            ServiceConfigUpdateRequest cfgBody) throws Throwable {

        TestContext ctx = testCreate(selfLinks.size());
        for (String link : selfLinks) {
            URI bUri = UriUtils.buildUri(getUri(), link,
                    ServiceHost.SERVICE_URI_SUFFIX_CONFIG);

            send(Operation.createPatch(bUri)
                    .setBody(cfgBody)
                    .setCompletion(ctx.getCompletion()));

        }
        testWait(ctx);
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

    public void logAllDocuments(Set<URI> baseHostUris) throws Throwable {
        QueryTask task = new QueryTask();
        task.setDirect(true);
        task.querySpec = new QuerySpecification();
        task.querySpec.query.setTermPropertyName("documentSelfLink").setTermMatchValue("*");
        task.querySpec.query.setTermMatchType(MatchType.WILDCARD);
        task.querySpec.options = EnumSet.of(QueryOption.EXPAND_CONTENT);

        TestContext ctx = testCreate(baseHostUris.size());
        for (URI baseHost : baseHostUris) {
            Operation queryPost = Operation
                    .createPost(UriUtils.buildUri(baseHost, ServiceUriPaths.CORE_QUERY_TASKS))
                    .setBody(task)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.failIteration(e);
                            return;
                        }
                        QueryTask t = o.getBody(QueryTask.class);
                        log(Utils.toJsonHtml(t));
                        ctx.completeIteration();
                    });
            this.send(queryPost);
        }
        testWait(ctx);
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
    public AuthorizationContext assumeIdentity(String userServicePath) throws GeneralSecurityException {
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

    public void deleteAllChildServices(URI factoryURI) throws Throwable {
        ServiceDocumentQueryResult res = getFactoryState(factoryURI);
        if (res.documentLinks.isEmpty()) {
            return;
        }
        TestContext ctx = testCreate(res.documentLinks.size());
        for (String link : res.documentLinks) {
            send(Operation.createDelete(UriUtils.buildUri(this, link))
                    .setCompletion(ctx.getCompletion()));
        }
        testWait(ctx);
    }

    public <T extends ServiceDocument> ServiceDocument verifyPost(Class<T> documentType,
            String factoryLink,
            T state,
            int expectedStatusCode) throws Throwable {
        final ServiceDocument[] outState = new ServiceDocument[1];
        URI uri = UriUtils.buildUri(this, factoryLink);

        TestContext ctx = testCreate(1);
        Operation op = Operation.createPost(uri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failIteration(e);
                        return;
                    }
                    if (o.getStatusCode() == expectedStatusCode) {
                        outState[0] = o.getBody(documentType);
                        ctx.completeIteration();
                        return;
                    }
                    ctx.failIteration(new IllegalStateException(
                            String.format("Status code expected: %s, actual: %s",
                                    expectedStatusCode, o.getStatusCode())));
                });

        send(op);
        testWait(ctx);

        return outState[0];
    }

    protected TemporaryFolder getTemporaryFolder() {
        return this.temporaryFolder;
    }

    public void setTemporaryFolder(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    /**
     * Sends an operation and waits for completion, using default completion handler
     */
    public void sendAndWaitExpectSuccess(Operation op) throws Throwable {
        TestContext ctx = testCreate(1);
        send(op.setCompletion(ctx.getCompletion()));
        testWait(ctx);
    }

    public void sendAndWaitExpectFailure(Operation op) throws Throwable {
        sendAndWaitExpectFailure(op, null);
    }

    public void sendAndWaitExpectFailure(Operation op, Integer expectedFailureCode)
            throws Throwable {
        TestContext ctx = testCreate(1);
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (expectedFailureCode != null) {
                    if (!expectedFailureCode.equals(o.getStatusCode())) {
                        ctx.failIteration(new IllegalStateException(
                                "got unexpected status: " + expectedFailureCode));
                        return;
                    }
                }
                ctx.completeIteration();
            } else {
                ctx.failIteration(new IllegalStateException("got success, expected failure"));
            }
        };
        send(op.setCompletion(c));
        testWait(ctx);
    }

    /**
     * Sends an operation and waits for completion. Completion handler must be set on operation
     */
    public void sendAndWait(Operation op) throws Throwable {
        // assume completion is attached, using our getCompletion() or
        // getExpectedFailureCompletion()
        testStart(1);
        send(op);
        testWait();
    }

    /**
     * Sends an operation, waits for completion and return the response representation.
     */
    public Operation waitForResponse(Operation op) throws Throwable {
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
     * @see com.vmware.xenon.services.common.TestExampleTaskService#testExampleTestServices()
     */
    public <T extends ServiceDocument> void sendFactoryPost(Class<? extends Service> service,
            T state, CompletionHandler handler) throws Throwable {
        URI factoryURI = UriUtils.buildFactoryUri(this, service);
        log(Level.INFO, "Creating POST for [uri=%s] [body=%s]", factoryURI, state);
        Operation createPost = Operation.createPost(factoryURI)
                .setBody(state)
                .setCompletion(handler);

        testStart(1);
        send(createPost);
        testWait();
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
     * Helper method that waits for {@code taskUri} to have a {@link TaskState.TaskStage} == {@code
     * TaskStage.FINISHED}.
     *
     * @param type    The class type that represent's the task's state
     * @param taskUri the URI of the task to wait for
     * @param <T>     the type that represent's the task's state
     * @return the state of the task once's it's {@code FINISHED}
     */
    public <T extends TaskService.TaskServiceState> T waitForFinishedTask(Class<T> type,
            String taskUri)
            throws Throwable {
        return waitForTask(type, taskUri, TaskState.TaskStage.FINISHED);
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
            String taskUri)
            throws Throwable {
        return waitForTask(type, taskUri, TaskState.TaskStage.FAILED);
    }

    /**
     * Helper method that waits for {@code taskUri} to have a {@link TaskState.TaskStage} == {@code
     * expectedStage}.
     *
     * @param type          The class type of that represent's the task's state
     * @param taskUri       the URI of the task to wait for
     * @param expectedStage the stage we expect the task to eventually get to
     * @param <T>           the type that represent's the task's state
     * @return the state of the task once it's {@link TaskState.TaskStage} == {@code expectedStage}
     */
    public <T extends TaskService.TaskServiceState> T waitForTask(Class<T> type, String taskUri,
            TaskState.TaskStage expectedStage) throws Throwable {
        URI uri = UriUtils.buildUri(this, taskUri);

        // If the task's state ever reaches one of these "final" stages, we can stop waiting...
        List<TaskState.TaskStage> finalTaskStages = Arrays
                .asList(TaskState.TaskStage.CANCELLED, TaskState.TaskStage.FAILED,
                        TaskState.TaskStage.FINISHED, expectedStage);

        T state = null;
        for (int i = 0; i < 20; i++) {
            state = this.getServiceState(null, type, uri);
            if (state.taskInfo != null) {
                if (finalTaskStages.contains(state.taskInfo.stage)) {
                    break;
                }
            }
            Thread.sleep(250);
        }
        assertEquals("Task did not reach expected state", state.taskInfo.stage, expectedStage);
        return state;
    }

    @FunctionalInterface
    public interface WaitHandler {
        boolean isReady() throws Throwable;
    }

    public void waitFor(String timeoutMsg, WaitHandler wh) throws Throwable {
        Date exp = getTestExpiration();
        while (new Date().before(exp)) {
            if (wh.isReady()) {
                return;
            }
            Thread.sleep(getMaintenanceIntervalMicros() / 1000);
        }
        throw new TimeoutException(timeoutMsg);
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
}
