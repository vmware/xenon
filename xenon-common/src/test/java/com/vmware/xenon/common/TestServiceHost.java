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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost.ServiceAlreadyStartedException;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.jwt.Rfc7519Claims;
import com.vmware.xenon.common.jwt.Signer;
import com.vmware.xenon.common.jwt.Verifier;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.AuthorizationContextService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.ExampleServiceHost;
import com.vmware.xenon.services.common.LuceneDocumentIndexService;
import com.vmware.xenon.services.common.MinimalFactoryTestService;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestServiceHost {

    private static final int MAINTENANCE_INTERVAL_MILLIS = 100;

    private VerificationHost host;

    public String testURI;

    public int requestCount = 1000;

    public int connectionCount = 32;

    public long serviceCount = 10;

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
    public void requestRateLimits() throws Throwable {
        setUp(true);
        this.host.setAuthorizationService(new AuthorizationContextService());
        this.host.setAuthorizationEnabled(true);
        this.host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(100));
        this.host.start();

        this.host.setSystemAuthorizationContext();
        Service s = this.host.startServiceAndWait(MinimalTestService.class, UUID.randomUUID().toString());
        String userPath = AuthorizationHelper.createUserService(this.host, this.host, "someone@example.org");
        this.host.resetAuthorizationContext();

        this.host.assumeIdentity(userPath, null);

        // set limit for this user to 1 request / second
        this.host.setRequestRateLimit(userPath, 1.0);
        Thread.sleep(this.host.getMaintenanceIntervalMicros() / 1000);
        AtomicInteger failureCount = new AtomicInteger();
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (o.getStatusCode() == Operation.STATUS_CODE_UNAVAILABLE) {
                    failureCount.incrementAndGet();
                }
            }
            this.host.completeIteration();
        };

        // send 1000 requests, at once, clearly violating the limit, and expect failures
        int count = 1000;

        this.host.testStart(count);
        for (int i = 0; i < count; i++) {
            Operation op = null;

            if (i % 2 == 0) {
                // every other operation send request to another service, request rate limiting
                // should apply across services, its the user that matters
                op = Operation.createGet(UriUtils.buildUri(this.host,
                        ServiceHostManagementService.SELF_LINK)).setCompletion(c);
            } else {
                op = Operation.createPatch(s.getUri())
                        .setBody(this.host.buildMinimalTestState())
                        .setCompletion(c);
            }
            this.host.send(op);
        }
        this.host.testWait();

        assertTrue(failureCount.get() > 0);
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
            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--sandbox=" + this.tmpFolder.getRoot().toURI(),
                    "--port=0",
                    "--bindAddress=" + bindAddress,
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

            assertEquals(bindAddress, h.getPreferredAddress());

            assertEquals(bindAddress, h.getUri().getHost());

            assertEquals(hostId, h.getId());
            assertEquals(h.getUri(), h.getPublicUri());

            // confirm the node group self node entry uses the public URI for the bind address
            NodeGroupState ngs = this.host.getServiceState(null, NodeGroupState.class,
                    UriUtils.buildUri(h.getUri(), ServiceUriPaths.DEFAULT_NODE_GROUP));

            NodeState selfEntry = ngs.nodes.get(h.getId());
            assertEquals(bindAddress, selfEntry.groupReference.getHost());
            assertEquals(h.getUri().getPort(), selfEntry.groupReference.getPort());

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
            ServiceHostState [] state = new ServiceHostState[1];
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

            String [] args2 = {
                    "--port=" + 0,
                    "--bindAddress=" + bindAddress,
                    "--sandbox=" + this.tmpFolder.getRoot().toURI(),
                    "--id=" + hostId
            };

            h.initialize(args2);
            h.start();

            assertEquals(bindAddress, h.getState().bindAddress);
            assertEquals(hostId, h.getState().id);

            verifyAuthorizedServiceMethods(h);
        } finally {
            h.stop();
        }

    }

    private void verifyAuthorizedServiceMethods(ServiceHost h) {
        MinimalTestService s = new MinimalTestService();
        try {
            h.getAuthorizationContext(s, UUID.randomUUID().toString());
            throw new IllegalStateException("call should have failed");
        } catch (IllegalStateException e) {
            throw e;
        } catch (RuntimeException e) {

        }

        try {
            h.cacheAuthorizationContext(s, UUID.randomUUID().toString(),
                    this.host.getGuestAuthorizationContext());
            throw new IllegalStateException("call should have failed");
        } catch (IllegalStateException e) {
            throw e;
        } catch (RuntimeException e) {

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
        args = new String[] {
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
        args = new String[] {
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
        args = new String[] {
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
                .setExpiration(Utils.getNowMicrosUtc() + maintenanceIntervalMicros)
                .setBody(initialState)
                .setCompletion(this.host.getExpectedFailureCompletion());
        this.host.startService(startPost, new MinimalTestService());
        this.host.testWait();
    }

    public static class StopOrderTestService extends StatefulService {

        public int stopOrder;

        public AtomicInteger globalStopOrder;

        public StopOrderTestService() {
            super(MinimalTestServiceState.class);
        }

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
    public void serviceHostMaintenanceAndStatsReporting() throws Throwable {
        setUp(true);

        // induce host to clear service state cache by setting mem limit low
        this.host.setServiceMemoryLimit(ServiceHost.ROOT_PATH, 0.0001);
        this.host.setServiceMemoryLimit(LuceneDocumentIndexService.SELF_LINK, 0.0001);
        long maintIntervalMillis = 100;
        long maintenanceIntervalMicros = TimeUnit.MILLISECONDS.toMicros(maintIntervalMillis);
        this.host.setMaintenanceIntervalMicros(maintenanceIntervalMicros);
        this.host.setServiceCacheClearDelayMicros(TimeUnit.MILLISECONDS
                .toMicros(maintIntervalMillis / 2));
        this.host.start();

        verifyMaintenanceDelayStat(maintenanceIntervalMicros);

        long opCount = 2;
        EnumSet<ServiceOption> caps = EnumSet.of(ServiceOption.PERSISTENCE,
                ServiceOption.INSTRUMENTATION, ServiceOption.PERIODIC_MAINTENANCE);

        List<Service> services = this.host.doThroughputServiceStart(
                this.serviceCount, MinimalTestService.class, this.host.buildMinimalTestState(),
                caps,
                null);

        long start = Utils.getNowMicrosUtc();
        List<Service> slowMaintServices = this.host.doThroughputServiceStart(null,
                this.serviceCount, MinimalTestService.class, this.host.buildMinimalTestState(),
                caps,
                null, maintenanceIntervalMicros * 10);

        List<URI> uris = new ArrayList<>();
        for (Service s : services) {
            uris.add(s.getUri());
        }

        this.host.doPutPerService(opCount, EnumSet.of(TestProperty.FORCE_REMOTE),
                services);

        long cacheMissCount = 0;
        long cacheClearCount = 0;
        Map<URI, ServiceStats> servicesWithMaintenance = new HashMap<>();

        // guarantee at least a few intervals have passed. Other we risk false negatives.
        Thread.sleep(maintIntervalMillis * 10);

        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            // issue GET to actually make the cache miss occur (if the cache has been cleared)
            this.host.getServiceState(null, MinimalTestServiceState.class, uris);

            // verify each service show at least a couple of maintenance requests
            URI[] statUris = buildStatsUris(this.serviceCount, services);
            Map<URI, ServiceStats> stats = this.host.getServiceState(null,
                    ServiceStats.class, statUris);

            for (Entry<URI, ServiceStats> e : stats.entrySet()) {
                long maintFailureCount = 0;
                ServiceStats s = e.getValue();

                for (ServiceStat st : s.entries.values()) {

                    if (st.name.equals(Service.STAT_NAME_CACHE_MISS_COUNT)) {
                        cacheMissCount += (long) st.latestValue;
                        continue;
                    }

                    if (st.name.equals(Service.STAT_NAME_CACHE_CLEAR_COUNT)) {
                        cacheClearCount += (long) st.latestValue;
                        continue;
                    }
                    if (st.name.equals(MinimalTestService.STAT_NAME_MAINTENANCE_SUCCESS_COUNT)) {
                        servicesWithMaintenance.put(e.getKey(), e.getValue());
                        continue;
                    }
                    if (st.name.equals(MinimalTestService.STAT_NAME_MAINTENANCE_FAILURE_COUNT)) {
                        maintFailureCount++;
                        continue;
                    }
                }

                assertTrue("maintenance failed", maintFailureCount == 0);
            }

            // verify that every single service has seen at least one maintenance interval
            if (servicesWithMaintenance.size() < this.serviceCount) {
                this.host.log("Services with maintenance: %d, expected %d",
                        servicesWithMaintenance.size(), this.serviceCount);
                Thread.sleep(maintIntervalMillis * 2);
                continue;
            }

            if (cacheMissCount < 1) {
                this.host.log("No cache misses seen");
                Thread.sleep(maintIntervalMillis * 2);
                continue;
            }

            if (cacheClearCount < 1) {
                this.host.log("No cache clears seen");
                Thread.sleep(maintIntervalMillis * 2);
                continue;
            }

            break;
        }
        long end = Utils.getNowMicrosUtc();

        this.host.log("State cache misses: %d, cache clears: %d", cacheMissCount, cacheClearCount);

        double expectedMaintIntervals = Math.max(1,
                (end - start) / this.host.getMaintenanceIntervalMicros());

        // allow variance up to 2x of expected intervals. We have the interval set to 100ms
        // and we are running tests on VMs, in over subscribed CI. So we expect significant
        // scheduling variance. This test is extremely consistent on a local machine
        expectedMaintIntervals *= 2;

        for (Entry<URI, ServiceStats> e : servicesWithMaintenance.entrySet()) {

            ServiceStat maintStat = e.getValue().entries.get(Service.STAT_NAME_MAINTENANCE_COUNT);
            this.host.log("%s has %f intervals", e.getKey(), maintStat.latestValue);
            if (maintStat.latestValue > expectedMaintIntervals + 2) {
                String error = String.format("Expected %f, got %f. Too many stats for service %s",
                        expectedMaintIntervals + 2,
                        maintStat.latestValue,
                        e.getKey());
                throw new IllegalStateException(error);
            }

        }


        if (cacheMissCount < 1) {
            throw new IllegalStateException(
                    "No cache misses observed through stats");
        }

        long slowMaintInterval = this.host.getMaintenanceIntervalMicros() * 10;
        end = Utils.getNowMicrosUtc();
        expectedMaintIntervals = Math.max(1, (end - start) / slowMaintInterval);

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
        // delete all minimal service instances
        for (Service s : slowMaintServices) {
            this.host.send(Operation.createDelete(s.getUri()).setBody(new ServiceDocument())
                    .setCompletion(this.host.getCompletion()));
        }
        this.host.testWait();

        // Test expiration: create N factory services, with expiration set in the initial state
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        List<URI> exampleURIs = new ArrayList<>();
        this.host.createExampleServices(this.host, this.serviceCount, exampleURIs,
                Utils.getNowMicrosUtc());

        ServiceDocumentQueryResult rsp = new ServiceDocumentQueryResult();
        exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {

            // let maintenance run
            Thread.sleep(maintIntervalMillis);
            rsp = this.host.getFactoryState(UriUtils.buildFactoryUri(this.host,
                    ExampleService.class));
            if (rsp.documentLinks == null || rsp.documentLinks.size() == 0) {
                break;
            }
        }

        if (rsp.documentLinks != null && rsp.documentLinks.size() > 0) {
            throw new IllegalStateException(
                    "Services are not expired:" + Utils.toJson(rsp));
        }

        // also verify expiration induced DELETE has resulted in permanent removal of all versions
        // of the document

        this.host.validatePermanentServiceDocumentDeletion(ExampleService.FACTORY_LINK, 0,
                true);

        // now validate that service handleMaintenance does not get called right after start, but at least
        // one interval later. We set the interval to 30 seconds so we can verify it did not get called within
        // one second or so
        long maintMicros = TimeUnit.SECONDS.toMicros(30);
        this.host.setMaintenanceIntervalMicros(maintMicros);

        // there is a small race: if the host scheduled a maintenance task already, using the default
        // 1 second interval, its possible it executes maintenance on the newly added services using
        // the 1 second schedule, instead of 30 seconds. So wait 1sec worth to minimize that
        Thread.sleep(ServiceHostState.DEFAULT_MAINTENANCE_INTERVAL_MICROS / 1000);

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
    }

    private void verifyMaintenanceDelayStat(long intervalMicros) throws Throwable {
        // verify state on maintenance delay takes hold
        this.host.setMaintenanceIntervalMicros(intervalMicros);
        MinimalTestService ts = new MinimalTestService();
        ts.delayMaintenance = true;
        ts.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        ts.toggleOption(ServiceOption.INSTRUMENTATION, true);
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = UUID.randomUUID().toString();
        ts = (MinimalTestService) this.host.startServiceAndWait(ts, UUID.randomUUID().toString(),
                body);
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            ServiceStats stats = this.host.getServiceState(null, ServiceStats.class,
                    UriUtils.buildStatsUri(ts.getUri()));
            if (stats.entries == null || stats.entries.isEmpty()) {
                Thread.sleep(intervalMicros / 1000);
                continue;
            }

            ServiceStat delayStat = stats.entries
                    .get(Service.STAT_NAME_MAINTENANCE_COMPLETION_DELAYED_COUNT);
            if (delayStat == null) {
                Thread.sleep(intervalMicros / 1000);
                continue;
            }
            break;
        }

        if (new Date().after(exp)) {
            throw new TimeoutException("Maintenance delay stat never reported");
        }

        ts.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
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
                    .setTargetReplicated(true)
                    .setExpiration(Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(1))
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
            public String name ;
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
        this.host.start();
        this.host.startFactoryServicesSynchronously(ParentService.createFactory(),
                ChildDependsOnParentService.createFactory());

        // verify instance services started
        ctx = this.host.testCreate(1);
        String childPath = UriUtils.buildUriPath(ChildDependsOnParentService.FACTORY_LINK, id);
        Operation get = Operation.createGet(UriUtils.buildUri(this.host, childPath))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
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
    public void servicePauseDueToMemoryPressure() throws Throwable {
        setUp(true);

        if (this.serviceCount >= 1000) {
            this.host.setStressTest(true);
        }

        // Set the threshold low to induce it during this test, several times. This will
        // verify that refreshing the index writer does not break the index semantics
        LuceneDocumentIndexService
                .setIndexFileCountThresholdForWriterRefresh(this.indexFileThreshold);

        // set memory limit low to force service pause
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

        AtomicLong selfLinkCounter = new AtomicLong();
        String prefix = "instance-";
        String name = UUID.randomUUID().toString();
        ExampleServiceState s = new ExampleServiceState();
        s.name = name;
        Consumer<Operation> bodySetter = (o) -> {
            s.documentSelfLink = prefix + selfLinkCounter.incrementAndGet();
            o.setBody(s);
        };

        URI factoryURI = UriUtils.buildFactoryUri(this.host, ExampleService.class);

        Map<URI, ExampleServiceState> states = this.host.doFactoryChildServiceStart(null,
                this.serviceCount,
                ExampleServiceState.class, bodySetter, factoryURI);

        // while pausing, issue updates to verify behavior under load. Services should either abort pause,
        // or be ignored due to recent update
        int updateCount = 100;
        if (this.testDurationSeconds > 0 || this.host.isStressTest()) {
            updateCount = 1;
        }
        patchExampleServices(states, updateCount);

        long expectedPauseTime = Utils.getNowMicrosUtc() + this.host.getMaintenanceIntervalMicros()
                * 5;
        while (this.host.getState().lastMaintenanceTimeUtcMicros < expectedPauseTime) {
            // memory limits are applied during maintenance, so wait for a few intervals.
            Thread.sleep(this.host.getMaintenanceIntervalMicros() / 1000);
        }

        // restore memory limit so we don't keep re-pausing/resuming services every time we talk to them.
        // In addition, if we try to get the stats from a service that was just scheduled for pause, the pause will
        // be cancelled, causing a test false negative (no pauses observed)
        this.host
                .setServiceMemoryLimit(ServiceHost.ROOT_PATH, ServiceHost.DEFAULT_PCT_MEMORY_LIMIT);

        // services should all be paused now since we set the host memory limit so low. Send requests to
        // prove resume worked. We will then verify the per stats to make sure both pause and resume actually occurred
        patchExampleServices(states, 1);

        Map<String, ServiceStats> stats = Collections.synchronizedMap(new HashMap<>());
        this.host.testStart(states.size() * 2);
        for (ExampleServiceState st : states.values()) {

            Operation getStats = Operation.createGet(
                    UriUtils.buildStatsUri(UriUtils.buildUri(this.host, st.documentSelfLink)))
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        ServiceStats rsp = o.getBody(ServiceStats.class);
                        stats.put(st.documentSelfLink, rsp);
                        this.host.completeIteration();
                    });
            this.host.send(getStats);

            Operation get = Operation.createGet(UriUtils.buildUri(this.host, st.documentSelfLink))
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    this.host.failIteration(e);
                                    return;
                                }

                                ExampleServiceState rsp = o.getBody(ExampleServiceState.class);
                                if (!rsp.name.startsWith("updated")) {
                                    this.host.failIteration(new IllegalStateException(Utils
                                            .toJsonHtml(rsp)));
                                    return;
                                }
                                this.host.completeIteration();
                            });
            this.host.send(get);
        }

        this.host.testWait();

        if (this.testDurationSeconds == 0) {
            for (ServiceStats statsPerInstance : stats.values()) {
                ServiceStat pauseStat = statsPerInstance.entries.get(Service.STAT_NAME_PAUSE_COUNT);
                ServiceStat resumeStat = statsPerInstance.entries
                        .get(Service.STAT_NAME_RESUME_COUNT);
                if (pauseStat == null) {
                    throw new IllegalStateException("No pauses observed");
                }
                if (resumeStat == null) {
                    throw new IllegalStateException("No resumes observed");
                }
            }
            return;
        }

        states.clear();
        // Long running test. Keep adding services, expecting pause to occur and free up memory so the
        // number of service instances exceeds available memory.
        Date exp = new Date(TimeUnit.MICROSECONDS.toMillis(
                Utils.getNowMicrosUtc()) + TimeUnit.SECONDS.toMillis(this.testDurationSeconds));

        this.host.setOperationTimeOutMicros(
                TimeUnit.SECONDS.toMicros(this.host.getTimeoutSeconds()));

        while (new Date().before(exp)) {
            this.host.doFactoryChildServiceStart(null,
                    this.serviceCount,
                    ExampleServiceState.class, bodySetter, factoryURI);
            Thread.sleep(500);
            this.host.log("created %d services, created so far: %d", this.serviceCount,
                    selfLinkCounter.get());
            Runtime.getRuntime().gc();
            this.host.logMemoryInfo();

            File f = new File(this.host.getStorageSandbox());
            this.host.log("Sandbox: %s, Disk: free %d, usable: %d, total: %d", f.toURI(),
                    f.getFreeSpace(),
                    f.getUsableSpace(),
                    f.getTotalSpace());

            int limit = (int) (this.serviceCount * 2);
            int step = 1;
            int count = limit / step;
            if (selfLinkCounter.get() < limit) {
                continue;
            }

            this.host.testStart(count);
            for (int i = 0; i < count; i += step) {
                // now that we have created a bunch of services, and a lot of them are paused, ping one randomly
                // to make sure it resumes
                URI instanceUri = UriUtils.buildFactoryUri(this.host, ExampleService.class);
                instanceUri = UriUtils.extendUri(instanceUri, prefix + (selfLinkCounter.get() - i));
                Operation get = Operation.createGet(instanceUri).setCompletion((o, e) -> {
                    if (e == null) {
                        this.host.completeIteration();
                        return;
                    }

                    if (o.getStatusCode() == Operation.STATUS_CODE_TIMEOUT) {
                        // check the document index, if we ever created this service
                        try {
                            this.host.createAndWaitSimpleDirectQuery(
                                    ServiceDocument.FIELD_NAME_SELF_LINK, o.getUri().getPath(), 1,
                                    1);
                        } catch (Throwable e1) {
                            this.host.failIteration(e1);
                            return;
                        }
                    }
                    this.host.failIteration(e);
                });
                this.host.send(get);
            }
            this.host.testWait();
        }
    }

    private void patchExampleServices(Map<URI, ExampleServiceState> states, int count)
            throws Throwable {
        this.host.testStart(states.size() * count);
        for (ExampleServiceState st : states.values()) {
            for (int i = 0; i < count; i++) {
                st.name = "updated" + Utils.getNowMicrosUtc() + "";
                Operation patch = Operation
                        .createPatch(UriUtils.buildUri(this.host, st.documentSelfLink))
                        .setCompletion(this.host.getCompletion())
                        .setBody(st);
                this.host.send(patch);
            }
        }
        this.host.testWait();
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
    public void getAvailableServicesWithOptions() throws Throwable {
        setUp(false);
        int serviceCount = 5;
        List<URI> exampleURIs = new ArrayList<>();
        this.host.createExampleServices(this.host, serviceCount, exampleURIs,
                Utils.getNowMicrosUtc());

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

        this.host.testStart(1);
        this.host.queryServiceUris(options, true, get.clone());
        this.host.testWait();
        assertEquals(serviceCount, results[0].documentLinks.size());
        this.host.testStart(1);
        this.host.queryServiceUris(options, false, get.clone());
        this.host.testWait();
        assertTrue(results[0].documentLinks.size() >= serviceCount);
    }

    /**
     * This test verify the custom Ui path resource of service
     **/
    @Test
    public void testServiceCustomUIPath() throws Throwable {
        setUp(false);
        String resourcePath = "customUiPath";
        //Service with custom path
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

        //Starting the  CustomUiPathService service
        this.host.startService(Operation.createPost(UriUtils.buildUri(this.host,
                CustomUiPathService.SELF_LINK)), new CustomUiPathService());
        this.host.waitForServiceAvailable(CustomUiPathService.SELF_LINK);

        String htmlPath = "/user-interface/resources/" + resourcePath + "/custom.html";
        //Sending get request for html
        String htmlResponse = this.host.sendWithJavaClient(
                UriUtils.buildUri(this.host, htmlPath),
                Operation.MEDIA_TYPE_TEXT_HTML, null);

        assertEquals("<html>customHtml</html>", htmlResponse);
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
        VerificationHost.createAndAttachSSLClient(this.host, null, null);
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

    @After
    public void tearDown() {
        if (this.host == null) {
            return;
        }
        this.host.tearDown();
    }

}
