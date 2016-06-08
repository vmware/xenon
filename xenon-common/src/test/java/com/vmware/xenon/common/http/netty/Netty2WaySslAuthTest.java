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

package com.vmware.xenon.common.http.netty;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.SslClientAuthMode;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * DCP Host with 2-way SSL certificate authentication test.
 * <p/>
 * Server uses server.crt as a certificate and server.pem as private key and trustedcerts.jks as a keystore of trusted
 * certificates used to validate client's certificate.
 * <p/>
 * Client uses client.p12 as client key and certificate and trustedcerts.jks as a keystore of trusted certificates used
 * to validate client's certificate
 */
public class Netty2WaySslAuthTest {
    public static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    public static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    public int securePort = 0;
    private VerificationHost host;
    private TemporaryFolder temporaryFolder;
    private static String savedTrustStore;
    private static String savedTrustStorePassword;

    @BeforeClass
    public static void setUpClass() throws Exception {
        savedTrustStore = System.getProperty(JAVAX_NET_SSL_TRUST_STORE);
        savedTrustStorePassword = System.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
        System.setProperty(JAVAX_NET_SSL_TRUST_STORE,
                getCanonicalFileForResource("/ssl/trustedcerts.jks")
                        .getCanonicalPath());
        System.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, "changeit");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (savedTrustStore == null) {
            System.clearProperty(JAVAX_NET_SSL_TRUST_STORE);
        } else {
            System.setProperty(JAVAX_NET_SSL_TRUST_STORE, savedTrustStore);
        }
        if (savedTrustStorePassword == null) {
            System.clearProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
        } else {
            System.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, savedTrustStorePassword);
        }
    }

    @Before
    public void setUp() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        this.temporaryFolder = new TemporaryFolder();
        this.temporaryFolder.create();
        this.host = new VerificationHost();
        ServiceHost.Arguments args = new ServiceHost.Arguments();
        args.securePort = this.securePort;
        args.port = 0;
        args.keyFile = getCanonicalFileForResource("/ssl/server.pem").toPath();
        args.certificateFile = getCanonicalFileForResource("/ssl/server.crt").toPath();
        args.sslClientAuthMode = SslClientAuthMode.WANT;
        args.sandbox = this.temporaryFolder.getRoot().toPath();
        args.bindAddress = ServiceHost.LOOPBACK_ADDRESS;

        if (args.securePort != 0) {
            args.port = 0;
            args.peerNodes = new String[] { "https://127.0.0.1:" + args.securePort };
        }

        this.host.initialize(args);
        this.host.start();

        if (args.securePort == 0) {
            return;
        }

        // verify that the self URI supplied, even if its a HTTPS, was filtered out of the
        // peer list
        assertEquals(this.host.getInitialPeerHosts().size(), 0);

        // verify quorum is set to 1, since we supplied just self as peer
        this.host.waitFor("quorum not set", () -> {
            NodeGroupState ngs = this.host.getServiceState(null,
                    NodeGroupState.class,
                    UriUtils.buildUri(this.host, ServiceUriPaths.DEFAULT_NODE_GROUP));
            NodeState self = ngs.nodes.get(this.host.getId());
            if (self == null) {
                return false;
            }
            if (self.membershipQuorum != 1) {
                return false;
            }
            return true;
        });
    }

    @After
    public void tearDown() {
        this.host.stop();
        this.temporaryFolder.delete();
    }

    @Test
    public void testCustomSslContext() throws Throwable {
        Path keyFile = getCanonicalFileForResource("/ssl/server.pem").toPath();
        Path certificateFile = getCanonicalFileForResource("/ssl/server.crt").toPath();
        SslContext customContext = SslContextBuilder.forServer(
                certificateFile.toFile(), keyFile.toFile(), null)
                .build();
        NettyHttpListener l = new NettyHttpListener(this.host);
        l.setSSLContext(customContext);

        // verify that we can not set context after host is started
        NettyHttpListener hostListener = (NettyHttpListener) this.host.getListener();
        try {
            hostListener.setSSLContext(customContext);
            throw new RuntimeException("call should have thrown an exception");
        } catch (IllegalStateException e) {

        }

        // now stop the host, replace the listener, and restart
        this.host.stop();
        this.host.setPort(0);
        this.host.setSecurePort(0);
        this.host.setListener(l);
        this.host.start();
        assertEquals(l, this.host.getListener());
        hostListener = (NettyHttpListener) this.host.getListener();
        assertEquals(hostListener.getSSLContext(), customContext);

        test2WaySsl();
    }

    @Test
    public void test2WaySsl() throws Throwable {
        // Start sample test service
        this.host.testStart(1);
        URI testServiceUri = UriUtils.buildUri(this.host, TestService.SELF_LINK);
        Operation post = Operation.createPost(testServiceUri)
                .setCompletion((o, e) -> this.host.completeIteration());
        this.host.startService(post, new TestService());
        this.host.testWait();

        // Create ServiceClient with client SSL auth support
        ServiceClient client = NettyHttpServiceClient.create(
                getClass().getCanonicalName(),
                Executors.newFixedThreadPool(4),
                Executors.newScheduledThreadPool(1));

        SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init((KeyStore) null);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                .getDefaultAlgorithm());
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream stream = Netty2WaySslAuthTest.class.getResourceAsStream("/ssl/client.p12")) {
            keyStore.load(stream, "changeit".toCharArray());
        }
        kmf.init(keyStore, "changeit".toCharArray());
        clientContext.init(kmf.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
        client.setSSLContext(clientContext);
        client.start();

        // Perform request and validate that detected principal is correct
        this.host.testStart(1);
        AtomicReference<String> result = new AtomicReference<>();
        Operation get = Operation
                .createGet(UriUtils.buildUri(this.host.getSecureUri(), TestService.SELF_LINK))
                .setReferer(this.host.getPublicUri())
                .setCompletion((o, e) -> {
                    if (e == null) {
                        result.set(o.getBody(TestServiceResponse.class).principal);
                    } else {
                        this.host.log(Level.SEVERE, "Operation failed: %s", Utils.toString(e));
                    }
                    this.host.completeIteration();
                });
        client.send(get);
        this.host.testWait();
        Assert.assertNotNull("Peer principal", result.get());
        Assert.assertEquals("Peer principal", "CN=agent-461b1767-ea89-4452-9408-283d0752fe40",
                result.get());
        client.stop();
    }

    private static File getCanonicalFileForResource(String resourceName) throws IOException,
            URISyntaxException {
        return new File(new URI(Netty2WaySslAuthTest.class.getResource(resourceName).toString()))
                .getCanonicalFile();
    }

    public static class TestServiceResponse {
        public String principal;
    }

    /**
     * This test service returns client's SSL principal name on each GET request.
     */
    public static class TestService extends StatelessService {
        public static final String SELF_LINK = "/ssl_test";

        public TestService() {
            super(ServiceDocument.class);
        }

        @Override
        public void handleGet(Operation get) {
            try {
                TestServiceResponse resp = new TestServiceResponse();
                resp.principal = get.getPeerPrincipal().toString();
                get.setBody(resp);
                get.complete();
            } catch (Exception e) {
                get.fail(e);
            }
        }
    }
}
