/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.host;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.UUID;
import java.util.concurrent.Executors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class XenonHostWithPeerListenerTest {

    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    private static String savedTrustStore;
    private static String savedTrustStorePassword;

    @BeforeClass
    public static void setUpClass() throws Exception {
        savedTrustStore = System.getProperty(JAVAX_NET_SSL_TRUST_STORE);
        savedTrustStorePassword = System.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
        System.setProperty(JAVAX_NET_SSL_TRUST_STORE,
                "../xenon-common/src/test/resources/ssl/trustedcerts.jks");
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

    @Test
    public void startUpWithArguments() throws Throwable {
        XenonHostWithPeerListener h = new XenonHostWithPeerListener();
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        try {
            String bindAddress = "127.0.0.1";
            String nodeGroupUri = "http://127.0.0.1:0";

            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--port=0",
                    "--nodeGroupPublicUri=" + nodeGroupUri,
                    "--sandbox=" + tmpFolder.getRoot().getAbsolutePath(),
                    "--bindAddress=" + bindAddress,
                    "--id=" + hostId
            };

            h.initialize(args);

            h.start();

            assertEquals(bindAddress, h.getPreferredAddress());
            assertEquals(bindAddress, h.getUri().getHost());

            assertNotEquals(h.getUri(), h.getPublicUri());
            assertNotEquals(nodeGroupUri, h.getPublicUri().toString());

            URI u = makeNodeGroupUri(h.getPublicUri());
            assertCanGetNodeGroup(h.getClient(), u);

            u = makeNodeGroupUri(h.getUri());
            assertCanGetNodeGroup(h.getClient(), u);
        } finally {
            h.stop();
            tmpFolder.delete();
        }
    }

    @Test
    public void testSslConfigInheritedFromDefaultListener() throws Throwable {
        ServiceClient client = createAllTrustingServiceClient();
        doSslConfigInheritedFromDefaultListener(client, false);
    }

    @Test
    public void testHttp2SslConfigInheritedFromDefaultListener() throws Throwable {
        Assume.assumeTrue(OpenSsl.isAlpnSupported());
        ServiceClient client = createAllTrustingHttp2ServiceClient();
        doSslConfigInheritedFromDefaultListener(client, true);
    }

    private void doSslConfigInheritedFromDefaultListener(ServiceClient client,
            boolean connectionSharing) throws Throwable {
        XenonHostWithPeerListener h = new XenonHostWithPeerListener();
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();

        try {
            String bindAddress = "127.0.0.1";
            String nodeGroupUri = "https://127.0.0.1:0";

            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--securePort=0",
                    "--port=-1",
                    "--nodeGroupPublicUri=" + nodeGroupUri,
                    "--keyFile=" + "../xenon-common/src/test/resources/ssl/server.pem",
                    "--certificateFile=" + "../xenon-common/src/test/resources/ssl/server.crt",
                    "--sslClientAuthMode=NEED",
                    "--sandbox=" + tmpFolder.getRoot().getAbsolutePath(),
                    "--bindAddress=" + bindAddress,
                    "--id=" + hostId
            };

            h.initialize(args);

            h.start();

            assertEquals(bindAddress, h.getPreferredAddress());
            assertEquals(bindAddress, h.getUri().getHost());
            assertNotEquals(h.getUri(), h.getPublicUri());
            assertNotEquals(nodeGroupUri, h.getPublicUri().toString());

            URI u = makeNodeGroupUri(h.getPublicUri());
            assertCanGetNodeGroup(client, u, connectionSharing);

            u = makeNodeGroupUri(h.getUri());
            assertCanGetNodeGroup(client, u, connectionSharing);
        } finally {
            h.stop();
            tmpFolder.delete();
        }
    }

    @Test
    public void testDifferentSslConfig() throws Throwable {
        ServiceClient client = createAllTrustingServiceClient();
        doDifferentSslConfig(client, false);
    }

    @Test
    public void testHttp2DifferentSslConfig() throws Throwable {
        Assume.assumeTrue(OpenSsl.isAlpnSupported());
        ServiceClient client = createAllTrustingHttp2ServiceClient();
        doDifferentSslConfig(client, true);
    }

    private void doDifferentSslConfig(ServiceClient client, boolean connectionSharing)
            throws Throwable {
        XenonHostWithPeerListener h = new XenonHostWithPeerListener();
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();

        try {
            String bindAddress = "127.0.0.1";
            String nodeGroupUri = "https://127.0.0.1:0";

            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--port=0",
                    "--nodeGroupPublicUri=" + nodeGroupUri,
                    "--peerKeyFile=" + "../xenon-common/src/test/resources/ssl/server.pem",
                    "--peerCertificateFile=" + "../xenon-common/src/test/resources/ssl/server.crt",
                    "--sslClientAuthMode=NEED",
                    "--sandbox=" + tmpFolder.getRoot().getAbsolutePath(),
                    "--bindAddress=" + bindAddress,
                    "--id=" + hostId
            };

            h.initialize(args);

            h.start();

            assertEquals(bindAddress, h.getPreferredAddress());
            assertEquals(bindAddress, h.getUri().getHost());
            assertNotEquals(h.getUri(), h.getPublicUri());
            assertNotEquals(nodeGroupUri, h.getPublicUri().toString());

            URI u = makeNodeGroupUri(h.getPublicUri());
            assertCanGetNodeGroup(client, u, connectionSharing);

            u = makeNodeGroupUri(h.getUri());
            assertCanGetNodeGroup(client, u, connectionSharing);
        } finally {
            h.stop();
            tmpFolder.delete();
        }
    }

    private ServiceClient newAllTrustingServiceClient() throws Throwable {
        // Create ServiceClient with client SSL auth support
        ServiceClient client = NettyHttpServiceClient.create(
                getClass().getCanonicalName(),
                Executors.newFixedThreadPool(4),
                Executors.newScheduledThreadPool(1));

        SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                .getDefaultAlgorithm());
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream stream = new FileInputStream("../xenon-common/src/test/resources/ssl/client.p12")) {
            keyStore.load(stream, "changeit".toCharArray());
        }
        kmf.init(keyStore, "changeit".toCharArray());
        clientContext.init(kmf.getKeyManagers(),
                InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
        client.setSSLContext(clientContext);
        return client;
    }

    private ServiceClient createAllTrustingServiceClient() throws Throwable {
        ServiceClient client = newAllTrustingServiceClient();
        client.start();
        return client;
    }

    private ServiceClient createAllTrustingHttp2ServiceClient() throws Throwable {
        ServiceClient client = newAllTrustingServiceClient();

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                .getDefaultAlgorithm());
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream stream = new FileInputStream("../xenon-common/src/test/resources/ssl/client.p12")) {
            keyStore.load(stream, "changeit".toCharArray());
        }
        kmf.init(keyStore, "changeit".toCharArray());

        SslContext http2ClientContext = SslContextBuilder.forClient()
                .keyManager(kmf)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                .applicationProtocolConfig(new ApplicationProtocolConfig(
                        ApplicationProtocolConfig.Protocol.ALPN,
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.HTTP_2))
                .build();

        ((NettyHttpServiceClient) client).setHttp2SslContext(http2ClientContext);
        client.start();
        return client;
    }

    private void assertCanGetNodeGroup(ServiceClient client, URI u) throws Throwable {
        assertCanGetNodeGroup(client, u, false);
    }

    private void assertCanGetNodeGroup(ServiceClient client, URI u, boolean connectionSharing)
            throws Throwable {

        TestRequestSender sender = new TestRequestSender(client);
        Operation op = Operation.createGet(u)
                .forceRemote()
                .setConnectionSharing(connectionSharing)
                .setReferer(u);

        op = sender.sendAndWait(op);
        assertEquals(Operation.STATUS_CODE_OK, op.getStatusCode());
    }

    private URI makeNodeGroupUri(URI u) {
        u = UriUtils.buildUri(u.getScheme(), u.getHost(), u.getPort(), ServiceUriPaths.DEFAULT_NODE_GROUP, null);
        return u;
    }
}