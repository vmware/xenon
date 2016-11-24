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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.http.netty.NettyHttpServiceClient;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class XenonHostWithPeerListenerTest {

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
    public void startWithSslConfigInheritedFromDefaultListener() throws Throwable {
        XenonHostWithPeerListener h = new XenonHostWithPeerListener();
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();

        ServiceClient client = createAllTrustingServiceClient();
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
            assertCanGetNodeGroup(client, u);

            u = makeNodeGroupUri(h.getUri());
            assertCanGetNodeGroup(client, u);
        } finally {
            h.stop();
            tmpFolder.delete();
        }
    }

    @Test
    public void startWithDifferentSslConfig() throws Throwable {
        XenonHostWithPeerListener h = new XenonHostWithPeerListener();
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();

        ServiceClient client = createAllTrustingServiceClient();
        try {
            String bindAddress = "127.0.0.1";
            String nodeGroupUri = "https://127.0.0.1:0";

            String hostId = UUID.randomUUID().toString();

            String[] args = {
                    "--port=0",
                    "--nodeGroupPublicUri=" + nodeGroupUri,
                    "--peerKeyFile=" + "../xenon-common/src/test/resources/ssl/server.pem",
                    "--peerCertificateFile=" + "../xenon-common/src/test/resources/ssl/server.crt",
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
            assertCanGetNodeGroup(client, u);

            u = makeNodeGroupUri(h.getUri());
            assertCanGetNodeGroup(client, u);
        } finally {
            h.stop();
            tmpFolder.delete();
        }
    }

    private ServiceClient createAllTrustingServiceClient()
            throws URISyntaxException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException,
            UnrecoverableKeyException, KeyManagementException {
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
        clientContext.init(kmf.getKeyManagers(), new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                            throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                            throws CertificateException {

                    }

                    @Override public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        }, null);
        client.setSSLContext(clientContext);
        client.start();
        return client;
    }

    private void assertCanGetNodeGroup(ServiceClient client, URI u)
            throws InterruptedException, ExecutionException {

        TestRequestSender sender = new TestRequestSender(client);
        Operation op = Operation.createGet(u)
                .forceRemote()
                .setReferer(u);

        op = sender.sendAndWait(op);
        assertEquals(Operation.STATUS_CODE_OK, op.getStatusCode());
    }

    private URI makeNodeGroupUri(URI u) {
        u = UriUtils.buildUri(u.getScheme(), u.getHost(), u.getPort(), ServiceUriPaths.DEFAULT_NODE_GROUP, null);
        return u;
    }
}