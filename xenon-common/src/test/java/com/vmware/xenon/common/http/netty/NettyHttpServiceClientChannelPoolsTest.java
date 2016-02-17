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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.ServiceClient;

public class NettyHttpServiceClientChannelPoolsTest {
    private NettyHttpServiceClient client;

    @Before
    public void setUp() throws Exception {
        client = (NettyHttpServiceClient) NettyHttpServiceClient.create(
                NettyHttpServiceClientChannelPoolsTest.class.getCanonicalName(),
                Executors.newFixedThreadPool(4),
                Executors.newScheduledThreadPool(1));

        SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
        clientContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
        client.setSSLContext(clientContext);
    }

    @Test
    public void testChannelPoolInitialization() {
        // Instantiated before calling client.start()
        assertNotNull(client.getChannelPool());
    }

    @Test
    public void testSSLChannelPoolInitialization() {
        assertNull(client.getSslChannelPool());

        client.start();

        assertNotNull(client.getSslChannelPool());
    }

    @Test
    public void testDefaultConnectionLimit() {
        client.start();

        assertEquals(NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST, client.getChannelPool()
                .getConnectionLimitPerHost());
        assertEquals(NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST, client
                .getSslChannelPool().getConnectionLimitPerHost());
    }

    @Test
    public void testSetConnectionLimitBeforeSslChanngelPoolInit() {
        int connectionLimit = 11;

        client.setConnectionLimitPerHost(connectionLimit);

        client.start();

        assertEquals(connectionLimit, client.getChannelPool().getConnectionLimitPerHost());
        assertEquals(connectionLimit, client.getSslChannelPool().getConnectionLimitPerHost());
    }

    @Test
    public void testSetConnectionLimitAfterSslChanngelPoolInit() {
        int connectionLimit = 11;

        client.start();

        client.setConnectionLimitPerHost(connectionLimit);

        assertEquals(connectionLimit, client.getChannelPool().getConnectionLimitPerHost());
        assertEquals(connectionLimit, client.getSslChannelPool().getConnectionLimitPerHost());
    }
}
