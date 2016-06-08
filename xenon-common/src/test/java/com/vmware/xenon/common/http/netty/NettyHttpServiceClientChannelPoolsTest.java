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

import java.util.concurrent.Executors;
import javax.net.ssl.SSLContext;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.ServiceClient;

public class NettyHttpServiceClientChannelPoolsTest {
    private NettyHttpServiceClient client;

    @Before
    public void setUp() throws Exception {
        this.client = (NettyHttpServiceClient) NettyHttpServiceClient.create(
                NettyHttpServiceClientChannelPoolsTest.class.getCanonicalName(),
                Executors.newFixedThreadPool(4),
                Executors.newScheduledThreadPool(1));

        SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
        clientContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
        this.client.setSSLContext(clientContext);
    }

    @Test
    public void testChannelPoolInitialization() {
        this.client.start();

        assertNotNull(this.client.getSslChannelPool());
        assertNotNull(this.client.getChannelPool());
        assertNotNull(this.client.getHttp2ChannelPool());
    }

    @Test
    public void testDefaultConnectionLimit() {
        this.client.start();

        assertEquals(NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST, this.client
                .getChannelPool()
                .getConnectionLimitPerHost());
        assertEquals(NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST, this.client
                .getSslChannelPool().getConnectionLimitPerHost());
    }

    @Test
    public void testSetConnectionLimitBeforeSslChannelPoolStart() {
        int connectionLimit = 11;

        this.client.setConnectionLimitPerHost(connectionLimit);

        this.client.start();

        assertEquals(connectionLimit, this.client.getChannelPool().getConnectionLimitPerHost());
        assertEquals(connectionLimit, this.client.getSslChannelPool().getConnectionLimitPerHost());
    }

    @Test
    public void testSetConnectionLimitAfterSslChannelPoolStart() {
        int connectionLimit = 11;

        this.client.start();

        this.client.setConnectionLimitPerHost(connectionLimit);

        assertEquals(connectionLimit, this.client.getChannelPool().getConnectionLimitPerHost());
        assertEquals(connectionLimit, this.client.getSslChannelPool().getConnectionLimitPerHost());
    }
}
