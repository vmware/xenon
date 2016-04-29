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

import javax.net.ssl.SSLContext;

public interface ServiceClient extends ServiceRequestSender {
    String SSL_PROTOCOL_NAME = "SSL";
    String TLS_PROTOCOL_NAME = "TLS";

    public static final int DEFAULT_CONNECTION_LIMIT_PER_HOST = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX + "NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST",
            128);

    public static final int DEFAULT_CONNECTION_LIMIT_PER_TAG = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX + "ServiceClient.DEFAULT_CONNECTION_LIMIT_PER_TAG", 4);

    /**
     * Well known connection tag used by runtime for request related to replication and multiple
     * node processes
     */
    public static final String CONNECTION_TAG_REPLICATION = "xn-cnx-tag-replication";

    /**
     * Well known connection tag used when no explicit tag is specified. It will use the per
     * client, per host connection limit
     */
    public static final String CONNECTION_TAG_DEFAULT = "xn-cnx-tag-default";

    /**
     * Well known HTTP2 connection tag used when no explicit tag is specified. It will use the per
     * tag default connection limit
     */
    public static final String CONNECTION_TAG_HTTP2_DEFAULT = "xn-cnx-tag-http2-default";

    public ServiceClient setSSLContext(SSLContext context);

    public SSLContext getSSLContext();

    void start();

    void handleMaintenance(Operation op);

    void stop();

    /**
     * Alias for {@link ServiceRequestSender#sendRequest(Operation)}.
     * @param op
     */
    void send(Operation op);

    void sendWithCallback(Operation op);

    /**
     * Maximum number of connections cached and re-used for a given host and port tuple. This
     * setting applies to HTTP1.1 and HTTPS on HTTP1.1 connections. If the connection limit is
     * used and all out bound channels are busy waiting for responses, any new requests are queued.
     *
     * For fine grained control of connection limits please use {@code ServiceClient#setConnectionLimitPerTag(int)}
     */
    ServiceClient setConnectionLimitPerHost(int limit);

    /**
     * Returns the maximum number of connections per host and port tuple
     */
    int getConnectionLimitPerHost();

    /**
     * Maximum number of connections cached and re-used for a given host, port and
     * connection tag tuple
     */
    ServiceClient setConnectionLimitPerTag(String connectionTag, int limit);

    /**
     * Returns the maximum number of connections for the given tag
     */
    int getConnectionLimitPerTag(String connectionTag);
}