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

    public static final String PROPERTY_NAME_REQUEST_PAYLOAD_SIZE_LIMIT =
            Utils.PROPERTY_NAME_PREFIX + "ServiceClient.REQUEST_PAYLOAD_SIZE_LIMIT";

    public static final String PROPERTY_NAME_MAX_BINARY_SERIALIZED_BODY_LIMIT =
            Utils.PROPERTY_NAME_PREFIX + "ServiceClient.MAX_BINARY_SERIALIZED_BODY_LIMIT";

    public static final String PROPERTY_NAME_DEFAULT_CONNECTION_LIMIT_PER_HOST =
            Utils.PROPERTY_NAME_PREFIX + "ServiceClient.DEFAULT_CONNECTIONS_PER_HOST";

    public static final String PROPERTY_NAME_DEFAULT_CONNECTION_LIMIT_PER_TAG =
            Utils.PROPERTY_NAME_PREFIX + "ServiceClient.DEFAULT_CONNECTION_LIMIT_PER_TAG";

    public static final int MAX_BINARY_SERIALIZED_BODY_LIMIT = Integer.getInteger(
            PROPERTY_NAME_MAX_BINARY_SERIALIZED_BODY_LIMIT, 1024 * 1024);

    public static final int DEFAULT_CONNECTION_LIMIT_PER_HOST = Integer.getInteger(
            PROPERTY_NAME_DEFAULT_CONNECTION_LIMIT_PER_HOST, 128);

    public static final int DEFAULT_CONNECTION_LIMIT_PER_TAG = Integer.getInteger(
            PROPERTY_NAME_DEFAULT_CONNECTION_LIMIT_PER_TAG, 4);

    public static final int REQUEST_PAYLOAD_SIZE_LIMIT = Integer.getInteger(
            PROPERTY_NAME_REQUEST_PAYLOAD_SIZE_LIMIT, 1024 * 1024 * 64);

    /**
     * Connection tag used by node group service for peer to peer random probing and liveness checks
     */
    public static final String CONNECTION_TAG_GOSSIP = "xn-cnx-tag-gossip";

    /**
     * Connection tag used by node selector services for request related to replication and multiple
     * node processes
     */
    public static final String CONNECTION_TAG_REPLICATION = "xn-cnx-tag-replication";

    /**
     * Connection tag used by service host for peer to peer forwarding during load
     * balancing and node selection
     */
    public static final String CONNECTION_TAG_FORWARDING = "xn-cnx-tag-p2p-fwd";

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

    /**
     * Sets the maximum size of a request payload in bytes, that can be sent from the client.
     */
    ServiceClient setRequestPayloadSizeLimit(int limit);

    /**
     * Returns the maximum size of a request payload in bytes.
     */
    int getRequestPayloadSizeLimit();
}
