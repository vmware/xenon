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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.util.AsciiString;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.common.Operation.SocketContext;
import com.vmware.xenon.common.OperationContext;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.http.netty.NettyChannelPool.NettyChannelGroupKey;

/**
 * Asynchronous request / response client with concurrent connection management
 */
public class NettyHttpServiceClient implements ServiceClient {
    /**
     * Number of maximum parallel connections to a remote host. Idle connections are groomed but if
     * this limit is set too high, and we are talking to many remote hosts, we can possibly exceed
     * the process file descriptor limit
     */
    public static final int DEFAULT_CONNECTIONS_PER_HOST = ServiceClient.DEFAULT_CONNECTION_LIMIT_PER_HOST;

    public static final Logger LOGGER = Logger.getLogger(ServiceClient.class
            .getName());
    private static final String ENV_VAR_NAME_HTTP_PROXY = "http_proxy";

    static final AsciiString TRANSACTION_ID_HEADER_ASCII = new AsciiString(
            Operation.TRANSACTION_ID_HEADER);

    static final AsciiString CONTEXT_ID_HEADER_ASCII = new AsciiString(
            Operation.CONTEXT_ID_HEADER);

    static final AsciiString AUTH_TOKEN_HEADER_ASCII = new AsciiString(
            Operation.REQUEST_AUTH_TOKEN_HEADER);

    static final AsciiString DEFAULT_MEDIA_TYPE_ASCII = new AsciiString(
            Operation.MEDIA_TYPE_EVERYTHING_WILDCARDS);

    static final AsciiString PRAGMA_DIRECTIVE_REPLICATED_ASCII = new AsciiString(
            Operation.PRAGMA_DIRECTIVE_REPLICATED);

    static final AsciiString MEDIA_TYPE_JSON_ASCII = new AsciiString(
            Operation.MEDIA_TYPE_APPLICATION_JSON);

    static final AsciiString MEDIA_TYPE_KRYO_OCTET_STREAM_ASCII = new AsciiString(
            Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM);

    private static final int MEDIA_TYPE_APPLICATION_PREFIX_LENGTH = 12;

    private URI httpProxy;
    private AsciiString userAgentAscii;

    private NettyChannelPool sslChannelPool;
    private NettyChannelPool channelPool;
    private NettyChannelPool http2SslChannelPool;
    private NettyChannelPool http2ChannelPool;
    private SortedMap<Long, Operation> pendingRequests = new ConcurrentSkipListMap<>();

    private ScheduledExecutorService scheduledExecutor;

    private ExecutorService executor;

    private SslContext http2SslContext;
    private SSLContext sslContext;

    private ServiceHost host;

    private CookieJar cookieJar = new CookieJar();

    private boolean isStarted;

    private boolean warnHttp2DisablingConnectionSharing = false;

    private final Object startSync = new Object();

    public static ServiceClient create(String userAgent,
            ExecutorService executor,
            ScheduledExecutorService scheduledExecutor) throws URISyntaxException {
        return create(userAgent, executor, scheduledExecutor, null);
    }

    public static ServiceClient create(String userAgent,
            ExecutorService executor,
            ScheduledExecutorService scheduledExecutor,
            ServiceHost host) throws URISyntaxException {
        NettyHttpServiceClient sc = new NettyHttpServiceClient();
        sc.userAgentAscii = new AsciiString(userAgent);
        sc.scheduledExecutor = scheduledExecutor;
        sc.executor = executor;
        sc.host = host;
        sc.channelPool = new NettyChannelPool();
        sc.http2ChannelPool = new NettyChannelPool();
        sc.sslChannelPool = new NettyChannelPool();
        sc.http2SslChannelPool = new NettyChannelPool();
        String proxy = System.getenv(ENV_VAR_NAME_HTTP_PROXY);
        if (proxy != null) {
            sc.setHttpProxy(new URI(proxy));
        }

        sc.setPendingRequestQueueLimit(ServiceClient.DEFAULT_PENDING_REQUEST_QUEUE_LIMIT);
        sc.setConnectionLimitPerTag(ServiceClient.CONNECTION_TAG_DEFAULT,
                DEFAULT_CONNECTIONS_PER_HOST);
        sc.setConnectionLimitPerTag(ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT,
                DEFAULT_CONNECTION_LIMIT_PER_TAG);
        sc.setRequestPayloadSizeLimit(ServiceClient.REQUEST_PAYLOAD_SIZE_LIMIT);

        return sc;
    }

    private String buildThreadTag() {
        if (this.host != null) {
            return UriUtils.extendUri(this.host.getUri(), "netty-client").toString();
        }
        return getClass().getSimpleName() + ":" + Utils.getSystemNowMicrosUtc();
    }

    @Override
    public void start() {
        synchronized (this.startSync) {
            if (this.isStarted) {
                return;
            }
            this.isStarted = true;
        }

        if (NettyChannelContext.isALPNEnabled() && this.http2SslContext == null) {
            try {
                this.http2SslContext = SslContextBuilder.forClient()
                        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                        .applicationProtocolConfig(new ApplicationProtocolConfig(
                                ApplicationProtocolConfig.Protocol.ALPN,
                                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                                ApplicationProtocolNames.HTTP_2))
                        .build();
            } catch (SSLException e) {
                throw new RuntimeException("SSL context builder exception", e);
            }
        }

        this.channelPool.setThreadTag(buildThreadTag());
        this.channelPool.setThreadCount(Utils.DEFAULT_IO_THREAD_COUNT);
        this.channelPool.setExecutor(this.executor);
        this.channelPool.start();

        // We make a separate pool for HTTP/2. We want to have only one connection per host
        // when using HTTP/2 since HTTP/2 multiplexes streams on a single connection.
        this.http2ChannelPool.setThreadTag(buildThreadTag());
        this.http2ChannelPool.setThreadCount(Utils.DEFAULT_IO_THREAD_COUNT);
        this.http2ChannelPool.setExecutor(this.executor);
        this.http2ChannelPool.setHttp2Only();
        this.http2ChannelPool.start();

        if (this.sslContext != null) {
            this.sslChannelPool.setThreadTag(buildThreadTag());
            this.sslChannelPool.setThreadCount(Utils.DEFAULT_IO_THREAD_COUNT);
            this.sslChannelPool.setExecutor(this.executor);
            this.sslChannelPool.setSSLContext(this.sslContext);
            this.sslChannelPool.start();
        }

        if (this.http2SslContext != null) {
            this.http2SslChannelPool.setThreadTag(buildThreadTag());
            this.http2SslChannelPool.setThreadCount(Utils.DEFAULT_IO_THREAD_COUNT);
            this.http2SslChannelPool.setExecutor(this.executor);
            this.http2SslChannelPool.setHttp2Only();
            this.http2SslChannelPool.setHttp2SslContext(this.http2SslContext);
            this.http2SslChannelPool.start();
        }

        if (this.host != null) {
            MaintenanceProxyService.start(this.host, this::handleMaintenance);
        }
    }

    @Override
    public void stop() {
        // In practice, it's safe not to synchronize here, but this make Findbugs happy.
        synchronized (this.startSync) {
            if (this.isStarted == false) {
                return;
            }
            this.isStarted = false;
        }

        this.channelPool.stop();
        this.sslChannelPool.stop();
        this.http2ChannelPool.stop();
        this.pendingRequests.clear();
    }

    public ServiceClient setHttpProxy(URI proxy) {
        this.httpProxy = proxy;
        return this;
    }

    @Override
    public void send(Operation op) {
        sendRequest(op);
    }

    @Override
    public void sendRequest(Operation op) {
        if (!validateOperation(op)) {
            return;
        }

        Operation clone = op.clone();

        setExpiration(clone);
        setCookies(clone);

        OperationContext ctx = OperationContext.getOperationContext();

        try {
            // First attempt in process delivery to co-located host
            if (!op.isRemote()) {
                if (this.host != null && this.host.handleRequest(clone)) {
                    return;
                }
            }
            sendRemote(clone);
        } finally {
            // we must restore the operation context after each send, since
            // it can be reset by the host, depending on queuing and dispatching behavior
            OperationContext.restoreOperationContext(ctx);
        }
    }

    private void setExpiration(Operation op) {
        if (op.getExpirationMicrosUtc() != 0) {
            return;
        }
        long expMicros = this.host != null ? this.host.getOperationTimeoutMicros()
                : ServiceHostState.DEFAULT_OPERATION_TIMEOUT_MICROS;
        op.setExpiration(Utils.fromNowMicrosUtc(expMicros));
    }

    private void setCookies(Operation clone) {
        if (this.cookieJar.isEmpty()) {
            return;
        }
        // Set cookies for outbound request
        clone.setCookies(this.cookieJar.list(clone.getUri()));
    }

    private void startTracking(Operation op) {
        this.pendingRequests.put(op.getId(), op);
    }

    void stopTracking(Operation op) {
        this.pendingRequests.remove(op.getId());
    }

    private void updateCookieJarFromResponseHeaders(Operation op) {
        String value = op.getResponseHeaderAsIs(Operation.SET_COOKIE_HEADER);
        if (value == null) {
            return;
        }

        Cookie cookie = ClientCookieDecoder.LAX.decode(value);
        if (cookie == null) {
            return;
        }

        this.cookieJar.add(op.getUri(), cookie);
    }

    private void sendRemote(Operation op) {
        startTracking(op);

        // Determine the remote host address, port number
        // and uri scheme (http or https)
        String remoteHost = op.getUri().getHost();
        String scheme = op.getUri().getScheme();
        int port = op.getUri().getPort();

        if (this.httpProxy != null && !ServiceHost.LOCAL_HOST.equals(remoteHost)) {
            remoteHost = this.httpProxy.getHost();
            port = this.httpProxy.getPort();
            scheme = this.httpProxy.getScheme();
        }

        boolean isHttpScheme = false;
        boolean isHttpsScheme = scheme.equals(UriUtils.HTTPS_SCHEME);
        if (!isHttpsScheme) {
            isHttpScheme = scheme.equals(UriUtils.HTTP_SCHEME);
        }

        if (!isHttpScheme && !isHttpsScheme) {
            op.setRetryCount(0);
            fail(new IllegalArgumentException(
                    "Scheme is not supported: " + op.getUri().getScheme()), op, op.getBodyRaw());
            return;
        }

        if (isHttpsScheme && this.getSSLContext() == null) {
            op.setRetryCount(0);
            fail(new IllegalArgumentException(
                    "HTTPS not enabled, set SSL context before starting client:" + op.getUri()),
                    op, op.getBodyRaw());
            return;
        }

        // if there are no ports specified, choose the default ports http or https
        if (port == -1) {
            port = isHttpScheme ? UriUtils.HTTP_DEFAULT_PORT : UriUtils.HTTPS_DEFAULT_PORT;
        }

        if (op.isConnectionSharing() && isHttpsScheme && !NettyChannelContext.isALPNEnabled()) {
            op.setConnectionSharing(false);
            if (!this.warnHttp2DisablingConnectionSharing) {
                this.warnHttp2DisablingConnectionSharing = true;
                LOGGER.warning(
                        "HTTP/2 requests are not supported on HTTPS. Falling back to HTTP1.1");
            }
        }

        // Determine the channel pool used for this request.
        NettyChannelPool pool = this.channelPool;

        if (op.isConnectionSharing()) {
            if (isHttpsScheme) {
                pool = this.http2SslChannelPool;
            } else {
                pool = this.http2ChannelPool;
            }
        } else if (isHttpsScheme) {
            pool = this.sslChannelPool;
        }

        connectChannel(pool, op, remoteHost, port);
    }

    private void connectChannel(NettyChannelPool pool, Operation op,
            String remoteHost, int port) {
        op.nestCompletion((o, e) -> {
            if (o.getStatusCode() == Operation.STATUS_CODE_TIMEOUT) {
                failWithTimeout(op, op.getBodyRaw());
                return;
            }
            if (e != null) {
                Object originalBody = op.getBodyRaw();
                ServiceErrorResponse rsp = null;
                if (o.hasBody() && (o.getBodyRaw() instanceof ServiceErrorResponse)) {
                    rsp = (ServiceErrorResponse) o.getBodyRaw();
                    if (rsp.details == null) {
                        rsp.details = EnumSet.of(ErrorDetail.SHOULD_RETRY);
                    } else {
                        rsp.details.add(ErrorDetail.SHOULD_RETRY);
                    }
                } else {
                    rsp = ServiceErrorResponse.create(e, Operation.STATUS_CODE_BAD_REQUEST,
                            EnumSet.of(ErrorDetail.SHOULD_RETRY));
                }
                op.setBodyNoCloning(rsp);
                fail(e, op, originalBody);
                return;
            }
            sendHttpRequest(op);
        });

        NettyChannelGroupKey key = NettyChannelPool.buildLookupKey(
                op.getConnectionTag(), remoteHost, port, pool.isHttp2Only());
        pool.connectOrReuse(key, op);
    }

    private void sendHttpRequest(Operation op) {
        final Object originalBody = op.getBodyRaw();
        try {
            byte[] body = Utils.encodeBody(op);
            if (op.getContentLength() > getRequestPayloadSizeLimit()) {
                String error = String.format("Content length %d, limit is %d",
                        op.getContentLength(), getRequestPayloadSizeLimit());
                Exception e = new IllegalArgumentException(error);
                op.setBody(ServiceErrorResponse.create(e, Operation.STATUS_CODE_BAD_REQUEST));
                fail(e, op, originalBody);
                return;
            }

            String pathAndQuery;
            String path = op.getUri().getPath();
            String query = op.getUri().getRawQuery();
            String userInfo = op.getUri().getRawUserInfo();
            path = path == null || path.isEmpty() ? "/" : path;
            if (query != null) {
                pathAndQuery = path + "?" + query;
            } else {
                pathAndQuery = path;
            }

            boolean useHttp2 = op.isConnectionSharing();
            if (this.httpProxy != null || useHttp2 || userInfo != null) {
                pathAndQuery = op.getUri().toString();
            }

            NettyFullHttpRequest request = null;
            HttpMethod method = toHttpMethod(op.getAction());
            if (body == null || body.length == 0) {
                request = new NettyFullHttpRequest(HttpVersion.HTTP_1_1, method, pathAndQuery,
                        Unpooled.buffer(0), false);
            } else {
                ByteBuf content = Unpooled.wrappedBuffer(body, 0, (int) op.getContentLength());
                request = new NettyFullHttpRequest(HttpVersion.HTTP_1_1, method, pathAndQuery,
                        content, false);
            }

            HttpHeaders httpHeaders = request.headers();

            /**
             * NOTE: Pay close attention to calls that access the operation request headers, since
             * they will cause a memory allocation. We avoid the allocation by first checking if
             * the operation has any custom headers to begin with, then we check for the specific
             * header
             */
            boolean hasRequestHeaders = op.hasRequestHeaders();

            if (useHttp2) {
                // when operation is cloned, it may contain original streamId header. remove it.
                if (hasRequestHeaders) {
                    op.getAndRemoveRequestHeaderAsIs(Operation.STREAM_ID_HEADER);
                }
                // We set the operation so that once a streamId is assigned, we can record
                // the correspondence between the streamId and operation: this will let us
                // handle responses properly later.
                request.setOperation(op);
            }

            String pragmaValue = op.getAndRemoveRequestHeaderAsIs(Operation.PRAGMA_HEADER);
            String acceptValue = op.getAndRemoveRequestHeaderAsIs(Operation.ACCEPT_HEADER);
            String authTokenValue = op
                    .getAndRemoveRequestHeaderAsIs(Operation.REQUEST_AUTH_TOKEN_HEADER);

            if (op.isFromReplication() && pragmaValue == null) {
                httpHeaders.add(HttpHeaderNames.PRAGMA, PRAGMA_DIRECTIVE_REPLICATED_ASCII);
            } else if (pragmaValue != null) {
                httpHeaders.add(HttpHeaderNames.PRAGMA, pragmaValue);
            }

            if (op.getTransactionId() != null) {
                httpHeaders.add(TRANSACTION_ID_HEADER_ASCII, op.getTransactionId());
            }

            if (op.getContextId() != null) {
                httpHeaders.add(CONTEXT_ID_HEADER_ASCII, op.getContextId());
            }

            hasRequestHeaders = op.hasRequestHeaders();
            boolean isXenonToXenon = op.isFromReplication() || op.isForwarded();
            if (hasRequestHeaders) {
                // remove all headers that we will set explicitly
                op.getAndRemoveRequestHeaderAsIs(Operation.CONTENT_LENGTH_HEADER);
                op.getAndRemoveRequestHeaderAsIs(Operation.CONTENT_TYPE_HEADER);
                op.getAndRemoveRequestHeaderAsIs(Operation.CONNECTION_HEADER);
                op.getAndRemoveRequestHeaderAsIs(Operation.REFERER_HEADER);
                op.getAndRemoveRequestHeaderAsIs(Operation.HOST_HEADER);
                op.getAndRemoveRequestHeaderAsIs(Operation.USER_AGENT_HEADER);
                hasRequestHeaders = op.hasRequestHeaders();
            }

            if (hasRequestHeaders) {
                for (Entry<String, String> nameValue : op.getRequestHeaders().entrySet()) {
                    httpHeaders.add(nameValue.getKey(), nameValue.getValue());
                }
            }

            if (authTokenValue != null) {
                httpHeaders.add(AUTH_TOKEN_HEADER_ASCII, authTokenValue);
            } else {
                AuthorizationContext ctx = op.getAuthorizationContext();
                if (ctx != null && ctx.getToken() != null) {
                    httpHeaders.add(AUTH_TOKEN_HEADER_ASCII, ctx.getToken());
                }
            }

            setAsciiContentType(op, httpHeaders);

            httpHeaders.add(HttpHeaderNames.CONTENT_LENGTH,
                    Long.toString(op.getContentLength()));

            if (op.isKeepAlive()) {
                httpHeaders.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            if (op.hasReferer() && !op.isFromReplication()) {
                httpHeaders.add(HttpHeaderNames.REFERER, op.getRefererAsString());
            }

            if (!isXenonToXenon) {
                if (op.getCookies() != null) {
                    String header = CookieJar.encodeCookies(op.getCookies());
                    httpHeaders.set(HttpHeaderNames.COOKIE, header);
                }

                request.headers().set(HttpHeaderNames.USER_AGENT, this.userAgentAscii);
                if (acceptValue == null) {
                    httpHeaders.add(HttpHeaderNames.ACCEPT, DEFAULT_MEDIA_TYPE_ASCII);
                } else {
                    httpHeaders.add(HttpHeaderNames.ACCEPT, acceptValue);
                }

                httpHeaders.add(HttpHeaderNames.HOST, op.getUri().getHost());
            } else {
                if (acceptValue != null) {
                    httpHeaders.add(HttpHeaderNames.ACCEPT, acceptValue);
                }
            }

            if (LOGGER.isLoggable(Level.FINEST)) {
                logRequestFraming(op, request);
            }

            boolean doCookieJarUpdate = !isXenonToXenon;
            op.nestCompletion((o, e) -> {
                if (e != null) {
                    fail(e, op, originalBody);
                    return;
                }

                stopTracking(op);

                if (doCookieJarUpdate) {
                    updateCookieJarFromResponseHeaders(o);
                }

                // After request is sent control is transferred to the
                // NettyHttpServerResponseHandler. The response handler will nest completions
                // and call complete() when response is received, which will invoke this completion
                op.complete();
            });

            op.toggleOption(OperationOption.SOCKET_ACTIVE, true);
            op.getSocketContext().writeHttpRequest(request);
        } catch (Throwable e) {
            op.setBody(ServiceErrorResponse.create(e, Operation.STATUS_CODE_BAD_REQUEST,
                    EnumSet.of(ErrorDetail.SHOULD_RETRY)));
            fail(e, op, originalBody);
        }
    }

    private void setAsciiContentType(Operation op, HttpHeaders httpHeaders) {
        String contentType = op.getContentType();
        /*
         * Optimization that saves char sequence to byte[] conversion on the content type, which
         * is non trivial overhead on small HTTP requests.
         * We first check for a specific char at a index in the string, then the hash code to nearly
         * eliminate the false positives from hash code alone.
         */
        if (contentType.length() >= MEDIA_TYPE_APPLICATION_PREFIX_LENGTH
                && contentType.charAt(MEDIA_TYPE_APPLICATION_PREFIX_LENGTH) == 'k'
                && Operation.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM.hashCode() == contentType
                        .hashCode()) {
            httpHeaders.add(HttpHeaderNames.CONTENT_TYPE, MEDIA_TYPE_KRYO_OCTET_STREAM_ASCII);
        } else if (contentType.length() >= MEDIA_TYPE_APPLICATION_PREFIX_LENGTH
                && contentType.charAt(MEDIA_TYPE_APPLICATION_PREFIX_LENGTH) == 'j'
                && Operation.MEDIA_TYPE_APPLICATION_JSON.hashCode() == contentType.hashCode()) {
            httpHeaders.add(HttpHeaderNames.CONTENT_TYPE, MEDIA_TYPE_JSON_ASCII);
        } else {
            httpHeaders.add(HttpHeaderNames.CONTENT_TYPE, op.getContentType());
        }
    }

    private void logRequestFraming(Operation op, NettyFullHttpRequest request) {
        StringBuilder s = new StringBuilder();
        s.append(op.getAction().toString())
                .append(" ")
                .append(op.getUri().toString())
                .append("\n");
        request.headers().forEach((e) -> {
            s.append(e.toString()).append("\n");
        });
        LOGGER.info(s.toString());
    }

    private static HttpMethod toHttpMethod(Action a) {
        switch (a) {
        case DELETE:
            return HttpMethod.DELETE;
        case GET:
            return HttpMethod.GET;
        case OPTIONS:
            return HttpMethod.OPTIONS;
        case PATCH:
            return HttpMethod.PATCH;
        case POST:
            return HttpMethod.POST;
        case PUT:
            return HttpMethod.PUT;
        default:
            throw new IllegalArgumentException("unknown method " + a);

        }
    }

    private void failWithTimeout(Operation op, Object originalBody) {
        Throwable e = new TimeoutException(op.getUri() + ":" + op.getExpirationMicrosUtc());
        op.setStatusCode(Operation.STATUS_CODE_TIMEOUT);
        fail(e, op, originalBody);
    }

    private void fail(Throwable e, Operation op, Object originalBody) {
        stopTracking(op);
        SocketContext ctx = op.getSocketContext();
        if (ctx != null && ctx instanceof NettyChannelContext) {
            NettyChannelContext nettyCtx = (NettyChannelContext) op.getSocketContext();
            NettyChannelPool pool;
            Runnable r = null;
            if (nettyCtx.getProtocol() == NettyChannelContext.Protocol.HTTP2) {
                nettyCtx.removeStreamForOperation(op);
                if (this.http2SslChannelPool != null
                        && this.http2SslChannelPool.isContextInUse(nettyCtx)) {
                    pool = this.http2SslChannelPool;
                } else {
                    pool = this.http2ChannelPool;
                }
                // For HTTP/2, we maintain multiple streams, so we don't close the connection.
                r = () -> pool.returnOrClose(nettyCtx, false);
            } else {
                op.setSocketContext(null);
                if (this.sslChannelPool != null
                        && this.sslChannelPool.isContextInUse(nettyCtx)) {
                    pool = this.sslChannelPool;
                } else {
                    pool = this.channelPool;
                }
                r = () -> pool.returnOrClose(nettyCtx, !op.isKeepAlive());
            }

            ExecutorService exec = this.host != null ? this.host.getExecutor() : this.executor;
            if (exec != null) {
                exec.execute(r);
            } else {
                r.run();
            }
        }

        if (this.scheduledExecutor.isShutdown()) {
            op.fail(new CancellationException());
            return;
        }

        boolean isRetryRequested = op.getRetryCount() > 0 && op.decrementRetriesRemaining() >= 0;

        if (isRetryRequested) {
            if (op.getStatusCode() >= Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD) {
                isRetryRequested = false;
            } else if (op.getStatusCode() == Operation.STATUS_CODE_CONFLICT) {
                isRetryRequested = false;
            } else if (op.getStatusCode() == Operation.STATUS_CODE_UNAUTHORIZED) {
                isRetryRequested = false;
            } else if (op.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                isRetryRequested = false;
            } else if (op.getStatusCode() == Operation.STATUS_CODE_FORBIDDEN) {
                isRetryRequested = false;
            }
        }

        if (!isRetryRequested) {
            LOGGER.fine(() -> String.format("Send of %d, from %s to %s failed with %s",
                    op.getId(), op.getRefererAsString(), op.getUri(), e));
            op.fail(e);
            return;
        }

        LOGGER.info(String.format("Retry %d of request %d from %s to %s due to %s",
                op.getRetryCount() - op.getRetriesRemaining(), op.getId(), op.getRefererAsString(),
                op.getUri(), e));

        int delaySeconds = op.getRetryCount() - op.getRetriesRemaining();

        // restore status code and body, then restart send state machine
        // (connect, encode, write to channel)
        op.setStatusCode(Operation.STATUS_CODE_OK).setBodyNoCloning(originalBody);

        this.scheduledExecutor.schedule(() -> {
            startTracking(op);
            sendRemote(op);
        }, delaySeconds, TimeUnit.SECONDS);
    }

    private static boolean validateOperation(Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("Operation is required");
        }

        Throwable e = null;
        if (op.getUri() == null) {
            e = new IllegalArgumentException("Uri is required");
        } else if (op.getUri().getHost() == null) {
            e = new IllegalArgumentException("Missing host in URI");
        } else if (op.getAction() == null) {
            e = new IllegalArgumentException("Action is required");
        } else if (!op.hasReferer()) {
            e = new IllegalArgumentException("Referer is required");
        } else {
            boolean needsBody = op.getAction() != Action.GET && op.getAction() != Action.DELETE &&
                    op.getAction() != Action.POST && op.getAction() != Action.OPTIONS;
            if (!op.hasBody() && needsBody) {
                e = new IllegalArgumentException("Body is required");
            }
        }

        if (e == null) {
            return true;
        }

        CompletionHandler c = op.getCompletion();
        if (c != null) {
            c.handle(op, e);
            return false;
        }
        throw new RuntimeException(e);
    }

    @Override
    public void handleMaintenance(Operation op) {
        long now = Utils.getSystemNowMicrosUtc();
        if (this.sslChannelPool != null) {
            this.sslChannelPool.handleMaintenance(Operation.createPost(op.getUri()));
        }
        if (this.http2ChannelPool != null) {
            this.http2ChannelPool.handleMaintenance(Operation.createPost(op.getUri()));
        }
        if (this.http2SslChannelPool != null) {
            this.http2SslChannelPool.handleMaintenance(Operation.createPost(op.getUri()));
        }
        this.channelPool.handleMaintenance(Operation.createPost(op.getUri()));

        failExpiredRequests(now);
        op.complete();
    }

    /**
     * Periodically check our pending request sorted map for expired operations. Since
     * maintenance (this method) runs in parallel with the connect and send state machine,
     * we need to be careful on how we fail expired operations. The connect() method
     * uses nestCompletion() which is meant to be used in a asynchronous, but isolated
     * flow over a single operation, where only one stage acts on the operation at a time.
     * We violate this design requirement, since we want to avoid locks, and we want to
     * leverage the maintenance interval. So, we run two passes, first marking the operation
     * with a status code, allowing the parallel connect/send pipeline to avoid triggering
     * a complete() and non atomic roll back of the nested completions.
     */
    private void failExpiredRequests(long now) {
        if (this.pendingRequests.isEmpty()) {
            return;
        }


        // We do a limited search of pending operation, in each maintenance period, to
        // determine if any have expired. The operations are kept in a sorted map,
        // with the key being the operation id. The operation id increments monotonically
        // so we are effectively traversing oldest to newest. We can not assume if operation
        // with id k, with k << n, has expired, also operation with id n has, since operations
        // have different expirations. We limit the search so we don't take too much time when
        // millions of operations are pending
        final int searchLimit = 1000;
        final long epsilonMicros = TimeUnit.SECONDS.toMicros(1);
        int expiredCount = 0;
        int forcedExpiredCount = 0;
        int i = 0;
        for (Operation o : this.pendingRequests.values()) {
            if (i++ >= searchLimit) {
                break;
            }
            long exp = o.getExpirationMicrosUtc();

            if (exp > now) {
                continue;
            }

            // Bad HTTP/2 connections will not close until idle detection kicks in, which can
            // be much longer than operation expiration. We force expiration even if the operation
            // has not been written to the channel, if it has expired and some additional time has
            // passed.
            boolean forceExpiration = o.hasOption(OperationOption.CONNECTION_SHARING) &&
                    now - exp > epsilonMicros;

            if (!forceExpiration && !o.hasOption(OperationOption.SOCKET_ACTIVE)) {
                continue;
            }
            o.fail(Operation.STATUS_CODE_TIMEOUT);
            expiredCount++;
            if (forceExpiration) {
                forcedExpiredCount++;
            }
        }

        if (expiredCount == 0) {
            return;
        }
        LOGGER.info("Failed expired operations, count: " + expiredCount + " forced count: "
                + forcedExpiredCount);
    }

    /**
     * @see ServiceClient#setPendingRequestQueueLimit(int)
     */
    @Override
    public ServiceClient setPendingRequestQueueLimit(int limit) {
        this.channelPool.setPendingRequestQueueLimit(limit);
        if (this.sslChannelPool != null) {
            this.sslChannelPool.setPendingRequestQueueLimit(limit);
        }
        if (this.http2ChannelPool != null) {
            this.http2ChannelPool.setPendingRequestQueueLimit(limit);
        }
        if (this.http2SslChannelPool != null) {
            this.http2SslChannelPool.setPendingRequestQueueLimit(limit);
        }
        return this;
    }

    /**
     * @see ServiceClient#getPendingRequestQueueLimit()
     */
    @Override
    public int getPendingRequestQueueLimit() {
        return this.channelPool.getPendingRequestQueueLimit();
    }

    /**
     * @see ServiceClient#setConnectionLimitPerTag(String, int)
     */
    @Override
    public ServiceClient setConnectionLimitPerTag(String tag, int limit) {
        this.channelPool.setConnectionLimitPerTag(tag, limit);
        if (this.sslChannelPool != null) {
            this.sslChannelPool.setConnectionLimitPerTag(tag, limit);
        }
        if (this.http2ChannelPool != null) {
            this.http2ChannelPool.setConnectionLimitPerTag(tag, limit);
        }
        if (this.http2SslChannelPool != null) {
            this.http2SslChannelPool.setConnectionLimitPerTag(tag, limit);
        }
        return this;
    }

    /**
     * @see ServiceClient#getConnectionLimitPerTag(String)
     */
    @Override
    public int getConnectionLimitPerTag(String tag) {
        return this.channelPool.getConnectionLimitPerTag(tag);
    }

    public ServiceClient setHttp2SslContext(SslContext context) {
        this.http2SslContext = context;
        return this;
    }

    public SslContext getHttp2SslContext() {
        return this.http2SslContext;
    }

    @Override
    public ServiceClient setSSLContext(SSLContext context) {
        this.sslContext = context;
        return this;
    }

    @Override
    public SSLContext getSSLContext() {
        return this.sslContext;
    }

    public NettyChannelPool getChannelPool() {
        return this.channelPool;
    }

    public NettyChannelPool getHttp2ChannelPool() {
        return this.http2ChannelPool;
    }

    public NettyChannelPool getSslChannelPool() {
        return this.sslChannelPool;
    }

    public NettyChannelContext getInUseHttp2SslContext(String tag, String host, int port) {
        if (this.http2SslChannelPool == null) {
            throw new IllegalStateException("Internal error: No HTTP/2 SSL channel pool");
        }
        return this.http2SslChannelPool.getFirstValidHttp2Context(tag, host, port);
    }

    /**
     * Find the HTTP/2 context that is currently being used to talk to a given host.
     * This is intended for infrastructure test purposes.
     */
    public NettyChannelContext getInUseHttp2Context(String tag, String host, int port) {
        if (this.http2ChannelPool == null) {
            throw new IllegalStateException("Internal error: no HTTP/2 channel pool");
        }
        return this.http2ChannelPool.getFirstValidHttp2Context(tag, host, port);
    }

    @Override
    public ConnectionPoolMetrics getConnectionPoolMetrics(String tag) {
        if (tag == null) {
            throw new IllegalArgumentException("tag is required");
        }

        ConnectionPoolMetrics tagInfo = null;
        if (this.http2ChannelPool != null) {
            tagInfo = this.http2ChannelPool.getConnectionTagInfo(tag);
        }
        ConnectionPoolMetrics secureTagInfo = null;
        if (this.http2SslChannelPool != null) {
            secureTagInfo = this.http2SslChannelPool.getConnectionTagInfo(tag);
        }

        if (tagInfo == null) {
            tagInfo = secureTagInfo;
        } else if (secureTagInfo != null) {
            tagInfo.inUseConnectionCount += secureTagInfo.inUseConnectionCount;
            tagInfo.pendingRequestCount += secureTagInfo.pendingRequestCount;
            tagInfo.availableConnectionCount += secureTagInfo.availableConnectionCount;
        }

        if (tagInfo != null) {
            return tagInfo;
        }

        if (this.channelPool != null) {
            tagInfo = this.channelPool.getConnectionTagInfo(tag);
        }
        if (this.sslChannelPool != null) {
            secureTagInfo = this.sslChannelPool.getConnectionTagInfo(tag);
        }

        if (tagInfo == null) {
            tagInfo = secureTagInfo;
        } else if (secureTagInfo != null) {
            tagInfo.inUseConnectionCount += secureTagInfo.inUseConnectionCount;
            tagInfo.pendingRequestCount += secureTagInfo.pendingRequestCount;
            tagInfo.availableConnectionCount += secureTagInfo.availableConnectionCount;
        }

        return tagInfo;
    }

    /**
     * Count how many HTTP/2 contexts we have. There may be more than one if we have
     * an exhausted connection that hasn't been cleaned up yet.
     * This is intended for infrastructure test purposes.
     */
    public int getInUseContextCount(String tag, String host, int port) {
        if (this.http2ChannelPool == null) {
            throw new IllegalStateException("Internal error: no HTTP/2 channel pool");
        }
        return this.http2ChannelPool.getHttp2ActiveContextCount(tag, host, port);
    }

    /**
     * Infrastructure testing use only: do not use this in production
     */
    public void clearCookieJar() {
        this.cookieJar = new CookieJar();
    }

    @Override
    public int getRequestPayloadSizeLimit() {
        return this.channelPool.getRequestPayloadSizeLimit();
    }

    @Override
    public ServiceClient setRequestPayloadSizeLimit(int limit) {
        synchronized (this.startSync) {
            if (this.isStarted) {
                throw new IllegalStateException("Already started");
            }

            this.channelPool.setRequestPayloadSizeLimit(limit);
            if (this.sslChannelPool != null) {
                this.sslChannelPool.setRequestPayloadSizeLimit(limit);
            }
            if (this.http2ChannelPool != null) {
                this.http2ChannelPool.setRequestPayloadSizeLimit(limit);
            }
            if (this.http2SslChannelPool != null) {
                this.http2SslChannelPool.setRequestPayloadSizeLimit(limit);
            }
        }

        return this;
    }
}
