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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Level;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.WebSocketService;


public class NettyWebSocketRequestHandler extends SimpleChannelInboundHandler<Object> {
    public static class CreateServiceResponse {
        public String uri;
    }

    private WebSocketServerHandshaker handshaker;
    private final ConcurrentMap<URI, Set<String>> serviceSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<URI, WebSocketService> webSocketServices = new ConcurrentHashMap<>();
    private ServiceHost host;

    private String handshakePath;
    private String servicePrefix;

    public NettyWebSocketRequestHandler(ServiceHost host, String socketHandshakePath,
            String servicePrefix) {
        this.host = host;
        this.handshakePath = socketHandshakePath;
        this.servicePrefix = servicePrefix;
    }

    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest nettyRequest = (FullHttpRequest) msg;
            return nettyRequest.uri().contentEquals(this.handshakePath);
        }
        if (msg instanceof WebSocketFrame) {
            return true;
        }
        return false;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest nettyRequest = (FullHttpRequest) msg;
            performWebsocketHandshake(ctx, nettyRequest);
            return;
        }

        if (msg instanceof WebSocketFrame) {
            processWebSocketFrame(ctx, (WebSocketFrame) msg);
            return;
        }
    }

    private void performWebsocketHandshake(final ChannelHandlerContext ctx,
            FullHttpRequest nettyRequest) {
        WebSocketServerHandshakerFactory factory =
                new WebSocketServerHandshakerFactory(this.handshakePath, null, false);
        this.handshaker = factory.newHandshaker(nettyRequest);
        if (this.handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
        } else {
            ChannelPromise promise = new DefaultChannelPromise(ctx.channel());
            promise.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        ctx.channel().close();
                    }
                    ctx.channel()
                            .closeFuture()
                            .addListener(f -> {
                                for (java.util.Map.Entry<URI, Set<String>> e :
                                        NettyWebSocketRequestHandler.this.serviceSubscriptions
                                                .entrySet()) {
                                    WebSocketService svc = NettyWebSocketRequestHandler.this.webSocketServices
                                            .get(e.getKey());
                                    if (svc != null) {
                                        deleteServiceSubscriptions(svc);
                                    }
                                    NettyWebSocketRequestHandler.this.host.stopService(svc);
                                }
                            });
                }
            });
            DefaultHttpHeaders responseHeaders = new DefaultHttpHeaders();
            this.handshaker.handshake(ctx.channel(), nettyRequest, responseHeaders, promise);
        }
    }

    private void deleteServiceSubscriptions(WebSocketService service) {
        Set<String> subscriptions = this.serviceSubscriptions.remove(service.getUri());
        ServiceSubscriptionState.ServiceSubscriber body =
                new ServiceSubscriptionState.ServiceSubscriber();
        body.reference = service.getUri();
        for (String unsubscribeFrom : subscriptions) {
            this.host.sendRequest(Operation
                    .createDelete(service, unsubscribeFrom)
                    .setBody(body).setReferer(service.getUri()));
        }
    }

    /**
     * Processes incoming web socket frame. {@link PingWebSocketFrame} and {@link CloseWebSocketFrame} frames are
     * processed in a usual way. {@link io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame} frames are not
     * supported. {@link TextWebSocketFrame} frames are processed as described below.
     * <p/>
     * Whenever invalid frame is encountered - the underlying connection is closed.
     * <p/>
     * <h3>Incoming frame format</h3>
     * Incoming frame format is the same for all frames:
     * <pre>
     * REQUEST_ID
     * METHOD URI
     * {jsonPayload} (Optional and may be multiline)
     * </pre>
     * REQUEST_ID is an arbitrary string generated on client which is used to correlate server response with
     * original client request. This string is included into response frames (described below). REQUEST_ID should
     * be unique within the same web socket connection.
     * <p/>
     * METHOD is one of [POST, DELETE, REPLY] and URI is service path, such as {@code /core/ws-service}.
     * <p/>
     * Web socket connection is not a proxy, so arbitrary methods and request URIs are not supported.
     * <p/>
     * Line breaks are always CRLF similar to HTTP
     * (<a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec2.html#sec2.2">RFC 2616</a>).
     * <p/>
     * <h3>Outgoing frame format</h3>
     * {@link io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame} frames are never sent to client.
     * Text frames have the following format:
     * <pre>
     * (RESULT_CODE REQUEST_ID (optional line))|(HTTP_METHOD SERVICE_URI)
     * {jsonPayload}
     * </pre>
     * RESULT_CODE is one of 200, 404, 500 with similar meanings to corresponding HTTP codes (OK, NOT_FOUND, ISE).
     * <p/>
     * REQUEST_ID is the same string which was passed by client in initial request.
     * <p/>
     * SERVICE_URI - a URI assigned to the client-side service.
     * <p/>
     * Json payload is either server response on request (in case when first line is RESULT_CODE REQUEST_ID) or
     * {@link com.vmware.xenon.common.Operation.SerializedOperation} in case when this frame is an operation to be
     * complete by a client service and first line is HTTP_METHOD SERVICE_URI. Method name corresponds to
     * {@link com.vmware.xenon.common.Operation.SerializedOperation#action} specified in the serialized operation.
     * <h3>Possible incoming request</h3>
     * All below requests should conform spec above. REQUEST_ID is omitted everywhere below to simplify the doc.
     * <ul>
     * <li>POST /core/ws-service with no body - requests a new service to be created. Response body is
     * {"uri": "http://some-ip-addr/core/ws-service/some-uuid"} where uri value is assigned temporary link to the
     * service. <b>ToDo: make node IP addresses invisible to client</b></li>
     * <li>DELETE /core/ws-service/some-uuid with no body - removes previously created web socket service.
     * Service should be defined in the same web socket connection.</li>
     * <li>POST /someservicepath/subscriptions with standard subscription body - subscribes the specified
     * service to the specified target service. Observer must be a web socket-based serviced created within
     * current connection. Subscription is removed automatically whenever web socket connection is broken.</li>
     * <li>DELETE /someservicepath/subscriptions with standard subscription body - removes previously created
     * subscription. Subscription should be made via request specified above within the same connection</li>
     * <li>REPLY /core/ws-service with serialized operation as body - should be issued in response for an
     * incoming request. No response from the server is assumed and REQUEST_ID field is ignored.
     * </li>
     * </ul>
     *
     * @param ctx   Netty channel context handler
     * @param frame Incoming websocket frame
     */
    private void processWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (frame instanceof CloseWebSocketFrame) {
            this.handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
            return;
        }
        if (frame instanceof PingWebSocketFrame) {
            ctx.channel().writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
            return;
        }
        if (!(frame instanceof TextWebSocketFrame)) {
            this.handshaker.close(
                    ctx.channel(),
                    new CloseWebSocketFrame(1003, String.format(
                            "%s frame types not supported", frame.getClass()
                                    .getName())));
            return;
        }
        TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
        String text = textFrame.text();
        int requestIdSep = text.indexOf(Operation.CR_LF);
        if (requestIdSep < 0) {
            this.handshaker.close(ctx.channel(),
                    new CloseWebSocketFrame(1003, "Malformed frame"));
            return;
        }
        String requestId = text.substring(0, requestIdSep);
        int requestLineSep = text.indexOf(Operation.CR_LF, requestIdSep + Operation.CR_LF.length());
        String body;
        if (requestLineSep < 0) {
            requestLineSep = text.length();
            body = "";
        } else {
            body = text.substring(requestLineSep + Operation.CR_LF.length());
        }
        String requestLine = text
                .substring(requestIdSep + Operation.CR_LF.length(), requestLineSep);
        int methodSep = requestLine.indexOf(" ");
        if (methodSep < 0) {
            this.handshaker.close(ctx.channel(),
                    new CloseWebSocketFrame(1003, "Malformed frame"));
            return;
        }
        String method = requestLine.substring(0, methodSep);
        String path = requestLine.substring(methodSep + 1);

        try {
            if (method.equals("DELETE")) {
                if (path.startsWith(this.servicePrefix)) {
                    // Shutdown service permanently and delete all known service subscriptions
                    URI serviceToDelete = UriUtils.buildPublicUri(this.host, path);
                    WebSocketService removed = this.webSocketServices.remove(serviceToDelete);
                    if (removed != null) {
                        deleteServiceSubscriptions(removed);
                        this.host.stopService(removed);
                        ctx.writeAndFlush(new TextWebSocketFrame("200 " + requestId));
                    } else {
                        ctx.writeAndFlush(new TextWebSocketFrame("404 " + requestId));
                    }
                    return;
                }
                if (path.endsWith(ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS)) {
                    // Delete a single subscription
                    ServiceSubscriptionState.ServiceSubscriber state = Utils.fromJson(body,
                            ServiceSubscriptionState.ServiceSubscriber.class);
                    WebSocketService service = this.webSocketServices.get(state.reference);
                    this.host.sendRequest(Operation
                            .createDelete(service, path)
                            .setBody(body)
                            .setReferer(
                                    service.getUri())
                            .setCompletion(
                                    (completedOp, failure) -> {
                                        ctx.writeAndFlush(new TextWebSocketFrame(completedOp
                                                .getStatusCode() + " " + requestId));
                                        Utils.atomicGetOrCreate(this.serviceSubscriptions,
                                                service.getUri(), ConcurrentSkipListSet::new)
                                                .remove(path);
                                    }));
                    return;
                }
                ctx.writeAndFlush(new TextWebSocketFrame(Integer
                        .toString(Operation.STATUS_CODE_NOT_FOUND) + " " + requestId));
                return;
            }
            if (method.equals(Action.POST.toString())) {
                if (path.equals(this.servicePrefix)) {
                    // Create a new ephemeral service
                    URI wsServiceUri = buildWsServiceUri(java.util.UUID.randomUUID().toString());
                    CreateServiceResponse response = new CreateServiceResponse();
                    response.uri = wsServiceUri.toString();
                    WebSocketService webSocketService = new WebSocketService(ctx, wsServiceUri);
                    this.host
                            .startService(
                                    Operation
                                            .createPost(wsServiceUri)
                                            .setCompletion(
                                                    (o, t) -> {
                                                        if (t != null) {
                                                            ctx.writeAndFlush(new TextWebSocketFrame(
                                                                    Integer.toString(Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD)
                                                                            + " "
                                                                            + requestId
                                                                            + Operation.CR_LF
                                                                            + Utils.toJson(t)));
                                                        } else {
                                                            ctx.writeAndFlush(new TextWebSocketFrame(
                                                                    Integer.toString(Operation.STATUS_CODE_ACCEPTED)
                                                                            + " "
                                                                            + requestId
                                                                            + Operation.CR_LF
                                                                            + Utils.toJson(response)));
                                                        }
                                                    }), webSocketService);
                    this.webSocketServices.put(wsServiceUri, webSocketService);
                    return;
                }
                if (path.endsWith(ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS)) {
                    // Subscribe for service updates with auto-unsubscribe
                    ServiceSubscriptionState.ServiceSubscriber state = Utils.fromJson(body,
                            ServiceSubscriptionState.ServiceSubscriber.class);
                    WebSocketService service = this.webSocketServices.get(state.reference);
                    this.host.sendRequest(Operation
                            .createPost(service, path)
                            .setBody(body)
                            .setReferer(
                                    service.getUri())
                            .setCompletion(
                                    (completedOp, failure) -> {
                                        ctx.writeAndFlush(new TextWebSocketFrame(completedOp
                                                .getStatusCode() + " " + requestId));
                                        if (completedOp.getStatusCode() >= 200
                                                && completedOp.getStatusCode() < 300) {
                                            Utils.atomicGetOrCreate(this.serviceSubscriptions,
                                                    service.getUri(), ConcurrentSkipListSet::new)
                                                    .add(path);
                                        }
                                    }));
                    return;
                }
            }
            if (method.equals("REPLY")) {
                if (path.startsWith(this.servicePrefix)
                        && path.length() > this.servicePrefix.length()) {
                    // Forward ephemeral service response to the caller
                    String serviceId = path.substring(this.servicePrefix.length() + 1);
                    URI serviceUri = buildWsServiceUri(serviceId);
                    WebSocketService service = this.webSocketServices.get(serviceUri);
                    if (service != null) {
                        service.handleWebSocketMessage(body);
                    }
                    return;
                }
            }
            ctx.writeAndFlush(new TextWebSocketFrame("404 " + requestId));
            this.host
                    .log(Level.FINE, "Unsupported websocket request: %s %s %s", method, path, body);
        } catch (Exception e) {
            ctx.writeAndFlush("500 " + requestId);
        }
    }

    /**
     * Builds public ephemeral web socket-based service URI based on service id.
     *
     * @param serviceId Service ID.
     * @return Service URI.
     */
    private URI buildWsServiceUri(String serviceId) {
        return UriUtils.buildPublicUri(this.host,
                UriUtils.buildUriPath(this.servicePrefix, serviceId));
    }

}