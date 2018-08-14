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

import static java.lang.String.format;

import java.net.HttpURLConnection;
import java.net.URI;
import java.security.Principal;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import javax.security.cert.X509Certificate;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.ServiceHost.ServiceNotFoundException;
import com.vmware.xenon.common.serialization.KryoSerializers;
import com.vmware.xenon.services.common.GuestUserService;
import com.vmware.xenon.services.common.QueryFilter;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.SystemUserService;

/**
 * Service operation container. Encapsulates the request / response pattern of client to service and
 * service to service asynchronous communication
 */
public class Operation implements Cloneable {

    /**
     * Portion of serialized JSON body string to include in {@code toString}
     */
    private static final int TO_STRING_SERIALIZED_BODY_LIMIT = 256;

    @FunctionalInterface
    public interface CompletionHandler {
        void handle(Operation completedOp, Throwable failure);
    }

    public static class SocketContext {
        private long lastUseTimeMicros;

        public long getLastUseTimeMicros() {
            return this.lastUseTimeMicros;
        }

        public void updateLastUseTime() {
            this.lastUseTimeMicros = Utils.getSystemNowMicrosUtc();
        }

        public void writeHttpRequest(Object request) {
            throw new IllegalStateException();
        }

        public void close() {
            throw new IllegalStateException();
        }
    }

    static class InstrumentationContext {
        long handleInvokeTimeMicros;
        long enqueueTimeMicros;
        long documentStoreCompletionTimeMicros;
        long handlerCompletionTimeMicros;
        long operationCompletionTimeMicros;
    }

    /**
     * Operation metadata being sent to the transaction coordinator.
     */
    public static class TransactionContext {

        /**
         * Action the service received
         */
        public Action action;

        /**
         * Set of pending transactions on the same service
         */
        public Set<String> coordinatorLinks;

        /**
         * Notify whether the service completed (true) or failed (false) the operation
         */
        public boolean isSuccessful;
    }

    static class RemoteContext {
        SocketContext socketCtx;
        Map<String, String> requestHeaders;
        Map<String, String> responseHeaders;
        Principal peerPrincipal;
        X509Certificate[] peerCertificateChain;
        String connectionTag;
        Map<String, String> cookies;
    }

    /**
     * An operation's authorization context.
     *
     * The {@link Claims} in this context was originally set by an authentication
     * if the operation originated from a remote client and the claims were encoded
     * in a token. If the operation is an internal derivative of such an operation,
     * the authorization context is inherited so that the claims object doesn't
     * need to be deserialized multiple times.
     */
    public static final class AuthorizationContext {

        /**
         * Set of claims for this authorization context.
         */
        private Claims claims;

        /**
         * Token representation for this set of claims.
         *
         * This field is only kept in this field so that we don't have to recreate the token
         * when this context is inherited and used for a request to a peer node.
         */
        private String token;

        /**
         * Whether this context should propagate to a client or not.
         *
         * If it is, the transport layer will propagate the context back to the client.
         * In the case of netty/http, it will add a Set-Cookie header for the token.
         */
        private boolean propagateToClient = false;

        /**
         * The resource query is a composite query constructed by grouping all
         * resource group queries that apply to this user's authorization context.
         */
        private Map<Action, Query> resourceQueryMap = null;

        /**
         * The resource query filter is a query filter of the composite query
         * constructed by grouping all resource group queries that apply to
         * this user's authorization context.
         */
        private Map<Action, QueryFilter> resourceQueryFiltersMap = null;

        public Claims getClaims() {
            return this.claims;
        }

        public String getToken() {
            return this.token;
        }

        public boolean shouldPropagateToClient() {
            return this.propagateToClient;
        }

        public Query getResourceQuery(Action action) {
            if (this.resourceQueryMap == null) {
                return null;
            }
            return Utils.clone(this.resourceQueryMap.get(action));
        }

        public QueryFilter getResourceQueryFilter(Action action) {
            if (this.resourceQueryFiltersMap == null) {
                return null;
            }
            return this.resourceQueryFiltersMap.get(action);
        }

        public boolean isSystemUser() {
            return isUserSubject(SystemUserService.SELF_LINK);
        }

        public boolean isGuestUser() {
            return isUserSubject(GuestUserService.SELF_LINK);
        }

        private boolean isUserSubject(String userLink) {
            Claims claims = getClaims();
            if (claims == null) {
                return false;
            }
            String subject = claims.getSubject();
            if (subject == null) {
                return false;
            }
            return subject.equals(userLink);
        }


        public static class Builder {
            private AuthorizationContext authorizationContext;

            public static Builder create() {
                return new Builder();
            }

            private Builder() {
                initialize();
            }

            protected void initialize() {
                this.authorizationContext = new AuthorizationContext();
            }

            public AuthorizationContext getResult() {
                AuthorizationContext result = this.authorizationContext;
                initialize();
                return result;
            }

            public Builder setClaims(Claims claims) {
                this.authorizationContext.claims = claims;
                return this;
            }

            public Builder setToken(String token) {
                this.authorizationContext.token = token;
                return this;
            }

            public Builder setPropagateToClient(boolean propagateToClient) {
                this.authorizationContext.propagateToClient = propagateToClient;
                return this;
            }

            public Builder setResourceQueryMap(Map<Action, Query> resourceQueryMap) {
                this.authorizationContext.resourceQueryMap = resourceQueryMap;
                return this;
            }

            public Builder setResourceQueryFilterMap(
                    Map<Action, QueryFilter> resourceQueryFiltersMap) {
                this.authorizationContext.resourceQueryFiltersMap = resourceQueryFiltersMap;
                return this;
            }
        }
    }

    public enum OperationOption {
        /**
         * Set to request underlying support for overlapping operations on the same connection.
         * For example, if set, and the service client is HTTP/2 aware, the operation will use the
         * same connection as many others, pending, operations
         */
        CONNECTION_SHARING,

        /**
         * Set by the client to both request a long lived connection on out-bound requests,
         * or indicate the operation was received on a long lived connection, for in-bound requests
         */
        KEEP_ALIVE,
        /**
         * Set on both out-bound and in-bound replicated updates
         */
        REPLICATED,

        /**
         * Set on both out-bound and in-bound forwarded requests
         */
        FORWARDED,

        /**
         * Set to prevent replication
         */
        REPLICATION_DISABLED,
        /**
         * Set by request listener to prevent cloning of the body during
         * {@link Operation#setBody(Object)}
         */
        CLONING_DISABLED,
        /**
         * Set to prevent notifications being sent after the service handler completes the
         * operation
         */
        NOTIFICATION_DISABLED,
        /**
         * Set if the target service is replicated
         */
        REPLICATED_TARGET,
        /**
         * Set by client to disable default logging of operation failures
         */
        FAILURE_LOGGING_DISABLED,
        /**
         * Set by a local {@code ServiceRequestListener} instance indicating the operation
         * originated outside the process / host
         */
        REMOTE,
        /**
         * The operation exceeded the rate limit associated with it logical context
         * (authorization subject by default)
         */
        RATE_LIMITED,

        /**
         * Infrastructure use only
         *
         * Set by transport/client to indicate the operation has an active socket
         * channel associated with it.
         */
        SOCKET_ACTIVE
    }

    public static class SerializedOperation extends ServiceDocument {
        public Action action;
        public String host;
        public int port;
        public String path;
        public String query;
        public Long id;
        public URI referer;
        public String jsonBody;
        public int statusCode;
        public EnumSet<OperationOption> options;
        public String contextId;
        public String transactionId;
        public String userInfo;

        public static final ServiceDocumentDescription DESCRIPTION = Operation.SerializedOperation
                .buildDescription();

        public static final String KIND = Utils.buildKind(Operation.SerializedOperation.class);

        public static SerializedOperation create(Operation op) {
            SerializedOperation ctx = new SerializedOperation();
            ctx.contextId = op.getContextId();
            ctx.action = op.action;
            ctx.referer = op.getReferer();
            ctx.id = op.id;
            ctx.statusCode = op.statusCode;
            ctx.options = op.options.clone();
            ctx.transactionId = op.getTransactionId();
            if (op.uri != null) {
                ctx.host = op.uri.getHost();
                ctx.port = op.uri.getPort();
                ctx.path = op.uri.getPath();
                ctx.query = op.uri.getQuery();
                ctx.userInfo = op.uri.getUserInfo();
            }

            Object body = op.getBodyRaw();
            if (body instanceof String) {
                ctx.jsonBody = (String) body;
            } else {
                ctx.jsonBody = Utils.toJson(body);
            }

            ctx.documentKind = KIND;
            ctx.documentExpirationTimeMicros = op.expirationMicrosUtc;
            return ctx;
        }

        public static boolean isValid(SerializedOperation sop) {
            if (sop.action == null || sop.id == null || sop.jsonBody == null || sop.path == null
                    || sop.referer == null) {
                return false;
            }
            return true;
        }

        public static ServiceDocumentDescription buildDescription() {
            EnumSet<Service.ServiceOption> options = EnumSet.of(Service.ServiceOption.PERSISTENCE);
            return Builder.create().buildDescription(SerializedOperation.class, options);
        }
    }

    public static void fail(Operation request, int statusCode, int errorCode, Throwable e) {
        request.setStatusCode(statusCode);
        ServiceErrorResponse r = Utils.toServiceErrorResponse(e);
        r.statusCode = statusCode;
        r.errorCode = errorCode;

        if (e instanceof ServiceNotFoundException) {
            r.stackTrace = null;
        }
        request.setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON).fail(e, r);
    }

    static void failOwnerMismatch(Operation op, String id, ServiceDocument body) {
        String owner = body != null ? body.documentOwner : "";
        op.setStatusCode(Operation.STATUS_CODE_CONFLICT);
        Throwable e = new IllegalStateException(format(
                "Owner in body: %s, computed locally: %s",
                owner, id));
        ServiceErrorResponse rsp = ServiceErrorResponse.create(e, op.getStatusCode(),
                EnumSet.of(ErrorDetail.SHOULD_RETRY));
        rsp.setInternalErrorCode(ServiceErrorResponse.ERROR_CODE_OWNER_MISMATCH);
        op.fail(e, rsp);
    }

    public static void failActionNotSupported(Operation request) {
        request.setStatusCode(Operation.STATUS_CODE_BAD_METHOD).fail(
                new IllegalStateException("Action not supported: " + request.getAction()));
    }

    public static void failLimitExceeded(Operation request, int errorCode, String queueDescription) {
        // Add a header indicating retry should be attempted after some interval.
        // Currently set to just one second, subject to change in the future
        request.addResponseHeader(Operation.RETRY_AFTER_HEADER, "1");
        fail(request, Operation.STATUS_CODE_UNAVAILABLE,
                errorCode,
                new CancellationException(format("queue limit exceeded (%s)", queueDescription)));
    }

    public static void failForwardedRequest(Operation op, Operation fo, Throwable fe) {
        op.setStatusCode(fo.getStatusCode());
        op.setBodyNoCloning(fo.getBodyRaw()).fail(fe);
    }

    public static void failServiceNotFound(Operation inboundOp) {
        failServiceNotFound(inboundOp,
                ServiceErrorResponse.ERROR_CODE_INTERNAL_MASK);
    }

    public static void failServiceNotFound(Operation inboundOp, int errorCode,
            String errorMsg) {
        fail(inboundOp, Operation.STATUS_CODE_NOT_FOUND,
                errorCode,
                new ServiceNotFoundException(inboundOp.getUri().toString(), errorMsg));
    }

    public static void failServiceNotFound(Operation inboundOp, int errorCode) {
        fail(inboundOp, Operation.STATUS_CODE_NOT_FOUND,
                errorCode,
                new ServiceNotFoundException(inboundOp.getUri().toString()));
    }

    static void failServiceMarkedDeleted(String documentSelfLink,
            Operation serviceStartPost) {
        fail(serviceStartPost, Operation.STATUS_CODE_CONFLICT,
                ServiceErrorResponse.ERROR_CODE_STATE_MARKED_DELETED,
                new IllegalStateException("Service marked deleted: "
                        + documentSelfLink));
    }

    public static void failServerNotAvailable(Operation inboundOp) {
        Throwable e = new IllegalStateException("Server unavailable");

        inboundOp.setStatusCode(Operation.STATUS_CODE_UNAVAILABLE);
        ServiceErrorResponse rsp = new ServiceErrorResponse();
        rsp.message = e.getMessage();
        rsp.details = EnumSet.of(ServiceErrorResponse.ErrorDetail.SHOULD_RETRY);
        rsp.statusCode = inboundOp.getStatusCode();
        inboundOp.fail(e, rsp);
    }

    // HTTP Header definitions
    public static final String REFERER_HEADER = "referer";
    public static final String CONNECTION_HEADER = "connection";
    public static final String CONTENT_TYPE_HEADER = "content-type";
    public static final String CONTENT_ENCODING_HEADER = "content-encoding";
    public static final String CONTENT_LENGTH_HEADER = "content-length";
    public static final String CONTENT_RANGE_HEADER = "content-range";
    public static final String RANGE_HEADER = "range";
    public static final String RETRY_AFTER_HEADER = "retry-after";
    public static final String PRAGMA_HEADER = "pragma";
    public static final String SET_COOKIE_HEADER = "set-cookie";
    public static final String COOKIE_HEADER = "cookie";
    public static final String LOCATION_HEADER = "location";
    public static final String USER_AGENT_HEADER = "user-agent";
    public static final String HOST_HEADER = "host";
    public static final String ACCEPT_HEADER = "accept";
    public static final String ACCEPT_ENCODING_HEADER = "accept-encoding";
    public static final String AUTHORIZATION_HEADER = "authorization";
    public static final String ACCEPT_LANGUAGE_HEADER = "accept-language";
    public static final String TRANSFER_ENCODING_HEADER = "transfer-encoding";
    public static final String CHUNKED_ENCODING = "chunked";
    public static final String LAST_EVENT_ID_HEADER = "last-event-id";

    // HTTP2 Header definitions
    public static final String STREAM_ID_HEADER = "x-http2-stream-id";
    public static final String STREAM_WEIGHT_HEADER = "x-http2-stream-weight";
    public static final String HTTP2_SCHEME_HEADER = "x-http2-scheme";

    // Proprietary header definitions
    public static final String HEADER_NAME_PREFIX = "x-xenon-";
    public static final String CONTEXT_ID_HEADER = HEADER_NAME_PREFIX + "ctx-id";
    public static final String REQUEST_AUTH_TOKEN_HEADER = HEADER_NAME_PREFIX
            + "auth-token";
    public static final String REPLICATION_PHASE_HEADER = HEADER_NAME_PREFIX
            + "rpl-phase";
    public static final String REPLICATION_QUORUM_HEADER = HEADER_NAME_PREFIX
            + "rpl-quorum";
    public static final String REPLICATION_PARENT_HEADER = HEADER_NAME_PREFIX + "rpl-parent";
    public static final String REPLICATION_QUORUM_HEADER_VALUE_ALL = HEADER_NAME_PREFIX
            + "all";
    public static final String TRANSACTION_HEADER = HEADER_NAME_PREFIX
            + "tx-phase";
    public static final String TRANSACTION_ID_HEADER = HEADER_NAME_PREFIX + "tx-id";
    public static final String TRANSACTION_REFLINK_HEADER = HEADER_NAME_PREFIX + "tx-reflink";

    // Non standard but widely used headers
    public static final String REQUEST_ID_HEADER = "x-request-id";

    /**
     * Infrastructure use only. Set when a service is first created due to a client request. Since
     * service start can be invoked by the runtime during node synchronization, restart, this
     * directive is the only way to distinguish original creation of arbitrary services (without relying
     * on state version heuristics)
     */
    public static final String PRAGMA_DIRECTIVE_CREATED = "xn-created";

    /**
     * Infrastructure use only. Set when the runtime, as part of its consensus protocol,
     * forwards a client request from one node, to a node deemed as the owner of the service
     */
    public static final String PRAGMA_DIRECTIVE_FORWARDED = "xn-fwd";

    /**
     * Infrastructure use only. Set when the consensus protocol replicates an update, on the owner
     * node service instance, to the peer replica instances
     */
    public static final String PRAGMA_DIRECTIVE_REPLICATED = "xn-rpl";

    /**
     * Infrastructure use only. Set when the Synchronization task makes a POST request
     * to trigger synchronization of a service instance. This post request is processed
     * by the OWNER of the service instance.
     */
    public static final String PRAGMA_DIRECTIVE_SYNCH_OWNER = "xn-synch-owner";

    /**
     * Infrastructure use only. Set when the owner node of a service instance
     * computes the best state as part of synchronization and broadcasts the
     * best state to all peer nodes.
     */
    public static final String PRAGMA_DIRECTIVE_SYNCH_PEER = "xn-synch-peer";

    /**
     * Infrastructure use only. Set when all versions, not just the latest, need
     * to be synchronized.
     */
    public static final String PRAGMA_DIRECTIVE_SYNCH_ALL_VERSIONS = "xn-synch-all-ver";

    /**
     * Infrastructure use only. Set when all versions, not just the latest, need
     * to be synchronized.
     */
    public static final String PRAGMA_DIRECTIVE_SYNCH_HISTORICAL_VERSIONS = "xn-synch-hist-ver";

    /**
     * Infrastructure use only. Set on a synchPeer request when a specific historical
     * version needs to be synchronized.
     */
    public static final String PRAGMA_DIRECTIVE_SYNCH_VERSION = "xn-synch-ver";

    /**
     * Advanced use. Instructs the runtime to queue a request, for a service to become available.
     */
    public static final String PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY = "xn-queue";

    /**
     * Infrastructure use only. Instructs the runtime that this request should be processed on the node
     * it arrived on. It should not be forwarded regardless of owner selection and load balancing decisions.
     */
    public static final String PRAGMA_DIRECTIVE_NO_FORWARDING = "xn-no-fwd";

    /**
     * Infrastructure use only. Set on notifications only.
     */
    public static final String PRAGMA_DIRECTIVE_NOTIFICATION = "xn-nt";

    /**
     * Infrastructure use only. Set by the runtime to inform a subscriber that they might have missed notifications
     * due to state updates occurring when the subscriber was not available.
     */
    public static final String PRAGMA_DIRECTIVE_SKIPPED_NOTIFICATIONS = "xn-nt-skipped";

    /**
     *  Infrastructure use only. Does a strict update version check and if the service exists or
     *  the service has been previously deleted the request will fail.
     *
     *  Overridden by: PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE
     */
    public static final String PRAGMA_DIRECTIVE_VERSION_CHECK = "xn-check-version";

    /**
     * Infrastructure use only. Used for on demand load of services. Checks the index if a service
     * exists.
     */
    public static final String PRAGMA_DIRECTIVE_INDEX_CHECK = "xn-check-index";

    /**
     * Instructs a persisted service to force the update, overriding version checks. It should be used
     * for conflict resolution only.
     *
     * Overrides: PRAGMA_DIRECTIVE_VERSION_CHECK
     */
    public static final String PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE = "xn-force-index-update";

    /**
     * Infrastructure use only. Instructs a persisted service to complete the operation but skip any index
     * updates.
     */
    public static final String PRAGMA_DIRECTIVE_NO_INDEX_UPDATE = "xn-no-index-update";

    /**
     * Infrastructure use only. Instructs AuthorizationContextService to treat this as a request to
     * clear the authz cache
     */
    public static final String PRAGMA_DIRECTIVE_CLEAR_AUTH_CACHE = "xn-clear-auth-cache";

    /**
     * Infrastructure use only. Debugging only. Indicates this operation was converted from POST to PUT
     * due to {@link com.vmware.xenon.common.Service.ServiceOption#IDEMPOTENT_POST}
     */
    public static final String PRAGMA_DIRECTIVE_POST_TO_PUT = "xn-post-to-put";

    /**
     * Infrastructure use only. Instructs AuthenticationService to treat this as a request to
     * authenticate and retrieve the auth token
     */
    public static final String PRAGMA_DIRECTIVE_AUTHENTICATE = "xn-authn";

    /**
     * Infrastructure use only. Instructs AuthenticationService to treat this as a request to
     * verify the auth token
     */
    public static final String PRAGMA_DIRECTIVE_VERIFY_TOKEN = "xn-verify-token";

    /**
     * Infrastructure use only. Instructs AuthenticationService to treat this as a request to
     * logout and invalidate the auth token
     */
    public static final String PRAGMA_DIRECTIVE_AUTHN_INVALIDATE = "xn-authn-invalidate";

    /**
     * Set when a MigrationTaskService invokes operations against the destination cluster.
     */
    public static final String PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK = "xn-from-migration";

    /**
     * Infrastructure use only. Indicate document state was not modified by the update request.
     * When this pragma is set in handler methods, rest of the pipeline will not update the
     * indexed/cached state.
     */
    public static final String PRAGMA_DIRECTIVE_STATE_NOT_MODIFIED = "xn-state-not-modified";

    public static final String TX_ENSURE_COMMIT = "ensure-commit";
    public static final String TX_COMMIT = "commit";
    public static final String TX_ABORT = "abort";
    public static final String REPLICATION_PHASE_COMMIT = "commit";

    public static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";
    public static final String MEDIA_TYPE_TEXT_YAML = "text/x-yaml";
    public static final String MEDIA_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM = "application/kryo-octet-stream";
    public static final String MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED = "application/x-www-form-urlencoded";
    public static final String MEDIA_TYPE_TEXT_HTML = "text/html";
    public static final String MEDIA_TYPE_TEXT_PLAIN = "text/plain";
    public static final String MEDIA_TYPE_TEXT_CSS = "text/css";
    public static final String MEDIA_TYPE_APPLICATION_JAVASCRIPT = "application/javascript";
    public static final String MEDIA_TYPE_IMAGE_SVG_XML = "image/svg+xml";
    public static final String MEDIA_TYPE_APPLICATION_FONT_WOFF2 = "application/font-woff2";
    public static final String MEDIA_TYPE_TEXT_EVENT_STREAM = "text/event-stream";

    public static final String CONTENT_ENCODING_GZIP = "gzip";

    public static final int STATUS_CODE_SERVER_FAILURE_THRESHOLD = HttpURLConnection.HTTP_INTERNAL_ERROR;
    public static final int STATUS_CODE_FAILURE_THRESHOLD = HttpURLConnection.HTTP_BAD_REQUEST;
    public static final int STATUS_CODE_UNAUTHORIZED = HttpURLConnection.HTTP_UNAUTHORIZED;
    public static final int STATUS_CODE_UNAVAILABLE = HttpURLConnection.HTTP_UNAVAILABLE;
    public static final int STATUS_CODE_FORBIDDEN = HttpURLConnection.HTTP_FORBIDDEN;
    public static final int STATUS_CODE_TIMEOUT = HttpURLConnection.HTTP_CLIENT_TIMEOUT;
    public static final int STATUS_CODE_CONFLICT = HttpURLConnection.HTTP_CONFLICT;
    public static final int STATUS_CODE_NOT_MODIFIED = HttpURLConnection.HTTP_NOT_MODIFIED;
    public static final int STATUS_CODE_NOT_FOUND = HttpURLConnection.HTTP_NOT_FOUND;
    public static final int STATUS_CODE_MOVED_PERM = HttpURLConnection.HTTP_MOVED_PERM;
    public static final int STATUS_CODE_MOVED_TEMP = HttpURLConnection.HTTP_MOVED_TEMP;
    public static final int STATUS_CODE_OK = HttpURLConnection.HTTP_OK;
    public static final int STATUS_CODE_CREATED = HttpURLConnection.HTTP_CREATED;
    public static final int STATUS_CODE_ACCEPTED = HttpURLConnection.HTTP_ACCEPTED;
    public static final int STATUS_CODE_BAD_REQUEST = HttpURLConnection.HTTP_BAD_REQUEST;
    public static final int STATUS_CODE_BAD_METHOD = HttpURLConnection.HTTP_BAD_METHOD;
    public static final int STATUS_CODE_INTERNAL_ERROR = HttpURLConnection.HTTP_INTERNAL_ERROR;

    public static final String MEDIA_TYPE_EVERYTHING_WILDCARDS = "*/*";
    public static final String EMPTY_JSON_BODY = "{}";
    public static final String HEADER_FIELD_VALUE_SEPARATOR = ":";
    public static final String CR_LF = "\r\n";

    private static final char DIRECTIVE_PRAGMA_VALUE_SEPARATOR_CHAR_CONST = ';';
    private static final char HEADER_FIELD_VALUE_SEPARATOR_CHAR_CONST = ':';

    private static final AtomicLong idCounter = new AtomicLong();
    private static final AtomicReferenceFieldUpdater<Operation, CompletionHandler> completionUpdater = AtomicReferenceFieldUpdater
            .newUpdater(Operation.class, CompletionHandler.class, "completion");

    private URI uri;
    private Object referer;
    private final long id = idCounter.incrementAndGet();
    private int statusCode = Operation.STATUS_CODE_OK;
    private Action action;
    private ServiceDocument linkedState;
    private byte[] linkedSerializedState;
    private volatile CompletionHandler completion;
    private String contextId;
    private String transactionId;
    private long expirationMicrosUtc;
    private Object body;
    private Object serializedBody;
    private String contentType = MEDIA_TYPE_APPLICATION_JSON;
    private long contentLength;
    private RemoteContext remoteCtx;
    private AuthorizationContext authorizationCtx;
    private InstrumentationContext instrumentationCtx;
    private short retryCount;
    private short retriesRemaining;

    private EnumSet<OperationOption> options = EnumSet.of(OperationOption.KEEP_ALIVE);

    private volatile Consumer<ServerSentEvent> serverSentEventHandler;

    private volatile Consumer<Operation> headersReceivedHandler;

    public static Operation create(SerializedOperation ctx, ServiceHost host) {
        Operation op = new Operation();
        op.action = ctx.action;
        op.body = ctx.jsonBody;
        op.expirationMicrosUtc = ctx.documentExpirationTimeMicros;
        op.setContextId(ctx.id.toString());
        op.referer = ctx.referer;
        op.uri = UriUtils.buildUri(host, ctx.path, ctx.query, ctx.userInfo);
        op.transactionId = ctx.transactionId;
        return op;
    }

    public Operation() {
        // Set operation context from thread local.
        // The thread local is populated by the service host when it handles an operation,
        // which means that derivative operations will automatically inherit this context.
        // It is set as early as possible since there is a possibility that it is
        // overridden by the service implementation (i.e. when it impersonates).
        OperationContext opCtx = OperationContext.getOperationContextNoCloning();
        this.authorizationCtx = opCtx.authContext;
        this.transactionId = opCtx.transactionId;
        this.contextId = opCtx.contextId;
    }

    static Operation createOperation(Action action, URI uri) {
        Operation op = new Operation();
        op.uri = uri;
        op.action = action;
        return op;
    }

    public static Operation createPost(Service sender, String targetPath) {
        return createPost(sender.getHost(), targetPath);
    }

    public static Operation createPost(ServiceHost sender, String targetPath) {
        return createPost(UriUtils.buildUri(sender, targetPath));
    }

    public static Operation createPost(URI uri) {
        return createOperation(Action.POST, uri);
    }

    public static Operation createPatch(Service sender, String targetPath) {
        return createPatch(sender.getHost(), targetPath);
    }

    public static Operation createPatch(ServiceHost sender, String targetPath) {
        return createPatch(UriUtils.buildUri(sender, targetPath));
    }

    public static Operation createPatch(URI uri) {
        return createOperation(Action.PATCH, uri);
    }

    public static Operation createPut(Service sender, String targetPath) {
        return createPut(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createPut(ServiceHost sender, String targetPath) {
        return createPut(UriUtils.buildUri(sender, targetPath));
    }

    public static Operation createPut(URI uri) {
        return createOperation(Action.PUT, uri);
    }

    public static Operation createOptions(Service sender, String targetPath) {
        return createOptions(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createOptions(ServiceHost sender, String targetPath) {
        return createOptions(UriUtils.buildUri(sender, targetPath));
    }

    public static Operation createOptions(URI uri) {
        return createOperation(Action.OPTIONS, uri);
    }

    public static Operation createDelete(Service sender, String targetPath) {
        return createDelete(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createDelete(ServiceHost sender, String targetPath) {
        return createDelete(UriUtils.buildUri(sender, targetPath));
    }

    public static Operation createDelete(URI uri) {
        return createOperation(Action.DELETE, uri);
    }

    public static Operation createGet(Service sender, String targetPath) {
        return createGet(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createGet(ServiceHost sender, String targetPath) {
        return createGet(UriUtils.buildUri(sender, targetPath));
    }

    public static Operation createGet(URI uri) {
        return createOperation(Action.GET, uri);
    }

    public void sendWith(ServiceRequestSender sender) {
        sender.sendRequest(this);
    }

    @Override
    public String toString() {
        SerializedOperation sop = SerializedOperation.create(this);
        if (sop.jsonBody != null && sop.jsonBody.length() > TO_STRING_SERIALIZED_BODY_LIMIT) {
            // Avoiding logging the entire body, which could be huge, and overwhelm the logs.
            // Keep just an arbitrary prefix, serving as a hint
            sop.jsonBody = sop.jsonBody.substring(0, TO_STRING_SERIALIZED_BODY_LIMIT);
        }
        return Utils.toJsonHtml(sop);
    }

    /**
     * Returns a string summary of the operation appropriate for logging
     */
    public String toLogString() {
        StringBuilder sb = Utils.getBuilder();
        sb.append(this.action.toString()).append(" ")
                .append(this.getUri()).append(" ")
                .append(this.id).append(" ")
                .append(this.getRefererAsString()).append(" ");
        if (this.contextId != null) {
            sb.append("[ctxId] ").append(this.contextId);
        }
        if (this.transactionId != null) {
            sb.append("[txId] ").append(this.transactionId);
        }
        if (this.authorizationCtx != null && this.authorizationCtx.claims != null) {
            sb.append("[subject] ").append(this.authorizationCtx.claims.getSubject());
        }
        return sb.toString();
    }

    @Override
    public Operation clone() {
        Operation clone;
        try {
            clone = (Operation) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }

        // Clone mutable fields
        // body is always cloned on set, so no need to re-clone
        clone.options = EnumSet.copyOf(this.options);

        if (this.remoteCtx != null) {
            clone.remoteCtx = new RemoteContext();
            if (this.remoteCtx.cookies != null) {
                clone.remoteCtx.cookies = new HashMap<>(this.remoteCtx.cookies);
            }
            // do not clone socket context
            clone.remoteCtx.socketCtx = null;
            if (this.remoteCtx.requestHeaders != null && !this.remoteCtx.requestHeaders.isEmpty()) {
                clone.remoteCtx.requestHeaders = new HashMap<>(this.remoteCtx.requestHeaders);
            }
            clone.remoteCtx.peerPrincipal = this.remoteCtx.peerPrincipal;
            if (this.remoteCtx.peerCertificateChain != null) {
                clone.remoteCtx.peerCertificateChain = Arrays.copyOf(
                        this.remoteCtx.peerCertificateChain,
                        this.remoteCtx.peerCertificateChain.length);
            }
            clone.remoteCtx.connectionTag = this.remoteCtx.connectionTag;
        }

        return clone;
    }

    private void allocateRemoteContext() {
        if (this.remoteCtx == null) {
            this.remoteCtx = new RemoteContext();
        }
    }

    private void allocateRequestHeaders() {
        if (this.remoteCtx.requestHeaders == null) {
            this.remoteCtx.requestHeaders = new HashMap<>();
        }
    }

    private void allocateResponseHeaders() {
        if (this.remoteCtx.responseHeaders == null) {
            this.remoteCtx.responseHeaders = new HashMap<>();
        }
    }

    public boolean isRemote() {
        return this.options.contains(OperationOption.REMOTE) ||
                (this.remoteCtx != null && this.remoteCtx.socketCtx != null);
    }

    public Operation forceRemote() {
        return toggleOption(OperationOption.REMOTE, true);
    }

    public AuthorizationContext getAuthorizationContext() {
        return this.authorizationCtx;
    }

    /**
     * Sets (overwrites) the authorization context of this operation.
     *
     * Infrastructure use only.
     */
    public Operation setAuthorizationContext(AuthorizationContext ctx) {
        this.authorizationCtx = ctx;
        return this;
    }

    public String getTransactionId() {
        return this.transactionId;
    }

    public Operation setTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public boolean isWithinTransaction() {
        return this.transactionId != null;
    }

    public String getContextId() {
        return this.contextId;
    }

    public Operation setContextId(String id) {
        this.contextId = id;
        return this;
    }

    public Operation setBody(Object body) {
        if (body != null) {
            if (hasOption(OperationOption.CLONING_DISABLED)) {
                this.body = body;
            } else {
                this.body = Utils.clone(body);
            }
        } else {
            this.body = null;
        }
        return this;
    }

    public Operation setStatusCode(int code) {
        this.statusCode = code;
        return this;
    }

    /**
     * Infrastructure use only
     *
     * @param body
     * @return
     */
    public Operation setBodyNoCloning(Object body) {
        this.body = body;
        return this;
    }

    public ServiceErrorResponse getErrorResponseBody() {
        if (!hasBody()) {
            return null;
        }
        ServiceErrorResponse rsp = getBody(ServiceErrorResponse.class);
        if (rsp.message == null && rsp.statusCode == 0) {
            // very likely not a error response body
            return null;
        }
        return rsp;
    }

    /**
     * Deserializes the body associated with the operation, given the type.
     *
     * Note: This method is *not* idempotent. It will modify the body contents
     * so subsequent calls will not have access to the original instance. This
     * occurs only for local operations, not operations that have a serialized
     * body already attached (in the form of a JSON string).
     *
     * If idempotent behavior is desired, use {@link #getBodyRaw}
     */
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> type) {
        if (this.body != null && this.body.getClass() == type) {
            return (T) this.body;
        }

        if (this.body != null && !(this.body instanceof String)) {
            if (this.contentType != null && Utils.isContentTypeKryoBinary(this.contentType)
                    && this.body instanceof byte[]) {
                byte[] bytes = (byte[])this.body;
                this.body = KryoSerializers.deserializeDocument(bytes, 0, bytes.length);
                this.serializedBody = Utils.toJson(this.body);
                return (T) this.body;
            }

            if (this.contentType == null
                    || !this.contentType.contains(MEDIA_TYPE_APPLICATION_JSON)) {
                throw new IllegalStateException("content type is not JSON: " + this.contentType);
            }

            if (this.serializedBody != null) {
                this.body = this.serializedBody;
            } else {
                String json = Utils.toJson(this.body);
                return Utils.fromJson(json, type);
            }
        }

        if (this.body != null) {
            if (this.body instanceof String) {
                this.serializedBody = this.body;
            }
            // Request must specify a Content-Type we understand
            if (this.contentType != null
                    && this.contentType.contains(MEDIA_TYPE_APPLICATION_JSON)) {
                try {
                    this.body = Utils.fromJson(this.body, type);
                } catch (com.google.gson.JsonSyntaxException e) {
                    throw new IllegalArgumentException("Unparseable JSON body: " + e.getMessage(),
                            e);
                }
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized Content-Type for parsing request body: " +
                                this.contentType);
            }
            return (T) this.body;
        }

        throw new IllegalStateException();
    }

    public Object getBodyRaw() {
        return this.body;
    }

    public long getContentLength() {
        return this.contentLength;
    }

    public Operation setContentLength(long l) {
        this.contentLength = l;
        return this;
    }

    public String getContentType() {
        return this.contentType;
    }

    public Operation setContentType(String type) {
        this.contentType = type;
        return this;
    }

    public Operation setCookies(Map<String, String> cookies) {
        allocateRemoteContext();
        this.remoteCtx.cookies = cookies;
        return this;
    }

    public Map<String, String> getCookies() {
        if (this.remoteCtx == null) {
            return null;
        }
        return this.remoteCtx.cookies;
    }

    public int getRetriesRemaining() {
        return this.retriesRemaining;
    }

    public int getRetryCount() {
        return this.retryCount;
    }

    public int incrementRetryCount() {
        return ++this.retryCount;
    }

    public Operation setRetryCount(int retryCount) {
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount must be positive");
        }
        if (retryCount > Short.MAX_VALUE) {
            throw new IllegalArgumentException("retryCount must be less than " + Short.MAX_VALUE);
        }
        this.retryCount = (short) retryCount;
        this.retriesRemaining = (short) retryCount;
        return this;
    }

    public Operation setCompletion(CompletionHandler completion) {
        this.completion = completion;
        return this;
    }

    /**
     * Takes two discrete callback handlers for completion.
     *
     * For completion that does not share code between success and failure paths, this should be the
     * preferred alternative.
     *
     * @param successHandler called at successful operation completion
     * @param failureHandler called at failure operation completion
     * @return operation
     */
    public Operation setCompletion(Consumer<Operation> successHandler,
            CompletionHandler failureHandler) {
        this.completion = (op, e) -> {
            if (e != null) {
                failureHandler.handle(op, e);
                return;
            }
            successHandler.accept(op);
        };
        return this;
    }

    /**
     * Sets the handler to be invoked upon receiving {@link ServerSentEvent}.
     * @param serverSentEventHandler called when {@link ServerSentEvent} is received
     * @return operation
     */
    public Operation setServerSentEventHandler(Consumer<ServerSentEvent> serverSentEventHandler) {
        this.serverSentEventHandler = serverSentEventHandler;
        return this;
    }

    /**
     * Inserts a handler in LIFO style.
     * @see #nestHeadersReceivedHandler(Consumer)
     * @see #nestCompletion(CompletionHandler)
     * @param serverSentEventHandler
     * @return
     */
    public Operation nestServerSentEventHandler(Consumer<ServerSentEvent> serverSentEventHandler) {
        if (this.serverSentEventHandler != null) {
            serverSentEventHandler = serverSentEventHandler.andThen(this.serverSentEventHandler);
        }
        return this.setServerSentEventHandler(serverSentEventHandler);
    }

    /**
     * Sets the handler to be invoked upon receiving the headers.
     * @param handler called after the headers are received.
     * @return operation
     */
    public Operation setHeadersReceivedHandler(Consumer<Operation> handler) {
        this.headersReceivedHandler = handler;
        return this;
    }

    /**
     * Inserts a handler in LIFO style.
     * @see #nestServerSentEventHandler(Consumer)
     * @see #nestCompletion(CompletionHandler)
     * @param handler
     * @return
     */
    public Operation nestHeadersReceivedHandler(Consumer<Operation> handler) {
        if (this.headersReceivedHandler != null) {
            handler = handler.andThen(this.headersReceivedHandler);
        }
        return this.setHeadersReceivedHandler(handler);
    }

    public CompletionHandler getCompletion() {
        return this.completion;
    }

    public Operation setUri(URI uri) {
        this.uri = uri;
        return this;
    }

    public URI getUri() {
        return this.uri;
    }

    Operation linkState(ServiceDocument serviceDoc) {
        if (serviceDoc != null && this.linkedState != null
                && this.linkedState.documentKind != null) {
            serviceDoc.documentKind = this.linkedState.documentKind;
        }
        // we do not clone here because the service will clone before the next
        // request is processed
        this.linkedState = serviceDoc;
        return this;
    }

    ServiceDocument getLinkedState() {
        return this.linkedState;
    }

    /**
     * Infrastructure use only
     */
    byte[] getLinkedSerializedState() {
        return this.linkedSerializedState;
    }

    boolean hasLinkedSerializedState() {
        return this.linkedSerializedState != null;
    }

    public Operation setReferer(URI uri) {
        this.referer = uri;
        return this;
    }

    public Operation setReferer(String uri) {
        this.referer = uri;
        return this;
    }

    public Operation transferRefererFrom(Operation op) {
        this.referer = op.referer;
        return this;
    }

    public boolean hasReferer() {
        return this.referer != null;
    }

    public URI getReferer() {
        if (this.referer instanceof String) {
            try {
                this.referer = new URI((String) this.referer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return (URI) this.referer;
    }

    public String getRefererAsString() {
        if (this.referer == null) {
            return null;
        }
        return this.referer.toString();
    }

    public Operation setAction(Action action) {
        this.action = action;
        return this;
    }

    public Action getAction() {
        return this.action;
    }

    public long getId() {
        return this.id;
    }

    public Operation setSocketContext(SocketContext socketContext) {
        allocateRemoteContext();
        this.remoteCtx.socketCtx = socketContext;
        return this;
    }

    public SocketContext getSocketContext() {
        return this.remoteCtx == null ? null : this.remoteCtx.socketCtx;
    }

    public long getExpirationMicrosUtc() {
        return this.expirationMicrosUtc;
    }

    /**
     * Sets the expiration using the supplied absolute time value, that should be in microseconds
     * since epoch
     */
    public Operation setExpiration(long futureMicrosUtc) {
        this.expirationMicrosUtc = futureMicrosUtc;
        return this;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public void complete() {
        completeOrFail(null);
    }

    public void fail(Throwable e) {
        fail(e, null);
    }

    /**
     * Sends a &quot;server sent event&quot;. See <a href=https://www.w3.org/TR/eventsource/>SSE specification</a>}
     * @param event The event to send.
     */
    public void sendServerSentEvent(ServerSentEvent event) {
        if (this.serverSentEventHandler == null) {
            return;
        }

        // Keep track of current operation context
        OperationContext originalContext = OperationContext.getOperationContext();
        try {
            OperationContext.setFrom(this);
            this.serverSentEventHandler.accept(event);
        } catch (Exception outer) {
            Utils.logWarning("Uncaught failure inside serverSentEventHandler: %s", Utils.toString(outer));
        } finally {
            // Restore original context
            OperationContext.setFrom(originalContext);
        }
    }

    /**
     * Sends the headers to the channel.
     * This effectively enables streaming.
     */
    public void sendHeaders() {
        if (this.headersReceivedHandler == null) {
            return;
        }

        // Keep track of current operation context
        OperationContext originalContext = OperationContext.getOperationContext();
        try {
            OperationContext.setFrom(this);
            this.headersReceivedHandler.accept(this);
        } catch (Exception outer) {
            Utils.logWarning("Uncaught failure inside headersReceivedHandler: %s", Utils.toString(outer));
        } finally {
            // Restore original context
            OperationContext.setFrom(originalContext);
        }
    }

    /**
     * Send the appropriate headers and prepare the connection for streaming.
     */
    public void startEventStream() {
        setContentType(MEDIA_TYPE_TEXT_EVENT_STREAM);
        addResponseHeader(Operation.TRANSFER_ENCODING_HEADER, Operation.CHUNKED_ENCODING);
        sendHeaders();
    }

    public void fail(int statusCode) {
        setStatusCode(statusCode);
        switch (statusCode) {
        case STATUS_CODE_FORBIDDEN:
            fail(new IllegalAccessError("forbidden"));
            break;
        case STATUS_CODE_TIMEOUT:
            fail(new TimeoutException());
            break;
        default:
            fail(new Exception("request failed with " + statusCode + ", no additional details provided"));
            break;
        }
    }

    public void fail(int statusCode, Throwable e, Object failureBody) {
        this.statusCode = statusCode;
        fail(e, failureBody);
    }

    public void fail(Throwable e, Object failureBody) {
        if (this.statusCode < STATUS_CODE_FAILURE_THRESHOLD) {
            this.statusCode = STATUS_CODE_SERVER_FAILURE_THRESHOLD;
        }

        if (e instanceof TimeoutException) {
            this.statusCode = Operation.STATUS_CODE_TIMEOUT;
        }

        if (failureBody != null) {
            setBodyNoCloning(failureBody);
        }

        boolean hasErrorResponseBody = false;
        if (this.body != null && this.body instanceof String) {
            if (Operation.MEDIA_TYPE_APPLICATION_JSON.equals(this.contentType)) {
                try {
                    ServiceErrorResponse rsp = Utils.fromJson(this.body,
                            ServiceErrorResponse.class);
                    if (rsp.message != null) {
                        hasErrorResponseBody = true;
                    }
                } catch (Exception ex) {
                    // the body is not JSON, ignore
                }
            } else {
                // the response body is text but not JSON, we will leave as is
                hasErrorResponseBody = true;
            }
        }

        if (this.body != null && this.body instanceof byte[]) {
            // the response body is binary or unknown text encoding, leave as is
            hasErrorResponseBody = true;
        }

        if (this.body == null
                || ((!hasErrorResponseBody) && !(this.body instanceof ServiceErrorResponse))) {
            ServiceErrorResponse rsp;
            if (Utils.isValidationError(e)) {
                this.statusCode = STATUS_CODE_BAD_REQUEST;
                rsp = Utils.toValidationErrorResponse(e, this);
            } else {
                rsp = Utils.toServiceErrorResponse(e);
            }
            rsp.statusCode = this.statusCode;
            setBodyNoCloning(rsp).setContentType(Operation.MEDIA_TYPE_APPLICATION_JSON);
        }

        completeOrFail(e);
    }

    private void completeOrFail(Throwable e) {
        CompletionHandler c = this.completion;
        if (c == null) {
            return;
        }

        if (!completionUpdater.compareAndSet(this, c, null)) {
            Utils.logWarning("%s:%s",
                    Utils.toString(new IllegalStateException("double completion")),
                    toString());
            return;
        }

        // Keep track of current operation context so that code AFTER "op.complete()"
        // or "op.fail()" retains its operation context, and is not overwritten by
        // the one associated with "op" (which might be different).
        OperationContext originalContext = OperationContext.getOperationContext();
        try {
            OperationContext.setFrom(this);
            c.handle(this, e);
        } catch (Exception outer) {
            Utils.logWarning("Uncaught failure inside completion: %s", Utils.toString(outer));
        }

        // Restore original context
        OperationContext.setFrom(originalContext);
    }

    public boolean hasBody() {
        return this.body != null;
    }

    /**
     * Add CompletionHandler in LIFO style.
     *
     * This is symmetric to {@link #appendCompletion(CompletionHandler)}.
     * <pre>
     * {@code
     *   op.setCompletion(ORG);
     *   op.nestCompletion(A);
     *   op.nestCompletion(B);
     *   // complete() will trigger: B -> A -> ORG
     * }
     * </pre>
     */
    public Operation nestCompletion(CompletionHandler h) {
        CompletionHandler existing = this.completion;
        this.completion = (o, e) -> {
            this.statusCode = o.statusCode;
            this.completion = existing;
            h.handle(o, e);
        };
        return this;
    }

    /**
     * Add CompletionHandler in LIFO style, with support for cloned operations.
     *
     * This is a workaround for https://www.pivotaltracker.com/story/show/151798288
     * which has some complications for a root cause fix.
     *
     * This is symmetric to {@link #appendCompletion(CompletionHandler)}.
     * <pre>
     * {@code
     *   op.setCompletion(ORG);
     *   op.nestCompletion(A);
     *   op.nestCompletion(B);
     *   // complete() will trigger: B -> A -> ORG
     * }
     * </pre>
     */
    public Operation nestCompletionCloneSafe(CompletionHandler h) {
        final CompletionHandler existing = this.completion;
        this.completion = (o, e) -> {
            this.statusCode = o.statusCode;
            o.completion = existing;
            h.handle(o, e);
        };
        return this;

    }

    public void nestCompletion(Consumer<Operation> successHandler) {
        CompletionHandler existing = this.completion;
        this.completion = (o, e) -> {
            this.statusCode = o.statusCode;
            this.completion = existing;
            if (e != null) {
                fail(e);
                return;
            }
            try {
                successHandler.accept(o);
            } catch (Exception ex) {
                fail(ex);
            }
        };
    }

    /**
     * Add CompletionHandler in FIFO style.
     *
     * This is symmetric to {@link #nestCompletion(CompletionHandler)}.
     * <pre>
     * {@code
     *   op.setCompletion(ORG);
     *   op.addCompletion(A);
     *   op.addCompletion(B);
     *   // complete() will trigger: ORG -> A -> B
     * }
     * </pre>
     */
    public Operation appendCompletion(CompletionHandler h) {
        CompletionHandler existing = this.completion;
        if (existing == null) {
            this.completion = h;
        } else {
            this.completion = (o, e) -> {
                o.nestCompletion(h);
                existing.handle(o, e);
            };
        }
        return this;
    }

    Operation addHeader(String headerLine, boolean isResponse) {
        if (headerLine == null) {
            throw new IllegalArgumentException("headerLine is required");
        }

        int idx = headerLine.indexOf(HEADER_FIELD_VALUE_SEPARATOR_CHAR_CONST);
        if (idx < 3) {
            throw new IllegalArgumentException("headerLine does not appear valid");
        }

        String name = headerLine.substring(0, idx);
        String value = headerLine.substring(idx + 1);
        if (isResponse) {
            addResponseHeader(name, value);
        } else {
            addRequestHeader(name, value);
        }
        return this;
    }

    public Operation addRequestHeader(String name, String value) {
        return addRequestHeader(name, value, true);
    }

    private Operation addRequestHeader(String name, String value, boolean normalize) {
        allocateRemoteContext();
        allocateRequestHeaders();
        if (normalize) {
            value = removeString(value, CR_LF).trim();
            name = name.toLowerCase();
        }
        this.remoteCtx.requestHeaders.put(name, value);
        return this;
    }

    public Operation addResponseHeader(String name, String value) {
        allocateRemoteContext();
        allocateResponseHeaders();
        value = removeString(value, CR_LF).trim();
        this.remoteCtx.responseHeaders.put(name.toLowerCase(), value);
        return this;
    }

    public Operation addResponseCookie(String key, String value) {
        addResponseHeader(SET_COOKIE_HEADER, key + '=' + value);
        return this;
    }

    private String removeString(String value, String delete) {
        int i = 0;
        while ((i = value.indexOf(delete, i)) != -1) {
            if (i == 0) {
                value = value.substring(i + delete.length());
            } else if (i + delete.length() == value.length()) {
                value = value.substring(0, i);
            } else {
                value = value.substring(0, i) + value.substring(i + delete.length());
            }
        }
        return value;
    }

    /**
     * Add a directive. Lower case strings must be used.
     */
    public Operation addPragmaDirective(String directive) {
        String existingDirectives = getRequestHeader(PRAGMA_HEADER, false);
        if (existingDirectives != null) {
            if (indexOfPragmaDirective(existingDirectives, directive) != -1) {
                return this;
            }

            directive = existingDirectives + DIRECTIVE_PRAGMA_VALUE_SEPARATOR_CHAR_CONST
                    + directive;
        }
        directive = removeString(directive, CR_LF).trim();
        addRequestHeader(PRAGMA_HEADER, directive, false);
        return this;
    }

    /**
     * Checks if a directive is present. Lower case strings must be used.
     */
    public boolean hasPragmaDirective(String directive) {
        String existingDirectives = getRequestHeaderAsIs(PRAGMA_HEADER);
        return existingDirectives != null
                && indexOfPragmaDirective(existingDirectives, directive) != -1;
    }

    /**
     * Checks if a directive is present. Lower case strings must be used.
     */
    public boolean hasAnyPragmaDirective(List<String> directives) {
        String existingDirectives = getRequestHeaderAsIs(PRAGMA_HEADER);
        if (existingDirectives == null) {
            return false;
        }

        for (String directive : directives) {
            if (indexOfPragmaDirective(existingDirectives, directive) != -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * Removes a directive. Lower case strings must be used.
     */
    public Operation removePragmaDirective(String directive) {
        String existingDirectives = getRequestHeaderAsIs(PRAGMA_HEADER);
        if (existingDirectives != null) {
            int i = indexOfPragmaDirective(existingDirectives, directive);
            if (i == -1) {
                return this;
            }

            if (i == 0) {
                existingDirectives = existingDirectives.substring(i + directive.length());
            } else {
                existingDirectives = existingDirectives.substring(0, i - 1) + existingDirectives
                        .substring(i + directive.length());
            }

            addRequestHeader(PRAGMA_HEADER, existingDirectives, false);
        }
        return this;
    }

    int indexOfPragmaDirective(String existingDirectives, String directive) {
        int i = 0;
        while ((i = existingDirectives.indexOf(directive, i)) != -1) {
            // make sure sure we fully match the directive
            if (i + directive.length() == existingDirectives.length()
                    || existingDirectives.charAt(i + directive.length())
                    == DIRECTIVE_PRAGMA_VALUE_SEPARATOR_CHAR_CONST) {
                return i;
            }

            i++;
        }

        return -1;
    }

    public boolean isKeepAlive() {
        return this.remoteCtx != null && hasOption(OperationOption.KEEP_ALIVE);
    }

    public Operation setKeepAlive(boolean isKeepAlive) {
        allocateRemoteContext();
        toggleOption(OperationOption.KEEP_ALIVE, isKeepAlive);
        return this;
    }

    /**
     * Sets a tag used to pool out bound connections together. The service request client uses this
     * tag as a hint, in addition with the operation URI host name and port, to determine connection
     * re-use for requests
     */
    public Operation setConnectionTag(String tag) {
        allocateRemoteContext();
        this.remoteCtx.connectionTag = tag;
        return this;
    }

    public String getConnectionTag() {
        return this.remoteCtx == null ? null : this.remoteCtx.connectionTag;
    }

    public Operation toggleOption(OperationOption option, boolean enable) {
        if (enable) {
            this.options.add(option);
        } else {
            this.options.remove(option);
        }
        return this;
    }

    public boolean hasOption(OperationOption option) {
        return this.options.contains(option);
    }

    public EnumSet<OperationOption> getOptions() {
        return EnumSet.copyOf(this.options);
    }

    void setHandlerInvokeTime(long nowMicros) {
        allocateInstrumentationContext();
        this.instrumentationCtx.handleInvokeTimeMicros = nowMicros;
    }

    void setEnqueueTime(long nowMicros) {
        allocateInstrumentationContext();
        this.instrumentationCtx.enqueueTimeMicros = nowMicros;
    }

    void setHandlerCompletionTime(long nowMicros) {
        allocateInstrumentationContext();
        this.instrumentationCtx.handlerCompletionTimeMicros = nowMicros;
    }

    void setDocumentStoreCompletionTime(long nowMicros) {
        allocateInstrumentationContext();
        this.instrumentationCtx.documentStoreCompletionTimeMicros = nowMicros;
    }

    void setCompletionTime(long nowMicros) {
        allocateInstrumentationContext();
        this.instrumentationCtx.operationCompletionTimeMicros = nowMicros;
    }

    private void allocateInstrumentationContext() {
        if (this.instrumentationCtx != null) {
            return;
        }
        this.instrumentationCtx = new InstrumentationContext();
    }

    InstrumentationContext getInstrumentationContext() {
        return this.instrumentationCtx;
    }

    /**
     * Toggles logging of failures.
     * The default is to log failures on all operations if no completion is supplied, or
     * if the operation is a service start operation. To disable the default failure
     * logs invoke this method passing true for the argument.
     */
    public Operation disableFailureLogging(boolean disable) {
        toggleOption(OperationOption.FAILURE_LOGGING_DISABLED, disable);
        return this;
    }

    public boolean isFailureLoggingDisabled() {
        return hasOption(OperationOption.FAILURE_LOGGING_DISABLED);
    }

    public Operation setReplicationDisabled(boolean disable) {
        toggleOption(OperationOption.REPLICATION_DISABLED, disable);
        return this;
    }

    public boolean isReplicationDisabled() {
        return hasOption(OperationOption.REPLICATION_DISABLED);
    }

    /**
     * Prefer using {@link #getRequestHeader(String)} for retrieving entries
     * and {@link #addRequestHeader(String, String)} for adding entries.
     */
    public Map<String, String> getRequestHeaders() {
        allocateRemoteContext();
        allocateRequestHeaders();
        return this.remoteCtx.requestHeaders;
    }

    /**
     * Prefer using {@link #getResponseHeader(String)} for retrieving entries
     * and {@link #addResponseHeader(String, String)} for adding entries.
     */
    public Map<String, String> getResponseHeaders() {
        allocateRemoteContext();
        allocateResponseHeaders();
        return this.remoteCtx.responseHeaders;
    }

    public boolean hasResponseHeaders() {
        return this.remoteCtx != null && this.remoteCtx.responseHeaders != null
                && !this.remoteCtx.responseHeaders.isEmpty();
    }

    public boolean hasRequestHeaders() {
        return this.remoteCtx != null && this.remoteCtx.requestHeaders != null
                && !this.remoteCtx.requestHeaders.isEmpty();
    }

    public String getRequestHeader(String headerName) {
        return getRequestHeader(headerName, true);
    }

    /**
     * Retrieves header from request headers, skipping normalization
     */
    public String getRequestHeaderAsIs(String headerName) {
        return getRequestHeader(headerName, false);
    }

    /**
     * Retrieves and removes header from request headers, skipping normalization
     */
    public String getAndRemoveRequestHeaderAsIs(String headerName) {
        if (!hasRequestHeaders()) {
            return null;
        }
        return this.remoteCtx.requestHeaders.remove(headerName);
    }

    private String getRequestHeader(String headerName, boolean normalize) {
        if (!hasRequestHeaders()) {
            return null;
        }
        String value = this.remoteCtx.requestHeaders.get(headerName);
        if (!normalize) {
            return value;
        }

        if (value == null) {
            value = this.remoteCtx.requestHeaders.get(headerName.toLowerCase());
            if (value == null) {
                return null;
            }
        }
        return removeString(value.trim(), CR_LF);
    }

    public String getResponseHeader(String headerName) {
        return getResponseHeader(headerName, true);
    }

    /**
     * Retrieves header from response headers, skipping normalization
     */
    public String getResponseHeaderAsIs(String headerName) {
        return getResponseHeader(headerName, false);
    }

    /**
     * Retrieves and removes header from response headers, skipping normalization
     */
    public String getAndRemoveResponseHeaderAsIs(String headerName) {
        if (!hasResponseHeaders()) {
            return null;
        }
        return this.remoteCtx.responseHeaders.remove(headerName);
    }

    private String getResponseHeader(String headerName, boolean normalize) {
        if (!hasResponseHeaders()) {
            return null;
        }
        String value = this.remoteCtx.responseHeaders.get(headerName);
        if (!normalize) {
            return value;
        }

        if (value == null) {
            value = this.remoteCtx.responseHeaders.get(headerName.toLowerCase());
            if (value == null) {
                return null;
            }
        }
        return removeString(value.trim(), CR_LF);
    }

    public Principal getPeerPrincipal() {
        return this.remoteCtx == null ? null : this.remoteCtx.peerPrincipal;
    }

    public X509Certificate[] getPeerCertificateChain() {
        return this.remoteCtx == null ? null : this.remoteCtx.peerCertificateChain;
    }

    public void setPeerCertificates(Principal peerPrincipal, X509Certificate[] certificates) {
        if (this.remoteCtx != null) {
            this.remoteCtx.peerPrincipal = peerPrincipal;
            this.remoteCtx.peerCertificateChain = certificates;
        }
    }

    /**
     * Infrastructure use only. Used by service request client logic.
     *
     * First decrements the retry count then returns the current value
     */
    public int decrementRetriesRemaining() {
        return --this.retriesRemaining;
    }

    /**
     * Value indicating whether the target service is replicated and might not yet be available
     * locally
     *
     * @return
     */
    public boolean isTargetReplicated() {
        return hasOption(OperationOption.REPLICATED_TARGET);
    }

    /**
     * Infrastructure use only
     */
    public Operation setFromReplication(boolean isFromReplication) {
        toggleOption(OperationOption.REPLICATED, isFromReplication);
        return this;
    }

    /**
     * Infrastructure use only.
     *
     * Value indicating whether this operation was created to apply locally a remote update
     */
    public boolean isFromReplication() {
        return hasOption(OperationOption.REPLICATED);
    }

    /**
     * Infrastructure use only
     */
    public Operation setConnectionSharing(boolean enable) {
        toggleOption(OperationOption.CONNECTION_SHARING, enable);
        return this;
    }

    /**
     * Infrastructure use only.
     *
     * Value indicating whether this operation is sharing a connection
     */
    public boolean isConnectionSharing() {
        return hasOption(OperationOption.CONNECTION_SHARING);
    }

    /**
     * Infrastructure use only.
     *
     * Value indicating whether this operation was forwarded from a peer node
     */
    public boolean isForwarded() {
        return this.hasOption(OperationOption.FORWARDED);
    }

    /**
     * Copies response headers from the operation supplied as the argument, to this instance. Any
     * headers with the same name already present on this instance will be overwritten.
     */
    public Operation transferResponseHeadersFrom(Operation op) {
        if (!op.hasResponseHeaders()) {
            return this;
        }

        allocateRemoteContext();
        allocateResponseHeaders();
        this.remoteCtx.responseHeaders.putAll(op.getResponseHeaders());
        return this;
    }

    /**
     * Copies request headers from the operation supplied as the argument, to this instance. Any
     * headers with the same name already present on this instance will be overwritten.
     */
    public Operation transferRequestHeadersFrom(Operation op) {
        if (!op.hasRequestHeaders()) {
            return this;
        }

        allocateRemoteContext();
        allocateRequestHeaders();
        this.remoteCtx.requestHeaders.putAll(op.getRequestHeaders());
        return this;
    }

    public Operation transferResponseHeadersToRequestHeadersFrom(Operation op) {
        if (!op.hasResponseHeaders()) {
            return this;
        }

        allocateRemoteContext();
        allocateRequestHeaders();
        this.remoteCtx.requestHeaders.putAll(op.getResponseHeaders());
        return this;
    }

    public Operation transferRequestHeadersToResponseHeadersFrom(Operation op) {
        if (!op.hasRequestHeaders()) {
            return this;
        }

        allocateRemoteContext();
        allocateResponseHeaders();
        this.remoteCtx.responseHeaders.putAll(op.getRequestHeaders());
        return this;
    }

    public boolean isNotification() {
        return hasPragmaDirective(PRAGMA_DIRECTIVE_NOTIFICATION);
    }

    public Operation setNotificationDisabled(boolean disable) {
        toggleOption(OperationOption.NOTIFICATION_DISABLED, disable);
        return this;
    }

    public boolean isNotificationDisabled() {
        return hasOption(OperationOption.NOTIFICATION_DISABLED);
    }

    public boolean isForwardingDisabled() {
        return hasPragmaDirective(PRAGMA_DIRECTIVE_NO_FORWARDING);
    }

    /**
     * Infrastructure use only.
     *
     * Value indicating whether this operation is a commit phase replication request.
     */
    public boolean isCommit() {
        String phase = getRequestHeader(Operation.REPLICATION_PHASE_HEADER, false);
        return Operation.REPLICATION_PHASE_COMMIT.equals(phase);
    }

    /**
     * Indicate whether this operation is a synchronization operation.
     */
    public boolean isSynchronize() {
        return isSynchronizeOwner() || isSynchronizePeer();
    }

    public boolean isSynchronizeOwner() {
        return hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER);
    }

    public boolean isSynchronizePeer() {
        return hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_PEER);
    }

    public boolean isUpdate() {
        return this.getAction() == Action.PUT || this.getAction() == Action.PATCH;
    }

    /**
     * Infrastructure use only
     */
    void linkSerializedState(byte[] data) {
        this.linkedSerializedState = data;
    }

}
