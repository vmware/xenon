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

import java.net.HttpURLConnection;
import java.net.URI;
import java.security.Principal;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import javax.security.cert.X509Certificate;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
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

        public SocketContext() {
        }

        private long lastUseTimeMicros;

        public long getLastUseTimeMicros() {
            return this.lastUseTimeMicros;
        }

        public void updateLastUseTime() {
            this.lastUseTimeMicros = Utils.getNowMicrosUtc();
        }

        public void writeHttpRequest(Object request) {
            throw new IllegalStateException();
        }

        public void close() {
            throw new IllegalStateException();
        }

        private static int maxRequestSize = 1024 * 1024 * 16;

        private static final int MAX_CLIENT_REQUEST_SIZE = 1024 * 1024 * 128;

        /**
         * Set maximum request/response size for socket I/O.
         * Note that this has to be called very early before client / listener initialize.
         * @param max size in bytes
         */
        public static void setMaxRequestSize(int max) {
            maxRequestSize = max;
        }

        public static int getMaxRequestSize() {
            return maxRequestSize;
        }

        public static int getMaxClientRequestSize() {
            return MAX_CLIENT_REQUEST_SIZE;
        }
    }

    static class InstrumentationContext {
        public long handleInvokeTimeMicrosUtc;
        public long enqueueTimeMicrosUtc;
        public long documentStoreCompletionTimeMicrosUtc;
        public long handlerCompletionTime;
        public long operationCompletionTimeMicrosUtc;
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
        public SocketContext socketCtx;
        public Map<String, String> requestHeaders = new HashMap<>();
        public Map<String, String> responseHeaders = new HashMap<>();
        public Principal peerPrincipal;
        public X509Certificate[] peerCertificateChain;
        public boolean isKeepAlive;
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
            Claims claims = getClaims();
            if (claims == null) {
                return false;
            }

            String subject = claims.getSubject();
            if (subject == null) {
                return false;
            }

            return subject.equals(SystemUserService.SELF_LINK);
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

            public Builder setResourceQueryFilterMap(Map<Action, QueryFilter> resourceQueryFiltersMap) {
                this.authorizationContext.resourceQueryFiltersMap = resourceQueryFiltersMap;
                return this;
            }
        }
    }

    public static enum OperationOption {
        REPLICATED, REPLICATION_DISABLED, CLONING_DISABLED, NOTIFICATION_DISABLED, REPLICATED_TARGET
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

        public static final ServiceDocumentDescription DESCRIPTION = Operation.SerializedOperation
                .buildDescription();

        public static final String KIND = Utils.buildKind(Operation.SerializedOperation.class);

        public static SerializedOperation create(Operation op) {
            SerializedOperation ctx = new SerializedOperation();
            ctx.contextId = op.getContextId();
            ctx.action = op.action;
            ctx.referer = op.referer;
            ctx.id = op.id;
            ctx.options = op.options.clone();
            ctx.transactionId = op.getTransactionId();
            if (op.uri != null) {
                ctx.host = op.uri.getHost();
                ctx.port = op.uri.getPort();
                ctx.path = op.uri.getPath();
                ctx.query = op.uri.getQuery();
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

    // HTTP Header definitions
    public static final String REFERER_HEADER = "referer";
    public static final String CONTENT_TYPE_HEADER = "content-type";
    public static final String CONTENT_RANGE_HEADER = "content-range";
    public static final String RANGE_HEADER = "range";
    public static final String RETRY_AFTER_HEADER = "retry-after";
    public static final String PRAGMA_HEADER = "pragma";
    public static final String SET_COOKIE_HEADER = "set-cookie";
    public static final String LOCATION_HEADER = "location";
    public static final String USER_AGENT_HEADER = "user-agent";
    public static final String ACCEPT_HEADER = "accept";

    // Proprietary header definitions
    public static final String HEADER_NAME_PREFIX = "x-xenon-";
    public static final String CONTEXT_ID_HEADER = HEADER_NAME_PREFIX + "ctx-id";
    public static final String REQUEST_CALLBACK_LOCATION_HEADER = HEADER_NAME_PREFIX
            + "req-location";
    public static final String RESPONSE_CALLBACK_STATUS_HEADER = HEADER_NAME_PREFIX
            + "rsp-status";
    public static final String REQUEST_AUTH_TOKEN_HEADER = HEADER_NAME_PREFIX
            + "auth-token";
    public static final String REPLICATION_PHASE_HEADER = HEADER_NAME_PREFIX
            + "rpl-phase";
    public static final String REPLICATION_QUORUM_HEADER = HEADER_NAME_PREFIX
            + "rpl-quorum";

    public static final String REPLICATION_QUORUM_HEADER_VALUE_ALL = HEADER_NAME_PREFIX
            + "all";
    public static final String TRANSACTION_HEADER = HEADER_NAME_PREFIX
            + "tx-phase";
    public static final String TRANSACTION_ID_HEADER = HEADER_NAME_PREFIX + "tx-id";

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
     * Infrastructure use only. Set when the consensus protocol determines that a service instance,
     * on the owner node, must synchronize its state with peers. Associated with a PUT action.
     */
    public static final String PRAGMA_DIRECTIVE_SYNCH = "xn-synch";

    /**
     * Advanced use. Prevents the request from getting queued, if a service is not yet available.
     */
    public static final String PRAGMA_DIRECTIVE_NO_QUEUING = "xn-no-queuing";

    /**
     * Advanced use. Instructs the runtime to queue a request, for a service to become available
     * independent of the service options.
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
     * Infrastructure use only. Experimental. Forces the use of HTTP2 streams.
     */
    public static final String PRAGMA_DIRECTIVE_USE_HTTP2 = "xn-use-http2";

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
     * Infrastructure use only. Debugging only. Indicates this operation was converted from POST to PUT
     * due to {@link ServiceOption.IDEMPOTENT_POST}
     */
    public static final String PRAGMA_DIRECTIVE_POST_TO_PUT = "xn-post-to-put";

    public static final String TX_TRY_COMMIT = "try-commit";
    public static final String TX_ENSURE_COMMIT = "ensure-commit";
    public static final String TX_COMMIT = "commit";
    public static final String TX_ABORT = "abort";
    public static final String REPLICATION_PHASE_COMMIT = "commit";

    public static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";
    public static final String MEDIA_TYPE_TEXT_YAML = "text/x-yaml";
    public static final String MEDIA_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED = "application/x-www-form-urlencoded";
    public static final String MEDIA_TYPE_TEXT_HTML = "text/html";
    public static final String MEDIA_TYPE_TEXT_PLAIN = "text/plain";
    public static final String MEDIA_TYPE_TEXT_CSS = "text/css";
    public static final String MEDIA_TYPE_APPLICATION_JAVASCRIPT = "application/javascript";
    public static final String MEDIA_TYPE_IMAGE_SVG_XML = "image/svg+xml";
    public static final String MEDIA_TYPE_APPLICATION_FONT_WOFF2 = "application/font-woff2";

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
    public static final int STATUS_CODE_ACCEPTED = HttpURLConnection.HTTP_ACCEPTED;
    public static final int STATUS_CODE_BAD_REQUEST = HttpURLConnection.HTTP_BAD_REQUEST;
    public static final int STATUS_CODE_BAD_METHOD = HttpURLConnection.HTTP_BAD_METHOD;

    public static final String MEDIA_TYPE_EVERYTHING_WILDCARDS = "*/*";
    public static final String EMPTY_JSON_BODY = "{}";
    public static final String HEADER_FIELD_VALUE_SEPARATOR = ":";
    public static final String CR_LF = "\r\n";

    private static AtomicLong idCounter = new AtomicLong();
    private static AtomicReferenceFieldUpdater<Operation, CompletionHandler> completionUpdater = AtomicReferenceFieldUpdater
            .newUpdater(Operation.class, CompletionHandler.class,
                    "completion");

    private URI uri;
    private URI referer;
    private final long id = idCounter.incrementAndGet();
    private int statusCode = HttpURLConnection.HTTP_OK;
    private Action action;
    private ServiceDocument linkedState;
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
    private Map<String, String> cookies;
    private short retryCount;
    private short retriesRemaining;

    public EnumSet<OperationOption> options = EnumSet.noneOf(OperationOption.class);

    public static Operation create(SerializedOperation ctx, ServiceHost host) {
        Operation op = new Operation();
        op.action = ctx.action;
        op.body = ctx.jsonBody;
        op.expirationMicrosUtc = ctx.documentExpirationTimeMicros;
        op.setContextId(ctx.id.toString());
        op.referer = ctx.referer;
        op.uri = UriUtils.buildUri(host, ctx.path, ctx.query);
        op.transactionId = ctx.transactionId;
        return op;
    }

    static Operation createOperation(Action action, URI uri) {
        Operation op = new Operation();
        op.uri = uri;
        op.action = action;

        // Set authorization context from thread local.
        // The thread local is populated by the service host when it handles an operation,
        // which means that derivative operations will automatically inherit this context.
        // It is set as early as possible since there is a possibility that it is
        // overridden by the service implementation (i.e. when it impersonates).
        op.authorizationCtx = OperationContext.getAuthorizationContext();

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

    @Override
    public Operation clone() {
        Operation clone;
        try {
            clone = (Operation) super.clone();
        } catch (CloneNotSupportedException e) {
            clone = new Operation();
        }

        clone.options = EnumSet.copyOf(this.options);
        clone.action = this.action;
        clone.completion = this.completion;
        clone.expirationMicrosUtc = this.expirationMicrosUtc;
        clone.referer = this.referer;
        clone.uri = this.uri;
        clone.contentLength = this.contentLength;
        clone.contentType = this.contentType;
        clone.retriesRemaining = this.retriesRemaining;
        clone.retryCount = this.retryCount;

        if (this.cookies != null) {
            clone.cookies = new HashMap<>(this.cookies);
        }

        if (this.remoteCtx != null) {
            clone.remoteCtx = new RemoteContext();
            // do not clone socket context
            clone.remoteCtx.socketCtx = null;
            if (!this.remoteCtx.requestHeaders.isEmpty()) {
                clone.remoteCtx.requestHeaders = new HashMap<>(this.remoteCtx.requestHeaders);
            }
            clone.remoteCtx.peerPrincipal = this.remoteCtx.peerPrincipal;
            if (this.remoteCtx.peerCertificateChain != null) {
                clone.remoteCtx.peerCertificateChain = Arrays.copyOf(
                        this.remoteCtx.peerCertificateChain,
                        this.remoteCtx.peerCertificateChain.length);
            }
        }

        // Direct copy of authorization context; it is immutable
        clone.authorizationCtx = this.authorizationCtx;
        clone.transactionId = this.transactionId;
        clone.contextId = this.contextId;

        // body is always cloned on set, so no need to re-clone
        clone.body = this.body;
        return clone;
    }

    private void allocateRemoteContext() {
        if (this.remoteCtx != null) {
            return;
        }
        this.remoteCtx = new RemoteContext();
    }

    public boolean isRemote() {
        return this.remoteCtx != null && this.remoteCtx.socketCtx != null;
    }

    public Operation forceRemote() {
        allocateRemoteContext();
        this.remoteCtx.socketCtx = new SocketContext();
        return this;
    }

    public AuthorizationContext getAuthorizationContext() {
        return this.authorizationCtx;
    }

    /**
     * Sets (overwrites) the authorization context of this operation.
     *
     * The visibility of this method is intentionally package-local. It is intended to
     * only be called by functions in this package, so that we can apply whitelisting
     * to limit the set of services that is able to set it.
     *
     * @param ctx the authorization context to set.
     */
    Operation setAuthorizationContext(AuthorizationContext ctx) {
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
            if (isCloningDisabled()) {
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

    /**
     * Deserializes the body associated with the operation, given the type.
     *
     * Note: This method is *not* idempotent. It will modify the body contents
     * so subsequent calls will not have access to the original instance. This
     * occurs only for local operations, not operations that have a serialized
     * body already attached (in the form of a JSON string).
     *
     * If idempotent behavior is desired, use {@link getBodyRaw}
     */
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> type) {
        if (this.body != null && this.body.getClass() == type) {
            return (T) this.body;
        }

        if (this.body != null && !(this.body instanceof String)) {

            if (this.isRemote()
                    && (this.contentType == null || !this.contentType
                            .contains(MEDIA_TYPE_APPLICATION_JSON))) {
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
                    throw new IllegalArgumentException("Unparseable JSON body: " + e.getMessage());
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

    public void setCookies(Map<String, String> cookies) {
        this.cookies = cookies;
    }

    public Map<String, String> getCookies() {
        return this.cookies;
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

    public Operation setReferer(URI uri) {
        this.referer = uri;
        return this;
    }

    public URI getReferer() {
        return this.referer;
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

    public void fail(int statusCode) {
        setStatusCode(statusCode);
        switch (statusCode) {
        case STATUS_CODE_FORBIDDEN:
            fail(new IllegalAccessError("forbidden"));
            break;
        default:
            fail(new Exception("request failed, no additional details provided"));
            break;
        }
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
                } catch (Throwable ex) {
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
                rsp = Utils.toValidationErrorResponse(e);
            } else {
                rsp = Utils.toServiceErrorResponse(e);
            }
            rsp.statusCode = this.statusCode;
            setBodyNoCloning(rsp).setContentType(
                    Operation.MEDIA_TYPE_APPLICATION_JSON);
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

        // Keep track of current authorization context so that code AFTER "op.complete()"
        // or "op.fail()" retains its authorization context, and is not overwritten by
        // the one associated with "op" (which might be different.
        AuthorizationContext originalContext = OperationContext.getAuthorizationContext();
        OperationContext.setAuthorizationContext(this.getAuthorizationContext());

        try {
            OperationContext.setContextId(this.contextId);
            c.handle(this, e);
        } catch (Throwable outer) {
            Utils.logWarning("Uncaught failure inside completion: %s", Utils.toString(outer));
        }

        // Restore original context
        OperationContext.setAuthorizationContext(originalContext);
    }

    public boolean hasBody() {
        return this.body != null;
    }

    public Operation nestCompletion(CompletionHandler h) {
        CompletionHandler existing = this.completion;

        setCompletion((o, e) -> {
            setCompletion(existing);
            setStatusCode(o.getStatusCode());
            h.handle(o, e);
        });
        return this;
    }

    public void nestCompletion(Consumer<Operation> successHandler) {
        CompletionHandler existing = this.completion;

        setCompletion((o, e) -> {
            setCompletion(existing);
            if (e != null) {
                o.fail(e);
                return;
            }
            try {
                successHandler.accept(o);
            } catch (Throwable ex) {
                o.fail(ex);
            }
        });
    }

    Operation addHeader(String headerLine, boolean isResponse) {
        if (headerLine == null) {
            throw new IllegalArgumentException("headerLine is required");
        }

        int idx = headerLine.indexOf(HEADER_FIELD_VALUE_SEPARATOR);
        if (idx == -1 || idx < 3) {
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
        allocateRemoteContext();
        value = value.replace(CR_LF, "").trim();
        this.remoteCtx.requestHeaders.put(name.toLowerCase(), value);
        return this;
    }

    public Operation addResponseHeader(String name, String value) {
        allocateRemoteContext();
        value = value.replace(CR_LF, "");
        this.remoteCtx.responseHeaders.put(name.toLowerCase(), value);
        return this;
    }

    public Operation addResponseCookie(String key, String value) {
        StringBuilder buf = new StringBuilder()
                .append(key)
                .append('=')
                .append(value);
        addResponseHeader(SET_COOKIE_HEADER, buf.toString());
        return this;
    }

    public Operation addPragmaDirective(String directive) {
        allocateRemoteContext();
        directive = directive.toLowerCase();
        String existingDirectives = getRequestHeader(PRAGMA_HEADER);
        if (existingDirectives != null) {
            if (!existingDirectives.contains(directive)) {
                directive = existingDirectives + ";" + directive;
            } else {
                directive = existingDirectives;
            }
        }
        addRequestHeader(PRAGMA_HEADER, directive);
        return this;
    }

    /**
     * Checks if a directive is present. Lower case strings must be used.
     */
    public boolean hasPragmaDirective(String directive) {
        String existingDirectives = getRequestHeader(PRAGMA_HEADER);
        if (existingDirectives != null
                && existingDirectives.contains(directive)) {
            return true;
        }
        return false;
    }

    /**
     * Removes a directive. Lower case strings must be used
     */
    public Operation removePragmaDirective(String directive) {
        allocateRemoteContext();
        String existingDirectives = getRequestHeader(PRAGMA_HEADER);
        if (existingDirectives != null) {
            directive = existingDirectives.replace(directive, "");
        }
        addRequestHeader(PRAGMA_HEADER, directive);
        return this;
    }

    public boolean isKeepAlive() {
        return this.remoteCtx == null ? false : this.remoteCtx.isKeepAlive;
    }

    public Operation setKeepAlive(boolean isKeepAlive) {
        allocateRemoteContext();
        this.remoteCtx.isKeepAlive = isKeepAlive;
        return this;
    }

    void setHandlerInvokeTime(long nowMicrosUtc) {
        allocateInstrumentationContext();
        this.instrumentationCtx.handleInvokeTimeMicrosUtc = nowMicrosUtc;
    }

    void setEnqueueTime(long nowMicrosUtc) {
        allocateInstrumentationContext();
        this.instrumentationCtx.enqueueTimeMicrosUtc = nowMicrosUtc;
    }

    void setHandlerCompletionTime(long nowMicrosUtc) {
        allocateInstrumentationContext();
        this.instrumentationCtx.handlerCompletionTime = nowMicrosUtc;
    }

    void setDocumentStoreCompletionTime(long nowMicrosUtc) {
        allocateInstrumentationContext();
        this.instrumentationCtx.documentStoreCompletionTimeMicrosUtc = nowMicrosUtc;
    }

    void setCompletionTime(long nowMicrosUtc) {
        allocateInstrumentationContext();
        this.instrumentationCtx.operationCompletionTimeMicrosUtc = nowMicrosUtc;
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

    public Operation setReplicationDisabled(boolean disable) {
        if (disable) {
            this.options.add(OperationOption.REPLICATION_DISABLED);
        } else {
            this.options.remove(OperationOption.REPLICATION_DISABLED);
        }
        return this;
    }

    public boolean isReplicationDisabled() {
        return this.options.contains(OperationOption.REPLICATION_DISABLED);
    }

    public Map<String, String> getRequestHeaders() {
        if (this.remoteCtx == null) {
            return new HashMap<>();
        }
        return this.remoteCtx.requestHeaders;
    }

    public Map<String, String> getResponseHeaders() {
        if (this.remoteCtx == null) {
            return new HashMap<>();
        }
        return this.remoteCtx.responseHeaders;
    }

    public String getRequestHeader(String headerName) {
        if (this.remoteCtx == null) {
            return null;
        }
        if (this.remoteCtx.requestHeaders == null) {
            return null;
        }
        String value = this.remoteCtx.requestHeaders.get(headerName.toLowerCase());
        if (value != null) {
            value = value.trim().replace(CR_LF, "");
        }
        return value;
    }

    public String getResponseHeader(String headerName) {
        if (this.remoteCtx == null) {
            return null;
        }
        if (this.remoteCtx.responseHeaders == null) {
            return null;
        }
        String value = this.remoteCtx.responseHeaders.get(headerName.toLowerCase());
        if (value != null) {
            value = value.trim().replace(CR_LF, "");
        }
        return value;
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
     * Value indicating the service target is replicated and might not yet be available. Set this to
     * true to enable availability registration for a service that might not be locally present yet.
     * It prevents the operation failing with 404 (not found)
     *
     * @param enable - true or false
     * @return
     */
    public Operation setTargetReplicated(boolean enable) {
        if (enable) {
            this.options.add(OperationOption.REPLICATED_TARGET);
        } else {
            this.options.remove(OperationOption.REPLICATED_TARGET);
        }
        return this;
    }

    /**
     * Value indicating whether the target service is replicated and might not yet be available
     * locally
     *
     * @return
     */
    public boolean isTargetReplicated() {
        return this.options.contains(OperationOption.REPLICATED_TARGET);
    }

    /**
     * Infrastructure use only
     */
    public Operation setFromReplication(boolean isFromReplication) {
        if (isFromReplication) {
            this.options.add(OperationOption.REPLICATED);
        } else {
            this.options.remove(OperationOption.REPLICATED);
        }
        return this;
    }

    /**
     * Infrastructure use only.
     *
     * Value indicating whether this operation was created to apply locally a remote update
     */
    public boolean isFromReplication() {
        return this.options.contains(OperationOption.REPLICATED);
    }

    /**
     * Infrastructure use only.
     *
     * Value indicating whether this operation was forwarded from a peer node
     */
    public boolean isForwarded() {
        return this.hasPragmaDirective(PRAGMA_DIRECTIVE_FORWARDED);
    }

    public String getRequestCallbackLocation() {
        if (this.remoteCtx == null) {
            return null;
        }
        return this.remoteCtx.requestHeaders
                .get(REQUEST_CALLBACK_LOCATION_HEADER);
    }

    public String getResponseCallbackStatus() {
        if (this.remoteCtx == null) {
            return null;
        }
        return this.remoteCtx.requestHeaders
                .get(RESPONSE_CALLBACK_STATUS_HEADER);
    }

    public Operation removeRequestCallbackLocation() {
        allocateRemoteContext();
        this.remoteCtx.requestHeaders.remove(REQUEST_CALLBACK_LOCATION_HEADER);
        return this;
    }

    public Operation setRequestCallbackLocation(URI location) {
        allocateRemoteContext();
        this.remoteCtx.requestHeaders.put(REQUEST_CALLBACK_LOCATION_HEADER,
                location == null ? null : location.toString());
        return this;
    }

    public Operation setResponseCallbackStatus(int status) {
        allocateRemoteContext();
        this.remoteCtx.requestHeaders.put(RESPONSE_CALLBACK_STATUS_HEADER,
                Integer.toString(status));
        return this;
    }

    /**
     * Copies response headers from the operation supplied as the argument, to this instance. Any
     * headers with the same name already present on this instance will be overwritten.
     */
    public Operation transferResponseHeadersFrom(Operation op) {
        if (op.remoteCtx == null || op.remoteCtx.responseHeaders == null
                || op.remoteCtx.responseHeaders.isEmpty()) {
            return this;
        }

        allocateRemoteContext();
        for (Entry<String, String> e : op.getResponseHeaders().entrySet()) {
            this.remoteCtx.responseHeaders.put(e.getKey(), e.getValue());
        }
        return this;
    }

    /**
     * Copies request headers from the operation supplied as the argument, to this instance. Any
     * headers with the same name already present on this instance will be overwritten.
     */
    public Operation transferRequestHeadersFrom(Operation op) {
        if (op.remoteCtx == null || op.remoteCtx.requestHeaders == null
                || op.remoteCtx.requestHeaders.isEmpty()) {
            return this;
        }

        allocateRemoteContext();
        for (Entry<String, String> e : op.getRequestHeaders().entrySet()) {
            this.remoteCtx.requestHeaders.put(e.getKey(), e.getValue());
        }
        return this;
    }

    public Operation transferResponseHeadersToRequestHeadersFrom(Operation op) {
        if (op.remoteCtx == null || op.remoteCtx.responseHeaders == null
                || op.remoteCtx.responseHeaders.isEmpty()) {
            return this;
        }

        allocateRemoteContext();
        for (Entry<String, String> e : op.getResponseHeaders().entrySet()) {
            this.remoteCtx.requestHeaders.put(e.getKey(), e.getValue());
        }
        return this;
    }

    public Operation transferRequestHeadersToResponseHeadersFrom(Operation op) {
        if (op.remoteCtx == null || op.remoteCtx.requestHeaders == null
                || op.remoteCtx.requestHeaders.isEmpty()) {
            return this;
        }

        allocateRemoteContext();
        for (Entry<String, String> e : op.getRequestHeaders().entrySet()) {
            this.remoteCtx.responseHeaders.put(e.getKey(), e.getValue());
        }
        return this;
    }

    /**
     * Infrastructure use only.
     *
     * If cloning is disabled, setBody() will not
     * clone the supplied argument. Requests received from external clients
     * can avoid the overhead of cloning a response body by disabling cloning
     */
    public Operation setCloningDisabled(boolean disable) {
        this.options.add(OperationOption.CLONING_DISABLED);
        return this;
    }

    public boolean isCloningDisabled() {
        return this.options.contains(OperationOption.CLONING_DISABLED);
    }

    public boolean isNotification() {
        return hasPragmaDirective(PRAGMA_DIRECTIVE_NOTIFICATION);
    }

    public Operation setNotificationDisabled(boolean disable) {
        if (disable) {
            this.options.add(OperationOption.NOTIFICATION_DISABLED);
        } else {
            this.options.remove(OperationOption.NOTIFICATION_DISABLED);
        }
        return this;
    }

    public boolean isNotificationDisabled() {
        return this.options.contains(OperationOption.NOTIFICATION_DISABLED);
    }

    boolean isForwardingDisabled() {
        return hasPragmaDirective(PRAGMA_DIRECTIVE_NO_FORWARDING);
    }

    boolean isCommit() {
        String phase = getRequestHeader(Operation.REPLICATION_PHASE_HEADER);
        return Operation.REPLICATION_PHASE_COMMIT.equals(phase);
    }

    boolean isSynchronize() {
        return hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH);
    }
}
