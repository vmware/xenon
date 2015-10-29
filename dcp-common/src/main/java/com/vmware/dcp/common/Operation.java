/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.common;

import java.net.HttpURLConnection;
import java.net.URI;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

import com.vmware.dcp.common.Service.Action;
import com.vmware.dcp.common.ServiceDocumentDescription.Builder;
import com.vmware.dcp.services.common.QueryFilter;
import com.vmware.dcp.services.common.QueryTask.Query;
import com.vmware.dcp.services.common.SystemUserService;

/**
 * Service operation container. Encapsulates the request / response pattern of client to service and
 * service to service asynchronous communication
 */
public class Operation implements Cloneable {

    public static class SocketContext {
        public static final int MAX_REQUEST_SIZE = 1024 * 1024 * 16;

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
    }

    static class InstrumentationContext {
        public long handleInvokeTimeMicrosUtc;
        public long enqueueTimeMicrosUtc;
        public long documentStoreCompletionTimeMicrosUtc;
        public long handlerCompletionTime;
        public long operationCompletionTimeMicrosUtc;
    }

    public static enum OperationOption {
        REPLICATED, REPLICATION_DISABLED, REMOTE, NOTIFICATION_DISABLED, REPLICATED_TARGET
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

        public static final String KIND = Utils.buildKind(Operation.SerializedOperation
                .class);

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

    /**
     * Operation metadata being sent to the transaction coordinator.
     */
    public static class OperationTransactionRecord {

        /**
         * Action the service received
         */
        public Action action;

        /**
         * Set of pending transactions on the same service
         */
        public Set<String> pendingTransactions;

        /**
         * Notify whether the service completed (true) or failed (false) the operation
         */
        public boolean isSuccessful;
    }

    @FunctionalInterface
    public interface CompletionHandler {
        void handle(Operation completedOp, Throwable failure);
    }

    static class RemoteContext {
        public SocketContext socketCtx;
        public Map<String, String> requestHeaders = new HashMap<>();
        public Map<String, String> responseHeaders = new HashMap<>();
        public Principal peerPrincipal;
        public X509Certificate[] peerCertificateChain;
        public boolean isKeepAlive;
        public int headerByteCount;
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
        private Query resourceQuery = null;

        /**
         * The resource query filter is a query filter of the composite query
         * constructed by grouping all resource group queries that apply to
         * this user's authorization context.
         */
        private QueryFilter resourceQueryFilter = null;

        public Claims getClaims() {
            return this.claims;
        }

        public String getToken() {
            return this.token;
        }

        public boolean shouldPropagateToClient() {
            return this.propagateToClient;
        }

        public Query getResourceQuery() {
            return Utils.clone(this.resourceQuery);
        }

        public QueryFilter getResourceQueryFilter() {
            return this.resourceQueryFilter;
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

            public Builder setResourceQuery(Query resourceQuery) {
                this.authorizationContext.resourceQuery = resourceQuery;
                return this;
            }

            public Builder setResourceQueryFilter(QueryFilter resourceQueryFilter) {
                this.authorizationContext.resourceQueryFilter = resourceQueryFilter;
                return this;
            }
        }
    }

    public static final String REFERER_HEADER = "Referer";
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String CONTENT_RANGE_HEADER = "Content-Range";
    public static final String RANGE_HEADER = "Range";

    public static final String VMWARE_DCP_HEADER_NAME_PREFIX = "X-VMware-DCP-";

    public static final String CONTEXT_ID_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX + "Context-Id";

    public static final String PRAGMA_HEADER = "Pragma";

    public static final String SET_COOKIE_HEADER = "Set-Cookie";

    public static final String PRAGMA_DIRECTIVE_NO_QUEUING = "dcp-no-queuing";
    public static final String PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY = "dcp-queue-for-service-availability";
    public static final String PRAGMA_DIRECTIVE_NO_FORWARDING = "dcp-no-forwarding";
    public static final String PRAGMA_DIRECTIVE_NOTIFICATION = "dcp-notification";
    public static final String PRAGMA_DIRECTIVE_SKIPPED_NOTIFICATIONS = "dcp-notifications-skipped";
    public static final String PRAGMA_DIRECTIVE_INDEX_CHECK = "dcp-check-index";
    public static final String PRAGMA_DIRECTIVE_VERSION_CHECK = "dcp-check-version";

    /**
     * Infrastructure use only. Instructs a persisted service to complete the operation but skip any index
     * updates.
     */
    public static final String PRAGMA_DIRECTIVE_NO_INDEX_UPDATE = "dcp-no-index-update";

    public static final String REQUEST_AUTH_TOKEN_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX
            + "Auth-Token";
    public static final String REQUEST_CALLBACK_LOCATION_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX
            + "Request-Callback-Location";
    public static final String RESPONSE_CALLBACK_STATUS_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX
            + "Response-Callback-Status";

    public static final String ENTRY_NODE_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX
            + "Entry-Node";

    public static final String REPLICATION_TARGET_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX
            + "Replication-Target";

    public static final String REPLICATION_PHASE_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX
            + "Replication-Phase";

    public static final String VMWARE_DCP_TRANSACTION_HEADER = VMWARE_DCP_HEADER_NAME_PREFIX
            + "Transaction-Phase";

    public static final String TX_TRY_COMMIT = "try-commit";
    public static final String TX_ENSURE_COMMIT = "ensure-commit";
    public static final String TX_COMMIT = "commit";
    public static final String TX_ABORT = "abort";

    public static final String REPLICATION_PHASE_COMMIT = "commit";

    public static final String REPLICATION_PHASE_SYNCHRONIZE = "synchronize";

    public static final String LOCATION_HEADER = "Location";

    public static final String USER_AGENT_HEADER = "User-Agent";

    public static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";
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
    private static AtomicReferenceFieldUpdater<Operation, CompletionHandler> completionUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Operation.class, CompletionHandler.class,
                    "completion");

    private URI uri;
    private URI referer;
    private final long id = idCounter.incrementAndGet();
    private int statusCode = HttpURLConnection.HTTP_OK;
    private Action action;
    private ServiceDocument linkedState;
    private volatile CompletionHandler completion;
    private RemoteContext remoteCtx;
    private AuthorizationContext authorizationCtx;
    private String contextId;
    private String transactionId;
    private long expirationMicrosUtc;
    private Object body;
    private Object serializedBody;
    private String contentType = MEDIA_TYPE_APPLICATION_JSON;
    private long contentLength;
    private Map<String, String> cookies;
    private int retryCount;
    private int retriesRemaining;
    private InstrumentationContext instrumentationCtx;
    public EnumSet<OperationOption> options = EnumSet.noneOf(OperationOption.class);

    volatile OperationJoin joinHandler;

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
        return createPost(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createPost(URI uri) {
        return createOperation(Action.POST, uri);
    }

    public static Operation createPatch(Service sender, String targetPath) {
        return createPatch(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createPatch(URI uri) {
        return createOperation(Action.PATCH, uri);
    }

    public static Operation createPut(Service sender, String targetPath) {
        return createPut(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createPut(URI uri) {
        return createOperation(Action.PUT, uri);
    }

    public static Operation createDelete(Service sender, String targetPath) {
        return createDelete(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createDelete(URI uri) {
        return createOperation(Action.DELETE, uri);
    }

    public static Operation createGet(Service sender, String targetPath) {
        return createGet(UriUtils.buildUri(sender.getHost(), targetPath));
    }

    public static Operation createGet(URI uri) {
        return createOperation(Action.GET, uri);
    }

    @Override
    public String toString() {
        SerializedOperation sop = SerializedOperation.create(this);
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
        clone.joinHandler = this.joinHandler;

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
                clone.remoteCtx.peerCertificateChain =
                        Arrays.copyOf(this.remoteCtx.peerCertificateChain,
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
    void setAuthorizationContext(AuthorizationContext ctx) {
        this.authorizationCtx = ctx;
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
        // TODO If operation is remote convert directly to JSON
        if (body != null) {
            this.body = Utils.clone(body);
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
            if (this.contentType != null && this.contentType.contains(MEDIA_TYPE_APPLICATION_JSON)) {
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

    public Operation setRetryCount(int retryCount) {
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount must be positive");
        }
        this.retryCount = retryCount;
        this.retriesRemaining = retryCount;
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
        if (serviceDoc != null && this.linkedState != null && this.linkedState.documentKind != null) {
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
            try {
                ServiceErrorResponse rsp = Utils.fromJson(this.body, ServiceErrorResponse.class);
                if (rsp.message != null) {
                    hasErrorResponseBody = true;
                }
            } catch (Throwable ex) {
                // the body is not JSON, ignore
            }
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

        if (!completionUpdater.compareAndSet(this, c, (CompletionHandler) null)) {
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
        final CompletionHandler existing = this.completion;

        setCompletion((o, e) -> {
            setCompletion(existing);
            setStatusCode(o.getStatusCode());
            h.handle(o, e);
        });
        return this;
    }

    public void nestCompletion(Consumer<Operation> successHandler) {
        final CompletionHandler existing = this.completion;

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
        this.remoteCtx.requestHeaders.put(name, value);
        return this;
    }

    public Operation addResponseHeader(String name, String value) {
        allocateRemoteContext();
        value = value.replace(CR_LF, "");
        this.remoteCtx.responseHeaders.put(name, value);
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
        String existingDirectives = getRequestHeader(PRAGMA_HEADER);
        if (existingDirectives != null && !existingDirectives.contains(directive)) {
            directive = existingDirectives + ";" + directive;
        }
        addRequestHeader(PRAGMA_HEADER, directive);
        return this;
    }

    public boolean hasPragmaDirective(String directive) {
        String existingDirectives = getRequestHeader(PRAGMA_HEADER);
        if (existingDirectives != null
                && existingDirectives.contains(directive)) {
            return true;
        }
        return false;
    }

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
        String value = this.remoteCtx.requestHeaders.get(headerName);
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
        String value = this.remoteCtx.responseHeaders.get(headerName);
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

    public int decrementRetriesRemaining() {
        return this.retriesRemaining--;
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
     * Join multiple {@link Operation}s together so that all {@link CompletionHandler} are invoked
     * after all joined {@link Operation}s have completed.
     *
     * @param op
     *            - the {@link Operation} to be joined with the current operation.
     *
     * @return the {@link Operation} passed as a parameter.
     */
    public Operation joinWith(Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("'operation' is required.");
        }

        if (this.joinHandler == null) {
            this.joinHandler = OperationJoin.create(this, op);
            return op;
        }

        if (this.joinHandler.getOperation(this.id) != this) {
            this.joinHandler.join(this);// reset the cloned operations
        }

        this.joinHandler.join(op);
        op.joinHandler = this.joinHandler;
        return op;
    }

    /**
     * Join the completion handlers during {@link ServiceClient#send(Operation)}.
     */
    public void joinCompletionHandlers() {
        this.joinHandler.addCompletionHandler(this);
    }

    /**
     * Indicates whether this is a single {@link Operation} instance or it is part of a join set of
     * {@link Operation}s that are proceeded together.
     *
     * @return true if the {@link Operation} has joined other {@link Operation}s; false otherwise;
     */
    public boolean isJoined() {
        return this.joinHandler != null && !this.joinHandler.isEmpty();
    }

    OperationJoin resetJoinHandler(OperationJoin joinHandler) {
        OperationJoin oj = this.joinHandler;
        this.joinHandler = joinHandler;
        return oj;
    }

    /**
     * Get a collection of all joined {@link Operation}s.
     *
     * @return a collection of all joined {@link Operation}s or a singleton collection of the
     *         current {@link Operation} if not in joined mode.
     */
    public Collection<Operation> getJoinedOperations() {
        if (isJoined()) {
            return this.joinHandler.getOperations();
        }
        return Collections.singleton(this);
    }

    /**
     * Get the cloned {@link Operation} based on the original {@link Operation#getId()} usually to
     * be used within the {@link CompletionHandler} in order to get the operation result from the
     * cloned {@link Operation} with the same <code>id</code>.
     *
     * @param op
     *            the initial {@link Operation} instance.
     *
     * @return the {@link Operation} with the same <code>id</code> base on the current context.
     */
    public Operation getJoinedOperation(Operation op) {
        if (op != null && isJoined()) {
            return this.joinHandler.getOperation(op.getId());
        }
        return null;
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
}
