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

import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.dcp.common.Operation.AuthorizationContext;
import com.vmware.dcp.common.Operation.InstrumentationContext;
import com.vmware.dcp.common.Operation.OperationTransactionRecord;
import com.vmware.dcp.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.dcp.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.dcp.common.ServiceStats.ServiceStat;
import com.vmware.dcp.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.dcp.common.jwt.Signer;
import com.vmware.dcp.services.common.QueryTask;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.dcp.services.common.ServiceUriPaths;
import com.vmware.dcp.services.common.TransactionService.ResolutionKind;
import com.vmware.dcp.services.common.TransactionService.ResolutionRequest;

/**
 * Service implementation class supporting a range of options. Supports lock free serialized
 * updates, replication, etc
 */
public class StatefulService implements Service {

    private static class RuntimeContext {
        public ProcessingStage processingStage = ProcessingStage.CREATED;
        public String selfLink;
        public long version;
        public long epoch;

        public EnumSet<ServiceOption> options = EnumSet.noneOf(ServiceOption.class);
        public Class<? extends ServiceDocument> stateType;
        public long maintenanceInterval;
        public OperationQueue operationQueue;
        public boolean isUpdateActive;
        public int getActiveCount;

        public transient ServiceHost host;
        public transient OperationProcessingChain opProcessingChain;

        public UtilityService utilityService;
        public String nodeSelectorLink = ServiceUriPaths.DEFAULT_NODE_SELECTOR;

        public Set<String> txCoordinatorLinks;
    }

    private static boolean isCommitRequest(Operation op) {
        String phase = op.getRequestHeader(Operation.REPLICATION_PHASE_HEADER);
        return Operation.REPLICATION_PHASE_COMMIT.equals(phase);
    }

    private static boolean isSynchronizeRequest(Operation op) {
        String phase = op.getRequestHeader(Operation.REPLICATION_PHASE_HEADER);
        return Operation.REPLICATION_PHASE_SYNCHRONIZE.equals(phase);
    }

    private RuntimeContext context = new RuntimeContext();

    /**
     * Infrastructure use. Called by utility derived classes to reduce memory footprint
     */
    void clearContext() {
        this.context = null;
    }

    public StatefulService(Class<? extends ServiceDocument> stateType) {
        if (stateType == null) {
            throw new IllegalArgumentException("stateType is required");
        }
        this.context.stateType = stateType;
        if (this.context.options.contains(ServiceOption.LIFO_QUEUE)) {
            this.context.operationQueue = OperationQueue
                    .createLifo(Service.OPERATION_QUEUE_DEFAULT_LIMIT);
        } else {
            this.context.operationQueue = OperationQueue
                    .createFifo(Service.OPERATION_QUEUE_DEFAULT_LIMIT);
        }
    }

    @Override
    public OperationProcessingChain getOperationProcessingChain() {
        return this.context.opProcessingChain;
    }

    @Override
    public Service.ProcessingStage getProcessingStage() {
        return this.context.processingStage;
    }

    @Override
    public void handleStart(Operation post) {
        post.complete();
    }

    /**
     * Infrastructure use only. The service host will invoke this method to allow a service to
     * either queue a request or, if it returns true, indicate it is ready to process it
     *
     * @param op
     * @return A value indicating whether the request has been completed in-line or queued for later
     *         processing. If value is false, request can be scheduled immediately
     */
    @Override
    public boolean queueRequest(Operation op) {

        if (checkServiceStopped(op, false)) {
            return true;
        }

        if (op.getAction() != Action.DELETE
                && this.context.processingStage != ProcessingStage.AVAILABLE) {
            // this should never happen since the host will not forward requests if we are not
            // available
            logWarning("Service in %s stage, cancelling operation",
                    this.context.processingStage);
            op.fail(new CancellationException());
            return true;
        }

        if (hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            op.setEnqueueTime(Utils.getNowMicrosUtc());
        }

        URI referer = op.getReferer();
        if (referer == null) {
            op.fail(new IllegalArgumentException("Referer is required"));
            return true;
        }

        if (!hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING) && queueSynchronizedRequest(op)) {
            return true;
        }

        return false;
    }

    private boolean checkServiceStopped(Operation op, boolean stop) {
        boolean isAlreadyStopped = this.context.processingStage == ProcessingStage.STOPPED;

        if (isAlreadyStopped) {
            if (op.getAction() != Action.DELETE) {
                logWarning("Service is stopped, cancelling operation");
                op.fail(new CancellationException());
            } else {
                op.complete();
            }
        }

        if (!isAlreadyStopped && !stop) {
            return false;
        }

        // even if service is stopped, check the pending queue for operations
        setProcessingStage(Service.ProcessingStage.STOPPED);
        Collection<Operation> opsToCancel = null;
        Set<String> txCoordinators = null;
        synchronized (this.context) {
            opsToCancel = this.context.operationQueue.toCollection();
            this.context.operationQueue.clear();
            if (hasPendingTransactions()) {
                txCoordinators = new HashSet<>(this.context.txCoordinatorLinks);
                txCoordinators.clear();
            }
        }

        abortTransactions(txCoordinators);

        for (Operation o : opsToCancel) {
            if (o.isFromReplication() && o.getAction() == Action.DELETE) {
                o.complete();
            } else {
                o.fail(new CancellationException(getSelfLink()));
            }
        }

        // return true only if service was stopped before we tried to stop it
        return isAlreadyStopped;
    }

    /**
     * Returns true if a request was handled (caller should not attempt to dispatch it)
     */
    private boolean queueSynchronizedRequest(final Operation op) {

        if (op.getAction() != Action.GET) {
            // serialize updates
            synchronized (this.context) {
                if (this.context.processingStage == ProcessingStage.STOPPED) {
                    // skip queuing
                } else if ((this.context.isUpdateActive || this.context.getActiveCount != 0)) {
                    if (!this.context.operationQueue.offer(op)) {
                        getHost().failRequestLimitExceeded(op);
                        return true;
                    }
                    return true;
                }
                this.context.isUpdateActive = true;
            }
        } else {
            // Indexed services serve GET directly from document store so they
            // can run in parallel with updates and each other
            if (isIndexed()) {
                return false;
            }

            // queue GETs, if updates are pending
            synchronized (this.context) {
                if (this.context.processingStage == ProcessingStage.STOPPED) {
                    // skip queuing
                } else if (this.context.isUpdateActive) {
                    this.context.operationQueue.offer(op);
                    return true;
                }
                this.context.getActiveCount++;
            }
        }

        // ask to stop service, even if it might be stopped, so we can drain any pending queues
        if (checkServiceStopped(op, false)) {
            return true;
        }

        return false;
    }

    @Override
    public void handleRequest(Operation request) {
        handleRequest(request, OperationProcessingStage.LOADING_STATE);
    }

    @Override
    public void handleRequest(Operation request, OperationProcessingStage opProcessingStage) {
        boolean isCompletionNested = false;
        try {
            if (opProcessingStage == OperationProcessingStage.LOADING_STATE) {
                if (handleRequestLoadingAndLinkingState(request)) {
                    return;
                }

                opProcessingStage = OperationProcessingStage.PROCESSING_FILTERS;
            }

            if (opProcessingStage == OperationProcessingStage.PROCESSING_FILTERS) {
                if (request.getAction() != Action.GET && validateReplicatedUpdate(request)) {
                    return;
                }

                request.nestCompletion((o, e) -> handleRequestCompletion(o, e));
                isCompletionNested = true;

                if (handleOperationInTransaction(request)) {
                    return;
                }

                if (request.getAction() != Action.GET && validateUpdate(request)) {
                    return;
                }

                if (hasOption(ServiceOption.OWNER_SELECTION) && request.isFromReplication()) {
                    // bypass service up call on replicas. We can offer a ServiceOption to alter
                    // this behavior, per service
                    request.complete();
                    return;
                }

                OperationProcessingChain opProcessingChain = getOperationProcessingChain();
                if (opProcessingChain != null && !opProcessingChain.processRequest(request)) {
                    return;
                }

                opProcessingStage = OperationProcessingStage.EXECUTING_SERVICE_HANDLER;
            }

            if (opProcessingStage == OperationProcessingStage.EXECUTING_SERVICE_HANDLER) {
                if (!isCompletionNested) {
                    request.nestCompletion((o, e) -> handleRequestCompletion(o, e));
                    isCompletionNested = true;
                }

                switch (request.getAction()) {
                case DELETE:
                    handleDelete(request);
                    break;
                case GET:
                    handleGet(request);
                    break;
                case PATCH:
                    handlePatch(request);
                    break;
                case POST:
                    handlePost(request);
                    break;
                case PUT:
                    handlePut(request);
                    break;
                case OPTIONS:
                    handleOptions(request);
                    break;
                default:
                    getHost().failRequestActionNotSupported(request);
                    break;
                }
            }
        } catch (Throwable e) {
            if (Utils.isValidationError(e)) {
                logFine("Validation Error: %s", Utils.toString(e));
            } else {
                logWarning("Uncaught exception: %s", e.toString());
                // log stack trace in a new log messages in case out of memory prevents us from forming
                // the stack trace string
                logWarning("Exception trace: %s", Utils.toString(e));
            }
            if (isCompletionNested) {
                request.fail(e);
            } else {
                handleRequestCompletion(request, e);
            }
        }
    }

    /**
     * Handles loading state and associating it with an in-bound operation
     *
     * @param request
     * @return True if state needs to be loaded and parent should postpone processing
     */
    private boolean handleRequestLoadingAndLinkingState(Operation request) {
        if (hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            request.setHandlerInvokeTime(Utils.getNowMicrosUtc());
            adjustStat(request.getAction() + Service.STAT_NAME_REQUEST_COUNT, 1.0);
        }

        if (checkServiceStopped(request, false)) {
            return true;
        }

        if (request.getAction() == Action.DELETE
                && request.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)) {
            // local shutdown induced delete. By pass two stage operation processing
            request.nestCompletion((o, e) -> {
                processPending(request);
                handleDeleteCompletion(request);
            });
            handleDelete(request);
            return true;
        }

        if (request.isFromReplication()) {
            // Replicated operations always have the state, from entry node or the owner
            // as the body. No need to load local state.
            ServiceDocument state = request.getBody(this.context.stateType);
            request.linkState(state);
            return false;
        }

        loadAndLinkState(request);
        return true;
    }

    private boolean validateReplicatedUpdate(Operation request) {
        if (hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            return false;
        }

        // do basic version checking, regardless of service options
        ServiceDocument stateFromOwner = request.getLinkedState();
        if (request.isFromReplication()) {
            if (stateFromOwner == null) {
                failRequest(request, new IllegalArgumentException("missing state in replicated op:"
                        + request.toString()));
                return true;
            }
            if (stateFromOwner.documentVersion == this.context.version) {
                return resolvePossibleVersionConflict(request);
            }
        }

        if (!request.isFromReplication()) {
            if (isSynchronizeRequest(request)) {
                request.setFromReplication(true);
                // we want to index state on synch completion, so nest completion
                request.nestCompletion((o, e) -> handleRequestCompletion(o, e));
                synchronizeWithPeers(request, null);
                return true;
            }
            if (hasOption(ServiceOption.OWNER_SELECTION)) {
                if (!hasOption(ServiceOption.DOCUMENT_OWNER)) {
                    synchronizeWithPeers(request, new IllegalStateException("not marked as owner"));
                    return true;
                } else {
                    // local instance is already the owner no need for further validation
                    return false;
                }
            }
        }

        if (!hasOption(ServiceOption.OWNER_SELECTION)) {
            return false;
        }

        if (stateFromOwner.documentOwner == null) {
            failRequest(request, new IllegalArgumentException("documentOwner is required"));
            return true;
        }

        // the following validation checks assume the remote sender is acting as the document
        // owner since the request is marked "replicated". The local service must agree on the
        // following:
        //
        // 1) The epoch for this document
        // 2) The owner for this document

        if (stateFromOwner.documentEpoch == null
                || this.context.epoch > stateFromOwner.documentEpoch) {
            String error = String.format(
                    "Expected epoch: %d, in update: %d", this.context.epoch,
                    stateFromOwner.documentEpoch);

            if (hasOption(ServiceOption.DOCUMENT_OWNER)) {
                synchronizeWithPeers(request, new IllegalStateException(error));
            } else {
                failRequest(request.setStatusCode(Operation.STATUS_CODE_CONFLICT),
                        new IllegalStateException(error));
            }
            return true;
        }

        if (hasOption(ServiceOption.DOCUMENT_OWNER)
                && !stateFromOwner.documentOwner.equals(getHost().getId())) {
            // The local host is no longer the owner. The service host would have failed the
            // request if we disagreed with the sender, on who the owner is. Here we simply
            // toggle the owner option off
            toggleOption(ServiceOption.DOCUMENT_OWNER, false);
            return false;
        }

        return false;
    }

    /**
     * If update checking is required we enforce version and signature equality in the incoming
     * request body with that of the current state
     *
     * @param request
     * @return True if the request was invalid and should not be processed, false if processing
     *         should continue
     */
    private boolean validateUpdate(Operation request) {
        if (hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            return false;
        }

        if (request.isFromReplication()) {
            // Skip update checking. We only do strict update matching on the node performing the
            // update (owner or entry node)
            return false;
        }

        if (!hasOption(ServiceOption.STRICT_UPDATE_CHECKING)) {
            return false;
        }

        ServiceDocument currentState = request.getLinkedState();
        Object body = request.getBodyRaw();
        if (body instanceof String) {
            body = request.getBody(ServiceDocument.class);
        } else if (!(body instanceof ServiceDocument)) {
            // we can't compare requests that do not derive from ServiceDocument
            request.fail(new IllegalArgumentException(
                    "request body must derive from ServiceDocument"));
            return true;
        }

        ServiceDocument sdBody = (ServiceDocument) body;

        boolean isVersionMatch = this.context.version == sdBody.documentVersion;

        // if the document is not the owner, and service is partitioned, then we do not do
        // validation. We let the owner service instance decide if this is a good request or not
        // since we forward requests to it. If the owner likes it, then we see a replicated update

        String errorString = null;
        if (!isVersionMatch) {
            errorString = String
                    .format(
                            "current version %d, update time %d. Request version %d, update time %d",
                            currentState.documentVersion,
                            currentState.documentUpdateTimeMicros,
                            sdBody.documentVersion,
                            sdBody.documentUpdateTimeMicros);
            request.fail(new IllegalArgumentException(errorString));
            return true;
        }

        return false;
    }

    private boolean resolvePossibleVersionConflict(Operation request) {
        ServiceDocument stateFromOwner = request.getLinkedState();

        if (isCommitRequest(request)) {
            if (request.getAction() != Action.DELETE) {
                // Update Commits are expected to have the same version as latest proposal
                request.complete();
                processPending(request);
                return true;
            } else {
                // a DELETE commit from the remote owner means its time to stop the service, so
                // return false to allow regular DELETE processing
                return false;
            }
        }

        request.nestCompletion((o, e) -> {
            ServiceDocument stateFromIndex = request.getLinkedState();
            boolean isEqual = ServiceDocument.equals(
                    getHost().buildDocumentDescription(this),
                    stateFromOwner, stateFromIndex);

            if (isEqual) {
                // versions match and signature matches. If this is a commit, its likely a duplicate
                logFine("Version match (%d) and signature match from owner %s",
                        stateFromOwner.documentVersion, stateFromOwner.documentOwner);
                request.complete();
                processPending(request);
                return;
            }

            adjustStat(STAT_NAME_VERSION_CONFLICT_COUNT, 1);
            setStat(STAT_NAME_VERSION_IN_CONFLICT, this.context.version);

            request.setStatusCode(Operation.STATUS_CODE_CONFLICT);
            Throwable ex = new IllegalStateException(
                    String.format(
                            "%s latest version is %d, replicated request version: %d",
                            getSelfLink(), this.context.version,
                            stateFromOwner.documentVersion));
            failRequest(request, ex);
        });

        getHost().loadServiceState(this, this.context.selfLink, request, this.context.stateType);
        return true;
    }

    public void handlePost(Operation post) {
        getHost().failRequestActionNotSupported(post);
    }

    public void handleDelete(Operation delete) {
        // nested completion from Service will take care of actually shutting
        // down the service
        delete.complete();
    }

    public void handlePatch(Operation patch) {
        getHost().failRequestActionNotSupported(patch);
    }

    public void handleOptions(Operation options) {
        getHost().failRequestActionNotSupported(options);
    }

    /**
     * Replace current state, with the body of the request, in one step
     */
    public void handlePut(Operation put) {
        ServiceDocument newState = put.getBody(this.context.stateType);
        setState(put, newState);
        put.complete();
    }

    public void handleGet(Operation get) {
        if (!hasPendingTransactions()) {
            handleGetSimple(get);
            return;
        }
        handleGetWithinTransaction(get);
    }

    /**
     * The normal path, if no transactions are detected.
     *
     * TODO: depends on (DCP-1160)
     */
    private void handleGetSimple(Operation get) {
        ServiceDocument d = get.getLinkedState();
        if (d == null) {
            if (checkServiceStopped(get, false)) {
                return;
            }

            if (this.context.version > 0) {
                throw new IllegalStateException("Version is non zero but no state was found");
            }
            d = new ServiceDocument();
            d.documentSelfLink = this.context.selfLink;
            d.documentKind = Utils.buildKind(this.context.stateType);
        }
        get.setBodyNoCloning(d).complete();
    }

    /**
     * Transaction-enabled path
     */
    private void handleGetWithinTransaction(Operation get) {
        QueryTask.Query selfLinkClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                .setTermMatchValue(getSelfLink());

        QueryTask.Query txClause = new QueryTask.Query();

        if (get.isWithinTransaction()) {
            // latest that has txid -- TODO: incorporate caching (DCP-1160)
            txClause.setTermPropertyName(ServiceDocument.FIELD_NAME_TRANSACTION_ID);
            txClause.setTermMatchValue(get.getTransactionId());
        } else {
            // latest that does not have txid -- TODO: incorporate caching (DCP-1160)
            txClause.setTermPropertyName(ServiceDocument.FIELD_NAME_TRANSACTION_ID);
            txClause.setTermMatchValue("");
        }
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.INCLUDE_ALL_VERSIONS);
        q.query.addBooleanClause(selfLinkClause);
        q.query.addBooleanClause(txClause);

        QueryTask task = QueryTask.create(q).setDirect(true);
        URI uri = UriUtils.buildUri(getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
        Operation startPost = Operation
                .createPost(uri)
                .setBody(task)
                .setCompletion((o, f) -> handleTransactionQueryCompletion(o, f, get));
        sendRequest(startPost);
    }

    /**
     * Process the latest version recovered
     */
    private void handleTransactionQueryCompletion(Operation o, Throwable f, Operation original) {
        if (f != null) {
            logInfo(f.toString());
            original.fail(f);
            return;
        }

        QueryTask response = o.getBody(QueryTask.class);

        // If we are within a transaction, empty state means there are no shadowed versions, so return previous visible
        // If we are not, however, this means a 404 -- there is no prior visible state!
        if (response.results.documentLinks.isEmpty()) {
            if (original.isWithinTransaction()) {
                // TODO: This has the possibility of returning a version that has a different transaction, if there are
                // more than one transaction pending -- depends on DCP 1160.
                handleGetSimple(original);
            } else {
                original.setStatusCode(Operation.STATUS_CODE_NOT_FOUND);
                failRequest(original, new IllegalStateException("Latest state not found"));
            }
            return;
        }

        List<String> dl = response.results.documentLinks;
        String latest = dl.get(0);
        Object obj = response.results.documents.get(latest);
        original.setBodyNoCloning(obj).complete();
        original.complete();
    }

    /**
     * Performs a series of actions, based on service options, after the service code has called
     * operation.complete() but before the client sees request completion
     *
     * For replicated services, the flow is
     *
     * 1) Evolve state version and update time
     *
     * 2) Replicate (Propose for services with OWNER_SELECTION)
     *
     * 3) Index (save state)
     *
     * 4) Notify transaction coordinator
     *
     * 5) Update operation stats
     *
     * 6) Publish notification to subscribers
     *
     * 7) Complete request
     *
     * Service options dictate the failure semantics at each stage, and if updates can be processed
     * concurrently with replication and indexing
     */
    private void handleRequestCompletion(Operation op, Throwable e) {
        if (hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            op.setHandlerCompletionTime(Utils.getNowMicrosUtc());
        }

        ServiceDocument linkedState = null;
        boolean isUpdate = op.getAction() != Action.GET;
        boolean isStateUpdated = isUpdate
                && op.getStatusCode() != Operation.STATUS_CODE_NOT_MODIFIED;

        if (op.isFromReplication()) {
            isStateUpdated = true;
        }

        // evolve the common properties such as version
        if (e == null && isStateUpdated) {
            try {
                // Clone latest state, before replication or indexing to isolate state changes
                // from service code (which can continue mutating the linked state after it
                // completes an operation)
                if (op.getLinkedState() != null
                        && !op.isFromReplication()
                        && !hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
                    op.linkState(Utils.clone(op.getLinkedState()));
                }
                applyUpdate(op);
                linkedState = op.getLinkedState();
            } catch (Throwable e1) {
                logSevere(e1);
                // set failure so we fail operation below
                e = e1;
            }
        }

        if (op.isWithinTransaction() && this.getHost().getTransactionServiceUri() != null) {
            notifyTransactionCoordinator(op, e);
        }

        if (e != null) {
            if (hasOption(Service.ServiceOption.INSTRUMENTATION)) {
                adjustStat(op.getAction() + Service.STAT_NAME_FAILURE_COUNT, 1);
            }
            // operation has failed, complete and process any queued operations
            failRequest(op, e);
            return;
        }

        boolean processPending = true;
        try {
            if (op.getAction() == Action.DELETE && op.getTransactionId() == null
                    && handleDeleteCompletion(op)) {
                processPending = false;
                return;
            }

            if (isStateUpdated && linkedState != null) {
                // defense in depth: conform state so core fields are properly set
                if (linkedState.documentDescription != null) {
                    linkedState.documentDescription = null;
                }

                linkedState.documentSelfLink = this.context.selfLink;
                if (linkedState.documentKind == null) {
                    linkedState.documentKind = Utils.buildKind(this.context.stateType);
                }

                if (replicateRequest(op)) {
                    processPending = false;
                    return;
                }

                saveState(op);
            } else {
                completeRequest(op);
            }
        } finally {
            if (processPending) {
                // this must always be called after state is saved / cloned (for SYNCHRONIZED
                // services
                processPending(op);
            }
        }
    }

    /**
     * Notify the transaction coordinator asynchronously (taking no action upon response)
     */
    protected void notifyTransactionCoordinator(Operation op, Throwable e) {
        OperationTransactionRecord operationsLogRecord = new OperationTransactionRecord();
        operationsLogRecord.action = op.getAction();
        operationsLogRecord.pendingTransactions = this.context.txCoordinatorLinks;
        operationsLogRecord.isSuccessful = e == null;

        URI transactionCoordinator = UriUtils.buildTransactionUri(getHost(), op.getTransactionId());

        synchronized (this.context) {
            if (this.context.txCoordinatorLinks == null) {
                this.context.txCoordinatorLinks = new HashSet<>();
            }
            this.context.txCoordinatorLinks.add(transactionCoordinator.toString());
        }

        sendRequest(Operation.createPut(transactionCoordinator).setBody(operationsLogRecord));
    }

    private void failRequest(Operation op, Throwable e) {
        if (op.getStatusCode() == Operation.STATUS_CODE_CONFLICT) {
            // Request client side retries on state or consensus conflict
            ServiceErrorResponse rsp = ServiceErrorResponse.create(e, op.getStatusCode(),
                    EnumSet.of(ErrorDetail.SHOULD_RETRY));
            op.setBodyNoCloning(rsp);
        }

        processPending(op);
        op.fail(e);
    }

    private boolean isIndexed() {
        return this.context.options.contains(ServiceOption.PERSISTENCE);
    }

    private boolean handleDeleteCompletion(Operation op) {
        if (op.isFromReplication() && hasOption(ServiceOption.OWNER_SELECTION)) {
            if (!isCommitRequest(op)) {
                return false;
            }
        }

        if (checkServiceStopped(op, true)) {
            return true;
        }

        getHost().stopService(this);
        if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)) {
            completeRequest(op);
            return true;
        }
        return false;
    }

    private boolean replicateRequest(Operation op) {
        if (!hasOption(ServiceOption.REPLICATION)) {
            return false;
        }

        if (op.getAction() == Action.GET || op.isReplicationDisabled() || op.isFromReplication()) {
            return false;
        }

        if (hasOption(ServiceOption.OWNER_SELECTION) && !hasOption(ServiceOption.DOCUMENT_OWNER)) {
            // only owner sends out replication / Propose requests
            return false;
        }

        if (!op.hasBody()) {
            // we do not replicate operations without a body. The only such
            // operation that is valid is a DELETE to stop a service locally,
            // but local service stops do not replicate. A DELETE with a body,
            // which causes both a stop and the service document to be removed,
            // will be replicated
            return false;
        }

        op.nestCompletion((o, e) -> {
            if (e != null) {
                synchronizeWithPeers(op, e);
                return;
            }
            op.setReplicationDisabled(true);
            try {
                saveState(op);
            } finally {
                processPending(op);
            }
        });

        getHost().replicateRequest(this.context.options, op.getLinkedState(),
                getPeerNodeSelectorPath(), getSelfLink(), op);
        return true;
    }

    private void saveState(Operation op) {
        op.nestCompletion((o, e) -> {
            if (e != null) {
                failRequest(op, e);
                return;
            }
            completeRequest(op);
        });

        ServiceDocument mergedState = op.getLinkedState();
        this.context.host.saveServiceState(this, op, mergedState);
    }

    private void completeRequest(Operation op) {
        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            updatePerOperationStats(op);
        }

        if (op.getAction() == Action.GET && !isIndexed()) {
            op.linkState(null);
            // run completions in parallel since non indexed GETs are serialized with updates
            getHost().run(() -> {
                op.complete();
            });
            return;
        }

        publish(op);

        op.complete();
    }

    private void scheduleCommitRequest(Operation op) {

        if (op.isFromReplication() || op.getAction() == Action.GET) {
            return;
        }

        if (!hasOption(ServiceOption.DOCUMENT_OWNER)) {
            return;
        }

        // The owner has the responsibility to advertise the most recent committed state
        // to all the peers. If operations are flowing, operation at version N services to
        // inform the replicas about commit N-1. However, if no new operations occur, the owner
        // must still communicate the last commit in a timely fashion otherwise the replicas
        // will be behind. Here we re-issue the current state (committed) when we notice the
        // pending operation queue is empty

        synchronized (this.context) {
            if (!this.context.operationQueue.isEmpty()) {
                return;
            }
        }

        ServiceDocument latestState = op.getLinkedState();
        URI u = getUri();
        Operation commitOp = Operation
                .createPut(u)
                .addRequestHeader(Operation.REPLICATION_PHASE_HEADER,
                        Operation.REPLICATION_PHASE_COMMIT)
                .setReferer(u)
                .setExpiration(getHost().getOperationTimeoutMicros() + Utils.getNowMicrosUtc());

        if (op.getAction() == Action.DELETE) {
            commitOp.setAction(op.getAction());
        }

        getHost().replicateRequest(this.context.options, latestState, getPeerNodeSelectorPath(),
                getSelfLink(),
                commitOp);
    }

    private void publish(Operation op) {
        if (op.getAction() == Action.GET) {
            return;
        }
        if (op.isNotificationDisabled()) {
            return;
        }
        if (this.context.utilityService == null) {
            // We check outside lock, since if a publish races with a external
            // subscribe, a lock would not help
            return;
        }
        if (!allocateUtilityService(false)) {
            return;
        }

        if (op.getStatusCode() == Operation.STATUS_CODE_NOT_MODIFIED) {
            return;
        }

        if (!op.hasBody()) {
            return;
        }
        this.context.utilityService.notifySubscribers(op);
    }

    private void updatePerOperationStats(Operation op) {
        op.setCompletionTime(Utils.getNowMicrosUtc());
        InstrumentationContext ctx = op.getInstrumentationContext();
        long queueLatency = ctx.handleInvokeTimeMicrosUtc - ctx.enqueueTimeMicrosUtc;
        long handlerLatency = ctx.handlerCompletionTime - ctx.handleInvokeTimeMicrosUtc;
        long endToEndDuration = ctx.operationCompletionTimeMicrosUtc - ctx.enqueueTimeMicrosUtc;
        if (ctx.documentStoreCompletionTimeMicrosUtc > 0) {
            ServiceStat s = getHistogramStat(Service.STAT_NAME_STATE_PERSIST_LATENCY);
            setStat(s,
                    ctx.documentStoreCompletionTimeMicrosUtc - ctx.handlerCompletionTime);
        }
        ServiceStat s = getHistogramStat(op.getAction()
                + Service.STAT_NAME_OPERATION_QUEUEING_LATENCY);
        setStat(s, queueLatency);
        s = getHistogramStat(op.getAction() + Service.STAT_NAME_SERVICE_HANDLER_LATENCY);
        setStat(s, handlerLatency);
        s = getHistogramStat(op.getAction() + Service.STAT_NAME_OPERATION_DURATION);
        setStat(op.getAction() + Service.STAT_NAME_OPERATION_DURATION, endToEndDuration);
    }

    private void loadAndLinkState(Operation op) {
        op.nestCompletion((o, e) -> {
            if (e != null) {
                failRequest(op, e);
                return;
            }

            ServiceDocument linkedState = op.getLinkedState();

            if (linkedState == null && hasOption(ServiceOption.PERSISTENCE)) {
                // the only way the document index will return null for state is if that state is
                // permanently deleted from the index. We need to fail the request
                failRequest(op, new IllegalStateException(
                        "Service state permanently deleted from index"));
                return;
            }

            if (linkedState != null) {
                if (hasOption(ServiceOption.DOCUMENT_OWNER)) {
                    linkedState.documentOwner = getHost().getId();
                }

                if (hasOption(ServiceOption.OWNER_SELECTION)) {
                    linkedState.documentEpoch = this.context.epoch;
                }
            }

            // state has already been linked with the operation
            handleRequest(op, OperationProcessingStage.PROCESSING_FILTERS);
        });
        getHost().loadServiceState(this, this.context.selfLink, op, this.context.stateType);
    }

    private void processPending(Operation op) {
        if (hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            return;
        }

        if (op.getAction() != Action.GET) {
            synchronized (this.context) {
                this.context.isUpdateActive = false;
            }
            scheduleCommitRequest(op);
        }

        if (op.getAction() == Action.GET) {
            if (isIndexed()) {
                // GETs to indexed services are not synchronized
                return;
            }

            synchronized (this.context) {
                this.context.getActiveCount--;
                if (this.context.getActiveCount < 0) {
                    logSevere(new IllegalStateException(
                            "Synchronization state is invalid: Negative pending gets"));
                    this.context.getActiveCount = 0;
                }
            }
        }

        this.context.host.handleRequest(this, null);
    }

    @Override
    public Operation dequeueRequest() {
        synchronized (this.context) {
            return this.context.operationQueue.poll();
        }
    }

    private boolean applyUpdate(Operation op) throws Throwable {
        long time = Utils.getNowMicrosUtc();

        ServiceDocument cachedState = op.getLinkedState();
        if (cachedState == null) {
            cachedState = this.context.stateType.newInstance();
        }

        if (!op.isFromReplication()) {
            if (hasOption(ServiceOption.OWNER_SELECTION)) {
                cachedState.documentEpoch = this.context.epoch;
            }

            // Update version only if request came directly from client (which also means local
            // node is owner for OWNER_SELECTION enabled services
            synchronized (this.context) {
                this.context.version++;
                cachedState.documentVersion = this.context.version;
                cachedState.documentUpdateTimeMicros = time;
            }
            op.linkState(cachedState);
            return true;
        }

        // a replica simply sets its version to the highest version it has seen. Agreement on
        // owner and epoch is done in validation methods upstream
        this.context.version = Math.max(cachedState.documentVersion, this.context.version);
        cachedState.documentUpdateTimeMicros = Math.max(
                cachedState.documentUpdateTimeMicros, time);

        if (hasOption(ServiceOption.OWNER_SELECTION)) {
            long prevEpoch = this.context.epoch;
            this.context.epoch = Math.max(cachedState.documentEpoch, this.context.epoch);
            if (prevEpoch != this.context.epoch) {
                logFine("Epoch updated from %d to %d", prevEpoch, this.context.epoch);
            }
        }

        // if this update version is less than the highest version we have seen, it will still get
        // indexed, but will not become the latest, authoritative version. Any caching logic
        // will ignore replicated updates with a smaller version than the max seen so far. The
        // index always serves the highest version seen.
        return this.context.version == cachedState.documentVersion;
    }

    /**
     * Synchronizes state and consensus properties (ownership, epoch, version) with a peer, due
     * to validation failure. The method will first determine if the current owner is still owner, and if
     * so, will ask the node group synchronization service to gather the best state for this instance.
     *
     * The original request will be failed, after synchronization is complete, which
     * should initiate a retry on behalf of the client
     * @param request
     * @param failure
     */
    private void synchronizeWithPeers(Operation request, Throwable failure) {
        if (failure instanceof CancellationException) {
            failRequest(request, failure);
            return;
        }

        // clone the request so we can update its body without affecting the client request
        Operation clonedRequest = request.clone();
        boolean wasOwner = hasOption(ServiceOption.DOCUMENT_OWNER);

        clonedRequest.setBody(request.getLinkedState()).setCompletion((o, e) -> {
            if (e != null) {
                failRequest(request, e);
                return;
            }

            boolean isOwner = hasOption(ServiceOption.DOCUMENT_OWNER);

            if (!isOwner) {
                completeSynchronizationRequest(request, failure);
                return;
            }

            // update and index using latest state from peers
            ServiceDocument state = (ServiceDocument) o.getBodyRaw();
            if (state != null) {
                if (hasOption(ServiceOption.DOCUMENT_OWNER)) {
                    state.documentOwner = getHost().getId();
                }
                synchronized (this.context) {
                    if (state.documentEpoch != null) {
                        this.context.epoch = Math.max(this.context.epoch, state.documentEpoch);
                    }
                    this.context.version = Math
                            .max(this.context.version, state.documentVersion);
                }

                // update synchronized linked state with the update operation, so it gets indexed
                request.linkState(state);
            }

            completeSynchronizationRequest(request, failure);

            if (wasOwner) {
                return;
            }

            getHost().scheduleServiceOptionToggleMaintenance(getSelfLink(),
                    EnumSet.of(ServiceOption.DOCUMENT_OWNER), null);
        });

        clonedRequest.setRetryCount(0);
        getHost().selectServiceOwnerAndSynchState(this, clonedRequest, true);
    }

    private void completeSynchronizationRequest(Operation request, Throwable failure) {
        if (failure != null) {
            request.setStatusCode(Operation.STATUS_CODE_CONFLICT);
            failRequest(request, new IllegalStateException(
                    "Synchronization complete, original failure: " + failure.toString()));
            return;
        }

        // this is an explicit synchronization request
        request.complete();
    }

    @Override
    public void setStat(String name, double newValue) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService(true);
        ServiceStat s = getStat(name);
        this.context.utilityService.setStat(s, newValue);
    }

    @Override
    public void setStat(ServiceStat s, double newValue) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService(true);
        this.context.utilityService.setStat(s, newValue);
    }

    @Override
    public void adjustStat(ServiceStat s, double delta) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService(true);
        this.context.utilityService.adjustStat(s, delta);
    }

    @Override
    public void adjustStat(String name, double delta) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService(true);
        ServiceStat s = getStat(name);
        this.context.utilityService.adjustStat(s, delta);
    }

    @Override
    public ServiceStat getStat(String name) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        if (!allocateUtilityService(true)) {
            return null;
        }
        return this.context.utilityService.getStat(name);
    }

    private ServiceStat getHistogramStat(String name) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat s = getStat(name);
        synchronized (s) {
            if (s.logHistogram == null) {
                s.logHistogram = new ServiceStatLogHistogram();
            }
        }
        return s;
    }

    private boolean allocateUtilityService(boolean forceAllocate) {
        synchronized (this.context) {
            if (!forceAllocate && this.context.utilityService == null) {
                // We check outside lock, since if a publish races with a
                // external
                // subscribe, a lock would not help
                return false;
            }

            if (this.context.utilityService == null) {
                this.context.utilityService = new UtilityService();
            }
            this.context.utilityService.setParent(this);
        }
        return true;
    }

    @Override
    public String getSelfLink() {
        return this.context.selfLink;
    }

    @Override
    public URI getUri() {
        return UriUtils.buildUri(this.context.host, this.context.selfLink);
    }

    @Override
    public ServiceHost getHost() {
        return this.context.host;
    }

    @Override
    public boolean hasOption(Service.ServiceOption cap) {
        return this.context.options.contains(cap);
    }

    @Override
    public void toggleOption(ServiceOption option, boolean enable) {

        if (option == ServiceOption.IDEMPOTENT_POST) {
            throw new IllegalArgumentException("Option not supported on singleton services."
                    + " Set this service option on the factory service instead.");
        }

        if (option != ServiceOption.HTML_USER_INTERFACE
                && option != ServiceOption.EAGER_CONSISTENCY
                && option != ServiceOption.DOCUMENT_OWNER
                && option != ServiceOption.PERIODIC_MAINTENANCE
                && option != ServiceOption.INSTRUMENTATION) {

            if (getProcessingStage() != Service.ProcessingStage.CREATED) {
                throw new IllegalStateException("Service already started");
            }
        }

        if (option == ServiceOption.EAGER_CONSISTENCY
                && !this.context.options.contains(ServiceOption.OWNER_SELECTION)) {
            if (getProcessingStage() != Service.ProcessingStage.CREATED) {
                throw new IllegalStateException(
                        "Service already started and OWNER_SELECTION is not set");
            }
        }

        synchronized (this.context) {
            if (enable) {
                this.context.options.add(option);
            } else {
                this.context.options.remove(option);
            }
        }
    }

    @Override
    public void setSelfLink(String path) {
        if (this.context.processingStage != Service.ProcessingStage.CREATED) {
            throw new IllegalStateException(
                    "Self link can not change past initialization");
        }

        this.context.selfLink = path.intern();
    }

    @Override
    public void setHost(ServiceHost serviceHost) {
        this.context.host = serviceHost;
    }

    @Override
    public void setOperationProcessingChain(OperationProcessingChain opProcessingChain) {
        this.context.opProcessingChain = opProcessingChain;
    }

    protected void setOperationQueueLimit(int limit) {
        this.context.operationQueue.setLimit(limit);
    }

    @Override
    public void setProcessingStage(Service.ProcessingStage stage) {
        IllegalStateException failure = null;
        String statName = null;
        try {
            synchronized (this.context) {
                if (this.context.processingStage == stage) {
                    return;
                }

                if (stage == ProcessingStage.PAUSED) {
                    if (this.context.processingStage != ProcessingStage.AVAILABLE) {
                        failure = new IllegalStateException("Service can not be paused, in stage: "
                                + this.context.processingStage);
                        return;
                    }
                    statName = STAT_NAME_PAUSE_COUNT;
                } else if (this.context.processingStage == ProcessingStage.PAUSED
                        && stage == ProcessingStage.AVAILABLE) {
                    statName = STAT_NAME_RESUME_COUNT;
                    this.context.isUpdateActive = false;
                } else if (this.context.processingStage.ordinal() > stage.ordinal()) {
                    throw new IllegalArgumentException(this.context.processingStage
                            + " can not move to "
                            + stage);
                }

                this.context.processingStage = stage;
            }
        } finally {
            if (failure != null) {
                throw failure;
            }
            if (statName != null) {
                adjustStat(statName, 1);
            }
        }

        if (stage == ProcessingStage.AVAILABLE) {
            getHost().notifyServiceAvailabilitySubscribers(this);
        }
    }

    @Override
    public Service getUtilityService(String uriPath) {
        allocateUtilityService(true);
        return this.context.utilityService;
    }

    @Override
    public void sendRequest(Operation op) {
        prepareRequest(op);
        this.context.host.sendRequest(op);
    }

    private void prepareRequest(Operation op) {
        if (!this.hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)
                && op.getContextId() == null) {
            op.setContextId(OperationContext.getContextId());
        }

        if (this.hasOption(ServiceOption.REPLICATION)) {
            // assume target is also replicated. If they are not, there is only a tiny performance
            // hit due to the availability registration and instant completion
            op.setTargetReplicated(true);
        }
        op.setReferer(UriUtils.buildUri(getHost().getPublicUri(), getSelfLink()));
    }

    /**
     * Generate an example instance of the document type. Override this method to provide a more
     * detailed example
     * Any exceptions are the result of programmer error - so RuntimeExceptions are thrown
     */
    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument d;
        Class<? extends ServiceDocument> type;

        type = this.context.stateType;
        try {
            d = type.newInstance();
        } catch (Throwable e) {
            logSevere(e);
            throw (new RuntimeException(e));
        }
        d.documentDescription = getHost().buildDocumentDescription(this);

        // populate fields in the PODO with default values
        for (PropertyDescription pd : d.documentDescription.propertyDescriptions.values()) {
            try {
                pd.accessor.set(d, pd.exampleValue);
            } catch (IllegalArgumentException e) {
                String msg = String.format(
                        "Cannot assign exampleValue: '%s' to field: %s of type: %s",
                        pd.exampleValue, pd.accessor.getName(), pd.accessor.getType());
                logSevere(msg);
                throw (new RuntimeException(msg, e));
            } catch (Throwable e) {
                logSevere(e);
                throw (new RuntimeException(e));
            }
        }
        // build true Kind
        d.documentKind = Utils.buildKind(type);
        return d;
    }

    public void logSevere(Throwable e) {
        log(Level.SEVERE, "%s", Utils.toString(e));
    }

    public void logSevere(String fmt, Object... args) {
        log(Level.SEVERE, fmt, args);
    }

    public void logInfo(String fmt, Object... args) {
        log(Level.INFO, fmt, args);
    }

    public void logFine(String fmt, Object... args) {
        log(Level.FINE, fmt, args);
    }

    public void logWarning(String fmt, Object... args) {
        log(Level.WARNING, fmt, args);
    }

    protected void log(Level level, String fmt, Object... args) {
        String uri = getUri() != null ? getUri().toString() : this.getClass().getSimpleName();
        Logger lg = Logger.getLogger(this.getClass().getName());
        Utils.log(lg, 4, uri, level, fmt, args);
    }

    @Override
    public void handleMaintenance(Operation post) {
        post.complete();
    }

    @Override
    public ServiceDocument setInitialState(String jsonState, Long version) {
        ServiceDocument s = Utils.fromJson(jsonState, this.context.stateType);
        if (version != null) {
            this.context.version = version;
            s.documentVersion = this.context.version;
        } else {
            this.context.version = s.documentVersion;
        }

        if (hasOption(ServiceOption.OWNER_SELECTION) && s.documentEpoch == null) {
            s.documentEpoch = 0L;
        }

        if (s.documentEpoch != null) {
            this.context.epoch = Math.max(this.context.epoch, s.documentEpoch);
        }
        return s;
    }

    @Override
    public String getPeerNodeSelectorPath() {
        return this.context.nodeSelectorLink;
    }

    @Override
    public void setPeerNodeSelectorPath(String link) {
        if (!hasOption(ServiceOption.REPLICATION)) {
            throw new IllegalStateException("Service is not replicated");
        }

        if (link == null) {
            throw new IllegalArgumentException("link is required");
        }

        this.context.nodeSelectorLink = link;
    }

    @Override
    public EnumSet<ServiceOption> getOptions() {
        synchronized (this.context) {
            return this.context.options.clone();
        }
    }

    /**
     * Replaces the state currently associated with the operation. When the operation completes the
     * state will be set as the current state for the service, and if the service is indexed, it
     * will be indexed with a new version
     */
    @Override
    public void setState(Operation op, ServiceDocument newState) {
        op.linkState(newState);
    }

    /**
     * Get service state associated with in bound operation
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T extends ServiceDocument> T getState(Operation op) {
        return (T) op.getLinkedState();
    }

    @Override
    public void setMaintenanceIntervalMicros(long micros) {
        if (micros < 0) {
            throw new IllegalArgumentException("micros must be positive");
        }
        this.context.maintenanceInterval = micros;
    }

    @Override
    public long getMaintenanceIntervalMicros() {
        return this.context.maintenanceInterval;
    }

    @Override
    public Class<? extends ServiceDocument> getStateType() {
        return this.context.stateType;
    }

    public boolean isConfigurationUpdate(Operation request) {
        if (request.getAction() == Action.GET || request.getAction() == Action.DELETE) {
            return false;
        }

        if (!request.hasBody()) {
            return false;
        }

        ServiceConfigUpdateRequest body = request.getBody(ServiceConfigUpdateRequest.class);
        if (!ServiceConfigUpdateRequest.KIND.equals(body.kind)) {
            return false;
        }

        return true;
    }

    @Override
    public void handleConfigurationRequest(Operation request) {
        if (request.getAction() == Action.PATCH) {
            allocateUtilityService(true);
            ServiceConfigUpdateRequest body = request.getBody(ServiceConfigUpdateRequest.class);
            synchronized (this.context) {
                if (body.epoch != null) {
                    if (this.context.epoch >= body.epoch.longValue()) {
                        request.fail(new IllegalArgumentException(
                                "New epoch is less or equal to current epoch: "
                                        + this.context.epoch));
                        return;
                    }
                    this.context.epoch = body.epoch;
                    logFine("Epoch updated to %d", this.context.epoch);
                }
            }
            if (body.operationQueueLimit != null) {
                setOperationQueueLimit(body.operationQueueLimit);
            }
            this.context.utilityService.handlePatchConfiguration(request, body);
            return;
        }

        if (request.getAction() == Action.GET) {
            ServiceConfiguration cfg = new ServiceConfiguration();
            cfg.options = getOptions();
            cfg.maintenanceIntervalMicros = getMaintenanceIntervalMicros();
            cfg.epoch = this.context.epoch;
            cfg.operationQueueLimit = this.context.operationQueue.getLimit();
            request.setBody(cfg).complete();
            return;
        }

        getHost().failRequestActionNotSupported(request);
        return;
    }

    private void abortTransactions(Set<String> coordinators) {
        if (coordinators == null || coordinators.isEmpty()) {
            return;
        }
        ResolutionRequest resolution = new ResolutionRequest();
        resolution.kind = ResolutionKind.ABORT;
        for (String coordinator : coordinators) {
            sendRequest(Operation.createPatch(UriUtils.buildUri(coordinator))
                    .setBodyNoCloning(resolution));
        }
    }

    /**
    * Set authorization context on operation.
    */
    public final void setAuthorizationContext(Operation op, AuthorizationContext ctx) {
        if (getHost().isPrivilegedService(this)) {
            op.setAuthorizationContext(ctx);
        } else {
            throw new RuntimeException("Service not allowed to set authorization context");
        }
    }

    /**
     * Returns the host's token signer.
     */
    public final Signer getTokenSigner() {
        if (getHost().isPrivilegedService(this)) {
            return getHost().getTokenSigner();
        } else {
            throw new RuntimeException("Service not allowed to get token signer");
        }
    }

    /**
     * Returns the system user's authorization context.
     */
    public final AuthorizationContext getSystemAuthorizationContext() {
        if (getHost().isPrivilegedService(this)) {
            return getHost().getSystemAuthorizationContext();
        } else {
            throw new RuntimeException("Service not allowed to get system authorization context");
        }
    }

    /**
     * Most of the transaction-related code becomes obsolete if we haven't seen transactions. A good
     * idea is to check whether the service has pending transactions
     */
    private boolean hasPendingTransactions() {
        return this.context.txCoordinatorLinks != null
                && !this.context.txCoordinatorLinks.isEmpty();
    }

    /**
     * Check whether it's a transactional control operation (i.e., expose shadowed state, abort
     * etc.), and take appropriate action
     */
    private boolean handleOperationInTransaction(Operation request) {
        if (request.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER) == null) {
            return false;
        }

        if (request.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER).equals(
                Operation.TX_COMMIT)) {
            // commit should expose latest state, i.e., remove shadow and bump the version
            // and remove transaction from pending
            this.context.txCoordinatorLinks.remove(request.getReferer().toString());

            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_TRANSACTION_ID);
            q.query.setTermMatchValue(UriUtils.getLastPathSegment(request.getReferer()));
            q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.INCLUDE_ALL_VERSIONS);
            QueryTask task = QueryTask.create(q).setDirect(true);
            URI uri = UriUtils.buildUri(getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
            Operation startPost = Operation
                    .createPost(uri)
                    .setBody(task)
                    .setCompletion((o, f) -> handleUnshadowQueryCompletion(o, f, request));
            sendRequest(startPost);

        } else if (request.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER).equals(
                Operation.TX_ABORT)) {
            // abort should just remove transaction from pending
            this.context.txCoordinatorLinks.remove(request.getReferer().toString());
            request.complete();
        } else {
            request.fail(new IllegalArgumentException(
                    "Transaction control message, but none of {commit, abort}"));
        }
        return true;
    }

    private void handleUnshadowQueryCompletion(Operation o, Throwable f, Operation original) {
        if (f != null) {
            logInfo(f.toString());
            original.fail(f);
            return;
        }

        QueryTask response = o.getBody(QueryTask.class);
        if (response.results.documentLinks.isEmpty()) {
            // TODO: When implement 2PC, abort entire transaction
            original.fail(new IllegalStateException(
                    "There should be at least one shadowed, but none was found"));
            return;
        }

        // Whereas, if more than a single version, get the latest..
        List<String> dl = response.results.documentLinks;
        String latest = dl.get(0);
        Object obj = response.results.documents.get(latest);
        // ..unshadow..
        ServiceDocument sd = Utils.fromJson((String) obj, this.context.stateType);
        sd.documentTransactionId = "";
        // ..and stick back in.
        setState(original, sd);
        original.complete();
    }
}
