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

import static com.vmware.xenon.common.TransactionServiceHelper.handleGetWithinTransaction;
import static com.vmware.xenon.common.TransactionServiceHelper.handleOperationInTransaction;
import static com.vmware.xenon.common.TransactionServiceHelper.notifyTransactionCoordinator;

import java.net.URI;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.InstrumentationContext;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.jwt.Signer;
import com.vmware.xenon.services.common.ServiceUriPaths;

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
        public long lastCommitTimeMicros;
    }

    private final RuntimeContext context = new RuntimeContext();

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
    public void handleCreate(Operation post) {
        post.complete();
    }

    @Override
    public void handleStart(Operation post) {
        post.complete();
    }

    /**
     * Authorization processing happens during the state loading phase of request handling.
     * This method always immediately completes the operation
     */
    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
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

            if (this.context.processingStage == ProcessingStage.PAUSED) {
                logWarning("Service in stage %s, retrying request", this.context.processingStage);
                getHost().handleRequest(this, op);
                return true;
            }

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

        if (!op.hasReferer()) {
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
                if (hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                    getHost().retryOnDemandLoadStopConflict(this, op);
                } else {
                    logWarning("Service is stopped, cancelling operation");
                    op.fail(new CancellationException());
                }
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
        synchronized (this.context) {
            opsToCancel = this.context.operationQueue.toCollection();
            this.context.operationQueue.clear();
        }

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
        boolean isPaused = false;
        if (op.getAction() != Action.GET && op.getAction() != Action.OPTIONS) {
            // serialize updates
            synchronized (this.context) {
                if (this.context.processingStage == ProcessingStage.PAUSED) {
                    isPaused = true;
                } else if (this.context.processingStage == ProcessingStage.STOPPED) {
                    // fall through, request will be cancelled
                } else if ((this.context.isUpdateActive || this.context.getActiveCount != 0)) {
                    if (!this.context.operationQueue.offer(op)) {
                        getHost().failRequestLimitExceeded(op);
                    }
                    return true;
                } else {
                    this.context.isUpdateActive = true;
                }
            }
        } else {
            if (op.getAction() == Action.OPTIONS) {
                return false;
            } else if (hasOption(ServiceOption.CONCURRENT_GET_HANDLING)) {
                // Indexed services serve GET directly from document store so they
                // can run in parallel with updates and each other
                return false;
            } else {
                // queue GETs, if updates are pending
                synchronized (this.context) {
                    if (this.context.processingStage == ProcessingStage.PAUSED) {
                        isPaused = true;
                    } else if (this.context.processingStage == ProcessingStage.STOPPED) {
                        // fall through, request will be cancelled
                    } else if (this.context.isUpdateActive) {
                        if (!this.context.operationQueue.offer(op)) {
                            getHost().failRequestLimitExceeded(op);
                        }
                        return true;
                    } else {
                        this.context.getActiveCount++;
                    }
                }
            }
        }

        if (isPaused && !getHost().isStopping()) {
            logWarning("Service in stage %s, retrying request", this.context.processingStage);
            getHost().handleRequest(this, op);
            return true;
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
                if (ServiceHost.isServiceStop(request)) {
                    // local shutdown induced delete. By pass two stage operation processing
                    request.nestCompletion((o, e) -> {
                        processPending(request);
                        if (e == null) {
                            handleStopCompletion(request);
                        }
                    });

                    if (hasOption(ServiceOption.OWNER_SELECTION)
                            && !hasOption(ServiceOption.DOCUMENT_OWNER)) {
                        // we only call handlers on the owner node
                        request.complete();
                    } else {
                        handleStop(request);
                    }
                    return;
                }

                if (handleRequestLoadingAndLinkingState(request)) {
                    return;
                }

                opProcessingStage = OperationProcessingStage.PROCESSING_FILTERS;
            }

            if (opProcessingStage == OperationProcessingStage.PROCESSING_FILTERS) {
                if (request.getAction() != Action.GET && validateOwnerSelectedUpdate(request)) {
                    return;
                }

                request.nestCompletion(this::handleRequestCompletion);
                isCompletionNested = true;

                if (handleOperationInTransaction(this, this.context.stateType,
                        request)) {
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
                isCompletionNested = true;

                switch (request.getAction()) {
                case DELETE:
                    if (ServiceHost.isServiceStop(request)) {
                        handleStop(request);
                    } else {
                        // after handleDelete completes the operation with success, run handleStop. If that
                        // succeeds, we proceed with normal processing
                        request.nestCompletion((o) -> {
                            handleStop(request);
                        });
                        handleDelete(request);
                    }
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

    private boolean validateOwnerSelectedUpdate(Operation request) {
        if (hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            return false;
        }

        // do basic version checking, regardless of service options
        ServiceDocument stateFromOwner = request.getLinkedState();
        if (!request.isFromReplication()) {
            if (request.isSynchronize()) {
                synchronizeWithPeers(request, null);
                return true;
            }

            if (hasOption(ServiceOption.OWNER_SELECTION)) {
                if (!hasOption(ServiceOption.DOCUMENT_OWNER)) {
                    synchronizeWithPeers(request, new IllegalStateException(
                            "not marked as owner"));
                    return true;
                } else {
                    return false;
                }
            }
        }

        if (!hasOption(ServiceOption.OWNER_SELECTION)) {
            return false;
        }

        // the code below applies to replicated updates that came from a remote owner

        if (stateFromOwner == null) {
            failRequest(request, new IllegalArgumentException("missing state in replicated op:"
                    + request.toString()), true);
            return true;
        }

        if (stateFromOwner.documentOwner == null) {
            failRequest(request, new IllegalArgumentException("documentOwner is required"), false);
            return true;
        }

        // the following validation checks assume the remote sender is acting as the document
        // owner since the request is marked "replicated". The local service must agree on the
        // following:
        //
        // 1) The epoch for this document
        // 2) The owner for this document
        // 3) The version of the document. However, since our index
        // and caching policy serves the highest version, even if we receive
        // updates out of order, they will not affect client results

        if (stateFromOwner.documentEpoch == null
                || this.context.epoch > stateFromOwner.documentEpoch) {
            String error = String.format(
                    "Expected epoch: %d, in update: %d", this.context.epoch,
                    stateFromOwner.documentEpoch);

            if (hasOption(ServiceOption.DOCUMENT_OWNER)) {
                synchronizeWithPeers(request, new IllegalStateException(error));
            } else {
                failRequest(request.setStatusCode(Operation.STATUS_CODE_CONFLICT),
                        new IllegalStateException(error), true);
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

        if (request.getRequestHeader(Operation.TRANSACTION_HEADER) != null) {
            // Skip update checking in case of a transaction control operation
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

    public void handlePost(Operation post) {
        getHost().failRequestActionNotSupported(post);
    }

    public void handleDelete(Operation delete) {
        delete.complete();
    }

    public void handleStop(Operation delete) {
        // nested completion from Service will take care of actually shutting
        // down the service
        delete.complete();
    }

    public void handlePatch(Operation patch) {
        getHost().failRequestActionNotSupported(patch);
    }

    public void handleOptions(Operation options) {
        options.setBody(null).complete();
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
        handleGetWithinTransaction(this, get, this::handleGetSimple, this::failRequest);
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

        ServiceDocument linkedState = op.getLinkedState();
        boolean isUpdate = op.getAction() != Action.GET && op.getAction() != Action.OPTIONS;
        boolean isStateUpdated = isUpdate;

        if (op.isFromReplication()) {
            isStateUpdated = true;
        }

        if (op.getStatusCode() == Operation.STATUS_CODE_NOT_MODIFIED) {
            isStateUpdated = false;
        } else if (op.getTransactionId() != null && linkedState != null &&
                op.getTransactionId().equals(linkedState.documentTransactionId)) {
            isStateUpdated = true;
        }

        // evolve the common properties such as version
        if (e == null && isStateUpdated) {
            try {
                // Clone latest state, before replication or indexing to isolate state changes
                // from service code (which can continue mutating the linked state after it
                // completes an operation)
                if (linkedState != null
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
            allocatePendingTransactions();
            notifyTransactionCoordinator(this, op, e);
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

            if (op.getAction() == Action.OPTIONS) {
                handleOptionsCompletion(op);
                return;
            }

            if (isStateUpdated && linkedState != null) {
                // defense in depth: conform state so core fields are properly set
                if (linkedState.documentDescription != null) {
                    linkedState.documentDescription = null;
                }

                linkedState.documentSelfLink = this.context.selfLink;
                linkedState.documentUpdateAction = op.getAction().name();
                if (linkedState.documentKind == null) {
                    linkedState.documentKind = Utils.buildKind(this.context.stateType);
                }

                if (processCompletionStageReplicationProposal(op)) {
                    processPending = false;
                    return;
                }

                // next stage will process pending operations, disable finally clause from
                // duplicating work
                processPending = false;
                processCompletionStageIndexing(op);
            } else {
                processCompletionStagePublishAndComplete(op);
            }
        } finally {
            if (!processPending) {
                return;
            }
            processPending(op);
        }
    }

    protected void handleOptionsCompletion(Operation options) {
        if (!options.hasBody()) {
            options.setBodyNoCloning(getDocumentTemplate());
        }
        processCompletionStagePublishAndComplete(options);
    }

    private void failRequest(Operation op, Throwable e) {
        failRequest(op, e, false);
    }

    private void failRequest(Operation op, Throwable e, boolean shouldRetry) {
        if (shouldRetry) {
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
            if (!op.isCommit()) {
                return false;
            }
        }

        if (checkServiceStopped(op, true)) {
            return true;
        }

        // DELETE completion runs when a DELETE was issued by a client, not local host shutdown.
        // It needs to stop the service now, since the handleDelete() and handleStop() handlers
        // have already run.
        getHost().markAsPendingDelete(this);
        getHost().stopService(this);
        return false;
    }

    private void handleStopCompletion(Operation op) {
        if (checkServiceStopped(op, true)) {
            return;
        }

        getHost().stopService(this);
        op.complete();
    }

    /**
     * Part of operation completion processing. If the service requires replication, we ask
     * the host to invoke the proper node selector service to replicate the update to peers.
     * On completion, we determine if we need to issue a commit. Then we invoke the next stage,
     * indexing of state.
     */
    private boolean processCompletionStageReplicationProposal(Operation op) {
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

        if (op.getAction() == Action.DELETE && !op.hasBody()) {
            ServiceDocument body = op.getLinkedState();
            op.setBodyNoCloning(body);
        }

        if (!op.hasBody()) {
            // we replicate the linked state but only do so if there is a request body
            // associated with the update. So even if we don't replicate the body, we
            // still require it to be there.
            return false;
        }

        op.nestCompletion((o, e) -> {
            if (e != null) {
                ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                // only proceed with synchronization if a retry is requested
                if (rsp != null && rsp.details != null
                        && rsp.details.contains(ErrorDetail.SHOULD_RETRY)) {
                    synchronizeWithPeers(op, e);
                } else {
                    failRequest(op, e);
                }
                return;
            }
            op.setReplicationDisabled(true);
            processCompletionStageCommit(op);
        });

        getHost().replicateRequest(this.context.options, op.getLinkedState(),
                getPeerNodeSelectorPath(), getSelfLink(), op);
        return true;
    }

    /**
     * Part of operation completion processing. If the service is about to idle, advertise last
     * request, as a commit, to all peers.
     * Only applies to services with {@link ServiceOption#OWNER_SELECTION}
     */
    private void processCompletionStageCommit(Operation op) {
        boolean indexState = true;
        try {
            if (op.getStatusCode() >= Operation.STATUS_CODE_FAILURE_THRESHOLD) {
                // we only commit updates that were accepted by the owner, so ignore this
                // failed update
                return;
            }

            if (op.isFromReplication() || op.getAction() == Action.GET) {
                // only owners advertise commits, on updates
                return;
            }

            if (!hasOption(ServiceOption.DOCUMENT_OWNER)) {
                // Explicit commit messages are only sent from this instance on the owner node
                return;
            }

            // The owner has the responsibility to advertise the most recent committed state
            // to all the peers. If operations are flowing, operation at version N, informs services to
            // commit proposal at N-1. However, if no new operations occur, the owner
            // must still communicate the last commit in a timely fashion otherwise the replicas
            // will be behind. Here we re-issue the current state (committed) when we notice the
            // pending operation queue is empty
            if (op.getAction() != Action.DELETE) {
                if (!this.context.operationQueue.isEmpty()) {
                    return;
                }
            } else {
                if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)) {
                    // Directive indicates this update should have no persistence side-effects. Do not
                    // forward to peers. This is often due to local stop from host shutting down.
                    return;
                }
            }

            ServiceDocument latestState = op.getLinkedState();
            long delta = latestState.documentUpdateTimeMicros - this.context.lastCommitTimeMicros;
            if (delta < getHost().getMaintenanceIntervalMicros()) {
                return;
            }

            if (latestState.documentVersion < this.context.version
                    || (latestState.documentEpoch != null
                            && latestState.documentEpoch < this.context.epoch)) {
                return;
            }

            this.context.lastCommitTimeMicros = latestState.documentUpdateTimeMicros;

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

            commitOp.linkState(latestState);

            String replQuorum = op.getRequestHeader(Operation.REPLICATION_QUORUM_HEADER);
            if (Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL.equals(replQuorum)) {
                // skip next processing stage until we get commit completion
                indexState = false;
                // Do not complete request until commit is done. Client will see completion after
                // replicas have committed
                commitOp.addRequestHeader(Operation.REPLICATION_QUORUM_HEADER, replQuorum);
                commitOp.setCompletion((o, e) -> {
                    // regardless of commit success, we need to proceed with operation processing
                    processCompletionStageIndexing(op);
                });
            }

            getHost().replicateRequest(this.context.options, latestState, getPeerNodeSelectorPath(),
                    getSelfLink(),
                    commitOp);
            return;
        } finally {
            if (!indexState) {
                return;
            }
            processCompletionStageIndexing(op);
        }
    }

    /**
     * Part of operation completion processing. Issues request to indexing service, through the host
     * and kicks of pending request processing. On indexing completion, it continues with next stage
     * of operation completion
     */
    private void processCompletionStageIndexing(Operation op) {
        try {
            op.nestCompletion((o, e) -> {
                if (e != null) {
                    failRequest(op, e);
                    return;
                }
                processCompletionStagePublishAndComplete(op);
            });

            ServiceDocument mergedState = op.getLinkedState();
            this.context.host.saveServiceState(this, op, mergedState);
        } finally {
            processPending(op);
        }
    }

    /**
     * Final stage of operation completion processing. Notifications will be published and client
     * will receive operation response
     */
    private void processCompletionStagePublishAndComplete(Operation op) {
        if (hasOption(ServiceOption.INSTRUMENTATION)) {
            updatePerOperationStats(op);
        }

        if (op.getAction() == Action.GET && !isIndexed()) {
            op.linkState(null);
            // run completions in parallel since non indexed GETs are serialized with updates
            getHost().run(op::complete);
            return;
        }

        publish(op);

        if (op.isFromReplication() && !op.isSynchronize()) {
            // avoid cost of sending the request body as a response
            op.setBodyNoCloning(null);
        }
        op.complete();
    }

    private void publish(Operation op) {
        if (op.getAction() == Action.GET || op.getAction() == Action.OPTIONS) {
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
                        "Service state permanently deleted from index"), false);
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
        getHost().loadServiceState(this, op);
    }

    private void processPending(Operation op) {
        if (hasOption(ServiceOption.CONCURRENT_UPDATE_HANDLING)) {
            return;
        }

        if (op.getAction() == Action.OPTIONS) {
            return;
        }

        if (op.getAction() != Action.GET) {
            synchronized (this.context) {
                this.context.isUpdateActive = false;
            }
        }

        if (op.getAction() == Action.GET) {
            if (hasOption(ServiceOption.CONCURRENT_GET_HANDLING)) {
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

    private void applyUpdate(Operation op) throws Throwable {
        ServiceDocument cachedState = op.getLinkedState();
        if (cachedState == null) {
            cachedState = this.context.stateType.newInstance();
        }

        if (!op.isFromReplication()) {
            long time = Utils.getNowMicrosUtc();
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
            return;
        }

        // a replica simply sets its version to the highest version it has seen. Agreement on
        // owner and epoch is done in validation methods upstream
        this.context.version = Math.max(cachedState.documentVersion, this.context.version);

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
            handleSynchronizeWithPeersCompletion(request, failure, wasOwner, o, e);
        });

        clonedRequest.setRetryCount(0);
        clonedRequest.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH);
        boolean isFactorySync = request
                .hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING);
        getHost().selectServiceOwnerAndSynchState(this, clonedRequest, isFactorySync);
    }

    private void handleSynchronizeWithPeersCompletion(Operation request, Throwable failure,
            boolean wasOwner, Operation o, Throwable e) {
        if (e != null) {
            failRequest(request, e, true);
            return;
        }

        boolean isOwner = hasOption(ServiceOption.DOCUMENT_OWNER);
        boolean isStateUpdated = false;

        // update and index using latest state from peers
        ServiceDocument state = (ServiceDocument) o.getBodyRaw();
        if (state != null) {
            if (hasOption(ServiceOption.DOCUMENT_OWNER)) {
                state.documentOwner = getHost().getId();
            }
            synchronized (this.context) {
                if (state.documentEpoch != null) {
                    if (state.documentEpoch > this.context.epoch) {
                        this.context.epoch = state.documentEpoch;
                        isStateUpdated = true;
                    }
                }

                if (state.documentVersion > this.context.version) {
                    this.context.version = state.documentVersion;
                    isStateUpdated = true;
                }
            }

            // update synchronized linked state with the update operation, so it gets indexed
            request.linkState(state);
        }

        if (!wasOwner && isOwner) {
            isStateUpdated = true;
        }

        if (!isStateUpdated) {
            request.setStatusCode(Operation.STATUS_CODE_NOT_MODIFIED);
        }

        completeSynchronizationRequest(request, failure, isStateUpdated);

        if (wasOwner) {
            return;
        }
    }

    private void completeSynchronizationRequest(Operation request, Throwable failure,
            boolean isStateUpdated) {
        if (failure != null) {
            logWarning("isUpdated:%s, e:%d v:%d, cause:%s (%d",
                    isStateUpdated, this.context.epoch, this.context.version, failure,
                    request.getId());
            request.setStatusCode(Operation.STATUS_CODE_CONFLICT);
            failRequest(request, new IllegalStateException(
                    "Synchronization complete, original failure: " + failure.toString()), true);
            return;
        }

        // avoid replicating this synchronization request, on completion
        request.setFromReplication(true);

        if (!isStateUpdated) {
            processPending(request);
            request.complete();
            return;
        }

        // proceed with normal completion pipeline, including indexing
        request.nestCompletion(this::handleRequestCompletion);
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

        if (option != ServiceOption.HTML_USER_INTERFACE
                && option != ServiceOption.DOCUMENT_OWNER
                && option != ServiceOption.PERIODIC_MAINTENANCE
                && option != ServiceOption.INSTRUMENTATION
                && option != ServiceOption.TRANSACTION_PENDING) {

            if (getProcessingStage() != Service.ProcessingStage.CREATED) {
                throw new IllegalStateException("Service already started");
            }
        }

        if (option == ServiceOption.PERSISTENCE && enable) {
            toggleOption(ServiceOption.CONCURRENT_GET_HANDLING, true);
        }

        if (option == ServiceOption.PERIODIC_MAINTENANCE && hasOption(ServiceOption.ON_DEMAND_LOAD)
                || option == ServiceOption.ON_DEMAND_LOAD
                        && hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
            throw new IllegalArgumentException("Service option PERIODIC_MAINTENANCE and " +
                    "ON_DEMAND_LOAD cannot co-exists.");
        }

        synchronized (this.context) {
            if (enable) {
                this.context.options.add(option);
            } else {
                this.context.options.remove(option);
            }
        }

        if (option == ServiceOption.DOCUMENT_OWNER) {
            EnumSet<ServiceOption> addedOptions = null;
            EnumSet<ServiceOption> removedOptions = null;
            EnumSet<ServiceOption> docOwner =
                    EnumSet.of(ServiceOption.DOCUMENT_OWNER);
            if (enable) {
                addedOptions = docOwner;
            } else {
                removedOptions = docOwner;
            }
            getHost().scheduleServiceOptionToggleMaintenance(getSelfLink(),
                    addedOptions, removedOptions);
        }
    }

    @Override
    public void setSelfLink(String path) {
        if (this.context.processingStage != Service.ProcessingStage.CREATED) {
            throw new IllegalStateException(
                    "Self link can not change past initialization");
        }

        this.context.selfLink = path;
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
                    if (this.context.isUpdateActive || !this.context.operationQueue.isEmpty()) {
                        failure = new IllegalStateException("Service has active updates");
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
            getHost().processPendingServiceAvailableOperations(this, null, false);
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
        if (this.hasOption(ServiceOption.REPLICATION)) {
            // assume target is also replicated. If they are not, there is only a tiny performance
            // hit due to the availability registration and instant completion
            op.setTargetReplicated(true);
        }
        op.setReferer(UriUtils.buildUri(getHost().getPublicUri(), getSelfLink()));
    }

    /**
     * Gets the cached ServiceDocumentDescription instance for the service state.
     */
    public ServiceDocumentDescription getStateDescription() {
        return getHost().buildDocumentDescription(this);
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
        doLogging(Level.SEVERE, () -> Utils.toString(e));
    }

    public void logSevere(String fmt, Object... args) {
        doLogging(Level.SEVERE, () -> String.format(fmt, args));
    }

    public void logSevere(Supplier<String> messageSupplier) {
        doLogging(Level.SEVERE, messageSupplier);
    }

    public void logInfo(String fmt, Object... args) {
        doLogging(Level.INFO, () -> String.format(fmt, args));
    }

    public void logInfo(Supplier<String> messageSupplier) {
        doLogging(Level.INFO, messageSupplier);
    }

    public void logFine(String fmt, Object... args) {
        doLogging(Level.FINE, () -> String.format(fmt, args));
    }

    public void logFine(Supplier<String> messageSupplier) {
        doLogging(Level.FINE, messageSupplier);
    }

    public void logWarning(String fmt, Object... args) {
        doLogging(Level.WARNING, () -> String.format(fmt, args));
    }

    public void logWarning(Supplier<String> messageSupplier) {
        doLogging(Level.WARNING, messageSupplier);
    }

    public void log(Level level, String fmt, Object... args) {
        doLogging(level, () -> String.format(fmt, args));
    }

    public void log(Level level, Supplier<String> messageSupplier) {
        doLogging(level, messageSupplier);
    }

    protected void doLogging(Level level, Supplier<String> messageSupplier) {
        String uri = this.context.host != null && getUri() != null ? getUri().toString()
                : this.getClass().getSimpleName();
        Logger lg = Logger.getLogger(this.getClass().getName());
        Utils.log(lg, 3, uri, level, messageSupplier);
    }

    /**
     * Infrastructure use. Invoked by host to execute a service handler for a maintenance request.
     * ServiceMaintenanceRequest object is set in the operation body, with the reasons field
     * indicating the maintenance reason. Its invoked when
     *
     * 1) Periodically, if ServiceOption.PERIODIC_MAINTENANCE is set.
     *
     * 2) Node group change.
     *
     * Services should override handlePeriodicMaintenance and handleNodeGroupMaintenance.
     *
     * An implementation of this method that needs to interact with the state of this service must
     * do so as if it were a client of this service. That is: the state of the service should be
     * retrieved by requesting a GET; and the state of the service should be mutated by submitting a
     * PATCH, PUT or DELETE.
     */
    @Override
    public void handleMaintenance(Operation post) {
        ServiceMaintenanceRequest request = post.getBody(ServiceMaintenanceRequest.class);
        if (request.reasons
                .contains(ServiceMaintenanceRequest.MaintenanceReason.PERIODIC_SCHEDULE)) {
            this.handlePeriodicMaintenance(post);
        } else if (request.reasons
                .contains(ServiceMaintenanceRequest.MaintenanceReason.NODE_GROUP_CHANGE)) {
            this.handleNodeGroupMaintenance(post);
        } else {
            post.complete();
        }
    }

    /**
     * Invoked by the host periodically, if ServiceOption.PERIODIC_MAINTENANCE is set.
     * ServiceMaintenanceRequest object is set in the operation body, with the reasons field
     * indicating the maintenance reason.
     *
     * An implementation of this method that needs to interact with the state of this service must
     * do so as if it were a client of this service. That is: the state of the service should be
     * retrieved by requesting a GET; and the state of the service should be mutated by submitting a
     * PATCH, PUT or DELETE.
     */
    public void handlePeriodicMaintenance(Operation post) {
        post.complete();
    }

    /**
     * Invoked by the host on node group change.
     * ServiceMaintenanceRequest object is set in the operation body, with the reasons field
     * indicating the maintenance reason.
     *
     * An implementation of this method that needs to interact with the state of this service must
     * do so as if it were a client of this service. That is: the state of the service should be
     * retrieved by requesting a GET; and the state of the service should be mutated by submitting a
     * PATCH, PUT or DELETE.
     */
    public void handleNodeGroupMaintenance(Operation post) {
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

    protected boolean checkForBody(Operation operation) {
        if (!operation.hasBody()) {
            operation.fail(new IllegalArgumentException("body is required"));
            return false;
        }
        return true;
    }

    protected <T extends ServiceDocument> T getBody(Operation op) {
        @SuppressWarnings("unchecked")
        T body = (T) op.getBody(this.context.stateType);
        return body;
    }

    protected String getSelfId() {
        return Service.getId(getSelfLink());
    }

    /**
     * Value indicating whether GET on /available returns 200 or 503
     * The method is a convenience method since it relies on STAT_NAME_AVAILABLE to report
     * availability.
     */
    public void setAvailable(boolean isAvailable) {
        this.toggleOption(ServiceOption.INSTRUMENTATION, true);
        this.setStat(STAT_NAME_AVAILABLE, isAvailable ? STAT_VALUE_TRUE : STAT_VALUE_FALSE);
    }

    /**
     * Value indicating whether GET on /available returns 200 or 503
     */
    public boolean isAvailable() {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return true;
        }
        // processing stage must also indicate service is started
        if (getProcessingStage() != ProcessingStage.PAUSED
                && getProcessingStage() != ProcessingStage.AVAILABLE) {
            return false;
        }
        ServiceStat st = this.getStat(STAT_NAME_AVAILABLE);
        if (st != null && st.latestValue == STAT_VALUE_TRUE) {
            return true;
        }
        return false;
    }

    @Override
    public void setMaintenanceIntervalMicros(long micros) {
        if (micros < 0) {
            throw new IllegalArgumentException("micros must be positive");
        }

        if (micros > 0 && micros < Service.MIN_MAINTENANCE_INTERVAL_MICROS) {
            logWarning("Maintenance interval %d is less than the minimum interval %d"
                    + ", reducing to min interval", micros,
                    Service.MIN_MAINTENANCE_INTERVAL_MICROS);
            micros = Service.MIN_MAINTENANCE_INTERVAL_MICROS;
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
     * Adds the specified coordinator link to this service' pending transactions
     */
    void addPendingTransaction(String txCoordinatorLink) {
        synchronized (this.context) {
            if (this.context.txCoordinatorLinks == null) {
                this.context.txCoordinatorLinks = new HashSet<>();
            }
            this.context.txCoordinatorLinks.add(txCoordinatorLink);
        }

        toggleOption(ServiceOption.TRANSACTION_PENDING, true);
    }

    /**
     * Removes the specified coordinator link from this service' pending transactions
     */
    void removePendingTransaction(String txCoordinatorLink) {
        boolean toggleTransactionPending = false;

        synchronized (this.context) {
            if (this.context.txCoordinatorLinks == null) {
                return;
            }
            this.context.txCoordinatorLinks.remove(txCoordinatorLink);
            toggleTransactionPending = this.context.txCoordinatorLinks.isEmpty();
        }

        if (toggleTransactionPending) {
            toggleOption(ServiceOption.TRANSACTION_PENDING, false);
        }
    }

    /**
     * Return this service' pending transactions. The returned reference is an
     * internal reference (to avoid cloning overhead) hence it should not
     * be accessed outside the scope of the current request.
     */
    Set<String> getPendingTransactions() {
        synchronized (this.context) {
            return this.context.txCoordinatorLinks;
        }
    }

    /**
     * Most of the transaction-related code becomes obsolete if we haven't seen transactions. A good
     * idea is to check whether the service has pending transactions
     */
    private boolean hasPendingTransactions() {
        synchronized (this.context) {
            return this.context.txCoordinatorLinks != null
                    && !this.context.txCoordinatorLinks.isEmpty();
        }
    }

    /**
     * Allocates this service' pending transactions if not already allocated
     */
    private void allocatePendingTransactions() {
        synchronized (this.context) {
            if (this.context.txCoordinatorLinks == null) {
                this.context.txCoordinatorLinks = new HashSet<>();
            }
        }
    }

}
