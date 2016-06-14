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

package com.vmware.xenon.services.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.TransactionContext;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Transaction Coordinator responsible for driving a single transaction. Naming conventions follow
 * the protocol conventions described in https://goo.gl/Mg4QcH
 * <p>
 * FIXME: "Try" and "Ensure" are WIP terms. We will be changing these soon.
 */
public class TransactionService extends StatefulService {

    /**
     * Capture status of coordinator.
     */
    public enum SubStage {
        /**
         * Collecting logs from services
         */
        COLLECTING,

        /**
         * Received request to commit; in the process resolving conflicts
         */
        RESOLVING,

        /**
         * Substage to be sent as a response, when in resolving phase but also detected circular
         * dependency with the coordinator issuing the request
         */
        RESOLVING_CIRCULAR,

        /**
         * No conflicts found, proceed with commit
         */
        COMMITTING,

        /**
         * Received request to abort; in the process of rolling back
         */
        ABORTING,

        /**
         * Commit tombstone
         */
        COMMITTED,

        /**
         * Abort tombstone
         */
        ABORTED
    }

    /**
     * Capture possible resolution requests; resolved by the PATCH handler
     */
    public enum ResolutionKind {
        COMMIT, ABORT, COMMITTING,
        // fire up notifications upon completion self-patch
        COMMITTED, ABORTED
    }

    /**
     * Various options that affect the performance and guarantees transactions offer. Default values are
     * made explicit for self-documentation purposes.
     */
    public static class Options {
        /**
         * Distinguish the semantics of HTTP errors from the properties transactions guarantee. For instance,
         * creating an existing service instance would cause an exception -- whereas this does not alter the
         * semantics of a "safe" transaction.
         */
        public boolean allowErrorsCauseAbort = true;
    }

    /**
     * Captures a request to commit or abort
     */
    public static class ResolutionRequest {
        public static final String KIND = Utils.buildKind(ResolutionRequest.class);
        public String kind = KIND;

        /**
         * Commit or Abort
         */
        public ResolutionKind resolutionKind;
        /**
         * Number of operations the coordinator must know about -- if operations issued
         * fully-asynchronously/concurrently.
         */
        public int pendingOperations;
    }

    /**
     * Captures a request to add a dependency to dependentLinks.
     */
    public static class AddDependentCoordinatorRequest {
        public static final String KIND = Utils.buildKind(AddDependentCoordinatorRequest.class);
        public String kind = KIND;
        public String coordinatorLink;
    }

    /**
     * The document backing up the service is consulted upon commit time. It includes a couple
     * of optimization fields that would allow it to proceed instantly in the common case (i.e.,
     * global overlaps, per-service potential conflicts) as well as a single level of the dependency
     * graph.
     */
    public static class TransactionServiceState extends ServiceDocument {
        /**
         * Set of services operations within the transaction read (e.g., GET) state from
         */
        public Set<String> readLinks;

        /**
         * Set of services operations within the transaction wrote (e.g., PUT, PATCH) state to
         */
        public Set<String> modifiedLinks;

        /**
         * Set of services that have been created in the context of this transaction
         */
        public Set<String> createdLinks;

        /**
         * Set of services that have been deleted in the context of this transaction
         */
        public Set<String> deletedLinks;

        /**
         * A mapping from services to coordinators responsible for pending operations on these services
         */
        public LinkedHashMap<String, Set<String>> servicesToCoordinators;

        /**
         * Tracks the task's stages. Managed by DCP.
         */
        public TaskState taskInfo = new TaskState();

        /**
         * Current stage in the protocol -- updated atomically
         */
        public SubStage taskSubStage;

        /**
         * A set of transactions that should commit before this one
         */
        public Set<String> dependentLinks;

        /**
         * Options customizing the behavior (performance, guarantees) of the coordinator/transactions.
         */
        public Options options;

        /**
         * Keeps track of services that have failed.
         */
        public Set<String> failedLinks;

        /**
         * Number of received transactional operations sent by services participating in the transaction
         */
        public int pendingOperationCount;

        /**
         * Number of expected transactional operations to be sent by services participating in the transaction
         */
        public int expectedOperationCount;
    }

    /**
     * Using increased guarantees to make sure the coordinator can sustain failures.
     */
    public TransactionService() {
        super(TransactionServiceState.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    /**
     * Initiate transaction context. It maintains options, but corrects stage to "collecting"
     * <p>
     * TODO: How to check if service is restarting from disk?
     */
    @Override
    public void handleStart(Operation start) {
        TransactionServiceState s = start.getBody(TransactionServiceState.class);

        // Default state
        s.taskSubStage = s.taskSubStage == null ? SubStage.COLLECTING : s.taskSubStage;
        s.options = s.options == null ? new Options() : s.options;
        s.servicesToCoordinators = s.servicesToCoordinators == null ? new LinkedHashMap<>()
                : s.servicesToCoordinators;
        s.readLinks = s.readLinks == null ? new HashSet<>() : s.readLinks;
        s.modifiedLinks = s.modifiedLinks == null ? new HashSet<>() : s.modifiedLinks;
        s.createdLinks = s.createdLinks == null ? new HashSet<>() : s.createdLinks;
        s.deletedLinks = s.deletedLinks == null ? new HashSet<>() : s.deletedLinks;
        s.dependentLinks = s.dependentLinks == null ? new HashSet<>() : s.dependentLinks;
        s.failedLinks = new HashSet<>();

        setState(start, s);
        // unfortunately, we need to allocate before we complete, because otherwise the client
        // is racing to commit, and there is a chance he might make it first
        allocateResolutionService(start);
    }

    /**
     * Allocate the stateless subservice responsible for initiating the commit resolution, while it holds the client
     * until the resolution is complete.
     */
    private void allocateResolutionService(Operation op) {
        Operation startResolutionService = Operation.createPost(
                UriUtils.extendUri(getUri(), TransactionResolutionService.RESOLUTION_SUFFIX))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    op.complete();
                });
        getHost().startService(startResolutionService, new TransactionResolutionService(this));
    }

    /**
     * Log new operations -- requests come in the form {Action, URI, [URI]}
     */
    @Override
    public void handlePut(Operation put) {
        if (!put.hasBody()) {
            put.fail(new IllegalArgumentException("Body is required"));
            return;
        }
        TransactionContext record = put.getBody(TransactionContext.class);
        TransactionServiceState existing = getState(put);

        if (record.action == Action.GET) {
            existing.readLinks.add(put.getReferer().getPath());
        } else {
            existing.modifiedLinks.add(put.getReferer().getPath());
        }
        if (record.action == Action.POST) {
            existing.createdLinks.add(put.getReferer().getPath());
        }
        if (record.action == Action.DELETE) {
            existing.deletedLinks.add(put.getReferer().getPath());
        }

        // This has the possibility of overwriting existing pending, but that's OK, because it means the service
        // evolved, either by (being asked to) commit/abort or having seen more operations -- in any case, this
        // "pending" is the most recent one, so we're good.
        if (record.coordinatorLinks != null) {
            existing.servicesToCoordinators.put(put.getReferer().getPath(),
                    record.coordinatorLinks);
        }
        if (!record.isSuccessful) {
            if (existing.failedLinks == null) {
                existing.failedLinks = new HashSet<>();
            }
            existing.failedLinks.add(put.getReferer().getPath());
        }
        ++existing.pendingOperationCount;
        setState(put, existing);
        put.complete();

        if (existing.taskSubStage == SubStage.RESOLVING
                && existing.pendingOperationCount == existing.expectedOperationCount) {
            // handle the case of pending operations received after transaction commit request
            selfPatch(ResolutionKind.COMMIT, existing.expectedOperationCount);
        }
    }

    /**
     * Handles commits and aborts (requests to cancel/rollback).
     * TODO 1: Should allow receiving number of transactions in flight
     * TODO 2: Does not support 2PC for integrity checks yet! This will require specific handler from services
     * TODO 3: Should eventually shield commit/abort against operations not coming via the /resolve utility suffix
     */
    @Override
    public void handlePatch(Operation patch) {
        if (!patch.hasBody()) {
            patch.fail(new IllegalArgumentException("Body is required"));
            return;
        }

        // In the first two cases, another coordinator is asking about state
        // in the fall-back, client is sending a commit/abort request.
        if (patch.getRequestHeader(Operation.TRANSACTION_HEADER) != null) {
            if (patch.getRequestHeader(Operation.TRANSACTION_HEADER)
                    .equals(Operation.TX_TRY_COMMIT)) {
                handleTryCommit(patch);
            } else if (patch.getRequestHeader(Operation.TRANSACTION_HEADER)
                    .equals(Operation.TX_ENSURE_COMMIT)) {
                handleEnsureCommit(patch);
            }
            return;
        }

        // Handle AddDependentCoordinator request
        TransactionServiceState currentState = getState(patch);
        AddDependentCoordinatorRequest addDependentCoordinatorRequest = patch
                .getBody(AddDependentCoordinatorRequest.class);
        if (addDependentCoordinatorRequest.kind == AddDependentCoordinatorRequest.KIND) {
            currentState.dependentLinks.add(addDependentCoordinatorRequest.coordinatorLink);
            patch.complete();
            return;
        }

        // Handle ResolutionRequest
        ResolutionRequest resolution = patch.getBody(ResolutionRequest.class);
        if (resolution.kind != ResolutionRequest.KIND) {
            patch.fail(new IllegalArgumentException(
                    "Unrecognized request kind: " + resolution.kind));
            return;
        }

        if (resolution.resolutionKind == ResolutionKind.ABORT) {
            if (currentState.taskSubStage == SubStage.COMMITTED
                    || currentState.taskSubStage == SubStage.COMMITTING) {
                patch.fail(new IllegalStateException(
                        String.format("Already %s", currentState.taskSubStage)));
                return;
            }

            if (currentState.taskSubStage == SubStage.ABORTING
                    || currentState.taskSubStage == SubStage.ABORTED) {
                logInfo("Alreading in sub-stage %s. Completing request.",
                        currentState.taskSubStage);
                patch.complete();
                return;
            }

            updateStage(patch, SubStage.ABORTING);
            patch.complete();
            handleAbort(currentState);
        } else if (resolution.resolutionKind == ResolutionKind.COMMIT) {
            if (currentState.taskSubStage == SubStage.ABORTED
                    || currentState.taskSubStage == SubStage.ABORTING) {
                patch.fail(new IllegalStateException("Already aborted"));
                return;
            }

            if (currentState.taskSubStage == SubStage.COMMITTED
                    || currentState.taskSubStage == SubStage.COMMITTING) {
                logInfo("Alreading in sub-stage %s. Completing request.",
                        currentState.taskSubStage);
                patch.complete();
                return;
            }

            currentState.expectedOperationCount = resolution.pendingOperations;
            updateStage(patch, SubStage.RESOLVING);
            patch.complete();
            handleCommitIfAllPendingOperationsReceived(currentState);
        } else if (resolution.resolutionKind == ResolutionKind.COMMITTING) {
            if (currentState.taskSubStage == SubStage.ABORTED
                    || currentState.taskSubStage == SubStage.ABORTING) {
                patch.fail(new IllegalStateException("Already aborted"));
                return;
            }

            updateStage(patch, SubStage.COMMITTING);
            patch.complete();
            notifyServicesToCommit(currentState);
        } else if (resolution.resolutionKind == ResolutionKind.COMMITTED) {
            updateStage(patch, SubStage.COMMITTED);
            patch.complete();
        } else if (resolution.resolutionKind == ResolutionKind.ABORTED) {
            updateStage(patch, SubStage.ABORTED);
            patch.complete();
        } else {
            patch.fail(new IllegalArgumentException(
                    "Unrecognized resolution kind: " + resolution.resolutionKind));
        }
    }

    /**
     * Checks if all pending operations sent by participating services have
     * been received by this coordinator, and if so proceeds with commit
     */
    private void handleCommitIfAllPendingOperationsReceived(TransactionServiceState currentState) {
        if (currentState.pendingOperationCount == currentState.expectedOperationCount) {
            logInfo("All operations have been received, proceeding with commit");
            handleCommit(currentState);
            return;
        }

        if (currentState.pendingOperationCount > currentState.expectedOperationCount) {
            String errorMsg = String.format(
                    "Illegal commit request: client provided pending operations %d is less than already received %d",
                    currentState.expectedOperationCount, currentState.pendingOperationCount);
            logWarning(errorMsg);
            selfPatch(ResolutionKind.ABORT);
            return;
        }

        // re-enter when the rest of the pending operations are received
        logInfo("Suspending transaction until all operations have been received");
    }

    /**
     * Handle the case where a client requests to commit. This initiates conflict resolution, state updates etc.
     */
    private void handleCommit(TransactionServiceState existing) {
        if (existing.options.allowErrorsCauseAbort && !existing.failedLinks.isEmpty()) {
            logWarning("Failed to commit: some transactional operations have failed. Aborting.");
            selfPatch(ResolutionKind.ABORT);
            return;
        }

        // kick-off the try-precede -> ensure-precede -> committed sequence
        tryPrecede(existing);
    }

    /**
     * Update stage to new stage
     */
    private void updateStage(Operation op, SubStage stage) {
        TransactionServiceState existing = getState(op);
        existing.taskSubStage = stage;
        setState(op, existing);
    }

    /**
     * Sends a selfPatch resolution request with zero pending operations
     */
    private void selfPatch(ResolutionKind resolution) {
        selfPatch(resolution, 0);
    }

    /**
     * Sends a selfPatch resolution request
     */
    private void selfPatch(ResolutionKind resolution, int pendingOperations) {
        ResolutionRequest resolve = new ResolutionRequest();
        resolve.resolutionKind = resolution;
        resolve.pendingOperations = pendingOperations;
        Operation operation = Operation
                .createPatch(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Failure self patching: %s", e.getMessage());
                    }
                })
                .setBody(resolve);
        sendRequest(operation);
    }

    /**
     * Sends a selfPatch AddDependentCoordinator request
     */
    private void selfPatch(String coordinator) {
        AddDependentCoordinatorRequest body = new AddDependentCoordinatorRequest();
        body.coordinatorLink = coordinator;
        Operation operation = Operation
                .createPatch(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Failure self patching: %s", e.getMessage());
                    }
                })
                .setBody(body);
        sendRequest(operation);
    }

    /**
     * Handle the case when a client requests to abort.
     */
    private void handleAbort(TransactionServiceState existing) {
        OperationJoin.create(createNotifyServicesToAbort(existing))
                .setCompletion((operations, failures) -> {
                    if (failures != null) {
                        logWarning("Transaction failed to notify some services to abort: %s",
                                failures.toString());
                    }
                    selfPatch(ResolutionKind.ABORTED);
                }).sendWith(this);
    }

    /**
     * Handle the case when another coordinator is about to commit, and wants to learn about the status
     * of one of the coordinators (this) in its read set. Any commit ordering is allowed, as long as
     * it is maintained across all nodes.
     */
    private void handleTryCommit(Operation op) {
        TransactionServiceState existing = getState(op);
        TransactionServiceState exchangeState = new TransactionServiceState();
        exchangeState.taskSubStage = existing.taskSubStage;
        boolean abort = false;

        if (existing.taskSubStage == SubStage.RESOLVING) {
            // notify about deterministic resolution..
            exchangeState.taskSubStage = SubStage.RESOLVING_CIRCULAR;
            // ..and resolve deterministically
            if (!compareTo(op.getReferer().getPath())) {
                logInfo("Conflicting transaction %s is trying to commit, aborting this transaction...",
                        op.getReferer().getPath());
                abort = true;
                updateStage(op, SubStage.ABORTING);
            }
        }

        op.setBodyNoCloning(exchangeState);
        op.complete();

        if (abort) {
            handleAbort(existing);
        }
    }

    /**
     * Handle the case when another, peer coordinator is about to commit, and wants to learn about a coordinator (this)
     * in its write set. If this coordinator has committed, the peer has to abort; otherwise, this
     * has to ensure it commits afterwards.
     */
    private void handleEnsureCommit(Operation op) {
        TransactionServiceState existing = getState(op);
        TransactionServiceState exchangeState = new TransactionServiceState();
        exchangeState.taskSubStage = existing.taskSubStage;
        boolean abort = false;

        if (existing.taskSubStage == SubStage.COLLECTING
                || existing.taskSubStage == SubStage.RESOLVING) {
            // notify about deterministic resolution..
            exchangeState.taskSubStage = SubStage.RESOLVING_CIRCULAR;
            // ..and resolve deterministically
            if (!compareTo(op.getReferer().getPath())) {
                logInfo("Conflicting transaction %s is trying to commit, aborting this transaction...",
                        op.getReferer().getPath());
                abort = true;
                updateStage(op, SubStage.ABORTING);
            }
        }

        op.setBodyNoCloning(exchangeState);
        op.complete();

        if (abort) {
            handleAbort(existing);
        }
    }

    /**
     * Contacts peer coordinators in the read set -- in an effort to set "mustCommitAfter".
     */
    private void tryPrecede(TransactionServiceState state) {
        // keep a cache of contacted coordinators, to minimize network overhead
        HashSet<String> cache = new HashSet<>();
        Collection<Operation> operations = new HashSet<>();
        boolean[] continueWithCommit = new boolean[1];
        continueWithCommit[0] = true;

        for (String service : state.readLinks) {
            if (!state.servicesToCoordinators.containsKey(service)) {
                continue;
            }
            for (String coordinator : state.servicesToCoordinators.get(service)) {
                if ((cache.contains(coordinator)) || (coordinator.equals(getSelfLink()))) {
                    continue;
                } else {
                    cache.add(coordinator);
                }

                // a peer at RESOLVING (incl. RESOLVING_CIRCULAR) will append this to mustCommitAfter
                operations.add(createNotifyOp(coordinator,
                        Operation.TX_TRY_COMMIT, (o, e) -> {
                            if (e == null) {
                                TransactionServiceState exchangeState = o
                                        .getBody(TransactionServiceState.class);
                                SubStage s = exchangeState.taskSubStage;
                                if (s == SubStage.COMMITTED || s == SubStage.COMMITTING) {
                                    selfPatch(coordinator);
                                } else if (s == SubStage.RESOLVING_CIRCULAR) {
                                    if (!compareTo(coordinator)) {
                                        continueWithCommit[0] = false;
                                        logInfo("Conflicting transaction %s is committing, aborting this transaction...",
                                                coordinator);
                                        selfPatch(ResolutionKind.ABORT);
                                    }
                                }
                            }
                        }));
            }
        }

        if (operations.isEmpty()) {
            ensurePrecede(state);
            return;
        }

        OperationJoin.create(operations).setCompletion((ops, failures) -> {
            if (failures != null) {
                logWarning("Failed to commit: %s. Aborting.", failures);
                selfPatch(ResolutionKind.ABORT);
                return;
            }

            if (continueWithCommit[0]) {
                ensurePrecede(state);
            }
        }).sendWith(getHost());
    }

    /**
     * Contacts peer coordinators in the write set -- in an effort to ensure sequencing
     */
    private void ensurePrecede(TransactionServiceState state) {
        // keep a cache of contacted coordinators, to minimize network overhead
        HashSet<String> cache = new HashSet<>();
        Collection<Operation> operations = new HashSet<>();
        boolean[] continueWithCommit = new boolean[1];
        continueWithCommit[0] = true;

        for (String service : state.modifiedLinks) {
            if (!state.servicesToCoordinators.containsKey(service)) {
                continue;
            }
            for (String coordinator : state.servicesToCoordinators.get(service)) {
                if ((cache.contains(coordinator)) || (coordinator.equals(getSelfLink()))) {
                    continue;
                } else {
                    cache.add(coordinator);
                }
                operations.add(createNotifyOp(coordinator,
                        Operation.TX_ENSURE_COMMIT, (o, e) -> {
                            if (e == null) {
                                TransactionServiceState exchangeState = o
                                        .getBody(TransactionServiceState.class);
                                SubStage s = exchangeState.taskSubStage;
                                if (s == SubStage.COMMITTED || s == SubStage.COMMITTING) {
                                    continueWithCommit[0] = false;
                                    logInfo("Conflicting transaction %s is committing, aborting this transaction...",
                                            coordinator);
                                    selfPatch(ResolutionKind.ABORT);
                                } else if (s == SubStage.RESOLVING_CIRCULAR) {
                                    if (!compareTo(coordinator)) {
                                        continueWithCommit[0] = false;
                                        logInfo("Conflicting transaction %s is committing, aborting this transaction...",
                                                coordinator);
                                        selfPatch(ResolutionKind.ABORT);
                                    }
                                }
                            }
                        }));
            }
        }

        if (operations.isEmpty()) {
            selfPatch(ResolutionKind.COMMITTING, state.pendingOperationCount);
            return;
        }

        OperationJoin.create(operations).setCompletion((ops, failures) -> {
            if (failures != null) {
                logWarning("Failed to commit: %s. Aborting.", failures);
                selfPatch(ResolutionKind.ABORT);
                return;
            }
            if (continueWithCommit[0]) {
                selfPatch(ResolutionKind.COMMITTING, state.pendingOperationCount);
            }
        }).sendWith(getHost());
    }

    /**
     * Lexicographic comparison between this selfLink and the remote
     * @return true, this wins; false, remote wins
     */
    private boolean compareTo(String remote) {
        return getSelfLink().compareTo(remote) < 0;
    }

    /**
     * Prepare a tiny metadata request to all services to abort
     */
    private Collection<Operation> createNotifyServicesToAbort(TransactionServiceState state) {
        Collection<Operation> operations = new HashSet<>();
        for (String service : state.createdLinks) {
            operations.add(createDeleteOp(service));
            state.readLinks.remove(service);
            state.modifiedLinks.remove(service);
        }
        for (String service : state.readLinks) {
            operations.add(createNotifyOp(service, Operation.TX_ABORT));
        }
        for (String service : state.modifiedLinks) {
            operations.add(createNotifyOp(service, Operation.TX_ABORT));
        }
        return operations;
    }

    /**
     * Send a tiny metadata request to all services to commit
     */
    private void notifyServicesToCommit(TransactionServiceState state) {
        Collection<Operation> operations = new HashSet<>();
        for (String service : state.deletedLinks) {
            operations.add(createDeleteOp(service));
            state.readLinks.remove(service);
            state.modifiedLinks.remove(service);
        }
        operations.addAll(state.readLinks.stream()
                .map(service -> createNotifyOp(service, Operation.TX_COMMIT))
                .collect(Collectors.toSet()));
        operations.addAll(state.modifiedLinks.stream()
                .map(service -> createNotifyOp(service, Operation.TX_COMMIT))
                .collect(Collectors.toSet()));

        if (operations.isEmpty()) {
            selfPatch(ResolutionKind.COMMITTED);
            return;
        }

        OperationJoin.create(operations).setCompletion((ops, failures) -> {
            if (failures != null) {
                logWarning("Failed to commit: %s. Aborting.", failures);
                selfPatch(ResolutionKind.ABORT);
                return;
            }

            selfPatch(ResolutionKind.COMMITTED);
        }).sendWith(getHost());
    }

    /**
     * Prepare a simple metadata request to a service
     */
    private Operation createNotifyOp(String service, String header) {
        // no completion handler, worst case something will fail, nothing will change on part of the service
        // and transactional operations headed there will need to contact us prior to commit
        // (handler would not change this, we will solve this using a transaction GC service)
        return Operation
                .createPatch(this, service)
                .addRequestHeader(Operation.TRANSACTION_HEADER, header)
                // just an empty body
                .setBody(new TransactionServiceState())
                .setReferer(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Notification of service %s failed: %s", service, e);
                    } else {
                        logInfo("Notification of service %s succeeded", service);
                    }
                });
    }

    /**
     * Prepare a delete request to a service
     */
    private Operation createDeleteOp(String service) {
        return Operation
                .createDelete(this, service)
                .setReferer(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Deletion of service %s failed: %s", service, e);
                    } else {
                        logInfo("Deletion of service %s succeeded", service);
                    }
                });
    }

    /**
     * Prepare an operation to resolve precedence with a remote coordinator
     */
    private Operation createNotifyOp(String coordinator, String header,
            Operation.CompletionHandler callback) {
        return Operation
                .createPatch(this, coordinator)
                .addRequestHeader(Operation.TRANSACTION_HEADER, header)
                // just an empty body
                .setBody(new TransactionServiceState())
                .setReferer(getUri())
                .setCompletion(callback);
    }

}
