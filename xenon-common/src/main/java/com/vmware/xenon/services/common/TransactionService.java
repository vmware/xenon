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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.TransactionContext;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
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
    }

    /**
     * A conflict check request to be sent to a parent coordinator
     */
    public static class ConflictCheckRequest {
        /**
         * Link of service with potential conflict
         */
        public String serviceLink;
    }

    /**
     * A conflict check response sent by a parent coordinator
     */
    public static class ConflictCheckResponse {
        /**
         * Substage of the parent coordinator
         */
        public SubStage subStage;

        /**
         * Whether the potentially conflicting service is in the parent
         * coordinator's write set
         */
        public boolean serviceIsInWriteSet;
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
         * Options customizing the behavior (performance, guarantees) of the coordinator/transactions.
         */
        public Options options;

        /**
         * Keeps track of services that have failed.
         */
        public Set<String> failedLinks;

        /**
         * A list of tenant links which can access this service.
         */
        public Set<String> tenantLinks;
    }

    private TransactionResolutionService resolutionHelper;

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
        this.resolutionHelper = new TransactionResolutionService(this);
        op.complete();
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

        String serviceLink = put.getRequestHeader(Operation.TRANSACTION_REFLINK_HEADER);
        if (record.action == Action.GET) {
            existing.readLinks.add(serviceLink);
        } else {
            existing.modifiedLinks.add(serviceLink);
        }
        if (record.action == Action.POST) {
            existing.createdLinks.add(serviceLink);
        }
        if (record.action == Action.DELETE) {
            existing.deletedLinks.add(serviceLink);
        }

        // This has the possibility of overwriting existing pending, but that's OK, because it means the service
        // evolved, either by (being asked to) commit/abort or having seen more operations -- in any case, this
        // "pending" is the most recent one, so we're good.
        if (record.coordinatorLinks != null) {
            existing.servicesToCoordinators.put(serviceLink,
                    record.coordinatorLinks);
        }
        if (!record.isSuccessful) {
            existing.failedLinks.add(serviceLink);
        }
        setState(put, existing);
        put.complete();
    }

    @Override
    public void handleConfigurationRequest(Operation request) {
        // resolution requests arriving directly from clients need to be routed through the resolution helper
        this.resolutionHelper.handleResolutionRequest(request);
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

        // Check if this is a conflict resolution check from another coordinator
        if (Operation.TX_ENSURE_COMMIT
                .equals(patch.getRequestHeaderAsIs(Operation.TRANSACTION_HEADER))) {
            handleCheckConflicts(patch);
            return;
        }

        // Handle ResolutionRequest
        ResolutionRequest resolution = patch.getBody(ResolutionRequest.class);
        if (!resolution.kind.equals(ResolutionRequest.KIND)) {
            patch.fail(new IllegalArgumentException(
                    "Unrecognized request kind: " + resolution.kind));
            return;
        }

        TransactionServiceState currentState = getState(patch);
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

            updateStage(patch, SubStage.RESOLVING);
            patch.complete();
            handleCommit(currentState);
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
     * Handle the case where a client requests to commit. This initiates conflict resolution, state updates etc.
     */
    private void handleCommit(TransactionServiceState existing) {
        if (existing.options.allowErrorsCauseAbort && !existing.failedLinks.isEmpty()) {
            logWarning("Failed to commit: some transactional operations have failed. Aborting.");
            selfPatch(ResolutionKind.ABORT);
            return;
        }

        checkPotentialConflicts(existing);
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
     * Sends a selfPatch resolution request
     */
    private void selfPatch(ResolutionKind resolution) {
        ResolutionRequest resolve = new ResolutionRequest();
        resolve.resolutionKind = resolution;
        Operation operation = Operation
                .createPatch(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Failure self patching: %s", e.getMessage());
                    }
                })
                .setBody(resolve)
                .setTransactionId(null);
        sendRequest(operation);
    }

    /**
     * Handle the case when a client requests to abort.
     */
    private void handleAbort(TransactionServiceState existing) {
        Collection<Operation> ops = createNotifyServicesToAbort(existing);
        if (ops.isEmpty()) {
            selfPatch(ResolutionKind.ABORTED);
            return;
        }

        OperationJoin.create(ops)
                .setCompletion((operations, failures) -> {
                    if (failures != null) {
                        logWarning("Transaction failed to notify some services to abort: %s",
                                failures.toString());
                    }
                    selfPatch(ResolutionKind.ABORTED);
                }).sendWith(this);
    }

    /**
     * Handle the case when another, peer coordinator is about to commit, and
     * wants to learn about a coordinator (this) in its write set.
     */
    private void handleCheckConflicts(Operation op) {
        TransactionServiceState existing = getState(op);
        ConflictCheckRequest req = op.getBody(ConflictCheckRequest.class);
        ConflictCheckResponse res = new ConflictCheckResponse();
        res.subStage = existing.taskSubStage;
        res.serviceIsInWriteSet = existing.modifiedLinks.contains(req.serviceLink);
        boolean abort = false;

        if (existing.taskSubStage == SubStage.COLLECTING
                || existing.taskSubStage == SubStage.RESOLVING) {
            // potential conflict - resolve deterministically
            String txLink = op.getRequestHeader(Operation.TRANSACTION_REFLINK_HEADER);
            if (!compareTo(txLink)) {
                logInfo("Conflicting transaction %s is trying to commit, aborting this transaction...",
                        txLink);
                abort = true;
                updateStage(op, SubStage.ABORTING);
                res.subStage = SubStage.ABORTING;
            }
        }

        op.setBodyNoCloning(res);
        op.complete();

        if (abort) {
            handleAbort(existing);
        }
    }

    /**
     * Check potential conflicts with coordinators corresponding with
     * this coordinator's write-set.
     */
    private void checkPotentialConflicts(TransactionServiceState state) {
        Collection<Operation> operations = new HashSet<>();
        boolean[] continueWithCommit = new boolean[1];
        continueWithCommit[0] = true;

        for (String serviceLink : state.modifiedLinks) {
            if (!state.servicesToCoordinators.containsKey(serviceLink)) {
                continue;
            }
            for (String coordinator : state.servicesToCoordinators.get(serviceLink)) {
                if (coordinator.equals(getSelfLink())) {
                    continue;
                }

                operations.add(createNotifyOp(coordinator, serviceLink,
                        Operation.TX_ENSURE_COMMIT, (o, e) -> {
                            if (e != null) {
                                continueWithCommit[0] = false;
                                logWarning(
                                        "Failed to receive response from transaction %s, aborting this transaction...",
                                        coordinator);
                                selfPatch(ResolutionKind.ABORT);
                                return;
                            }

                            ConflictCheckResponse res = o
                                    .getBody(ConflictCheckResponse.class);
                            if (!res.serviceIsInWriteSet || res.subStage == SubStage.ABORTED
                                    || res.subStage == SubStage.ABORTING) {
                                // no conflict
                                return;
                            }

                            if (res.subStage == SubStage.COMMITTED
                                    || res.subStage == SubStage.COMMITTING
                                    || !compareTo(coordinator)) {
                                continueWithCommit[0] = false;
                                logInfo("Conflicting transaction %s is committing, aborting this transaction...",
                                        coordinator);
                                selfPatch(ResolutionKind.ABORT);
                            }
                        }));
            }
        }

        if (operations.isEmpty()) {
            selfPatch(ResolutionKind.COMMITTING);
            return;
        }

        OperationJoin.create(operations).setCompletion((ops, failures) -> {
            if (failures != null) {
                logWarning("Failed to commit: %s. Aborting.", failures);
                selfPatch(ResolutionKind.ABORT);
                return;
            }
            if (continueWithCommit[0]) {
                selfPatch(ResolutionKind.COMMITTING);
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
        }
        for (String service : state.readLinks) {
            if (!state.createdLinks.contains(service)) {
                operations.add(createNotifyOp(service, Operation.TX_ABORT));
            }
        }
        for (String service : state.modifiedLinks) {
            if (!state.createdLinks.contains(service) && !state.readLinks.contains(service)) {
                operations.add(createNotifyOp(service, Operation.TX_ABORT));
            }
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
        }
        for (String service : state.readLinks) {
            if (!state.deletedLinks.contains(service)) {
                operations.add(createNotifyOp(service, Operation.TX_COMMIT));
            }
        }
        for (String service : state.modifiedLinks) {
            if (!state.deletedLinks.contains(service) && !state.readLinks.contains(service)) {
                operations.add(createNotifyOp(service, Operation.TX_COMMIT));
            }
        }

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
                .addRequestHeader(Operation.TRANSACTION_REFLINK_HEADER, getSelfLink())
                .setTransactionId(null)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Notification %s of service %s failed: %s", header, service, e);
                    } else {
                        logInfo("Notification %s of service %s succeeded", header, service);
                    }
                });
    }

    /**
     * Prepare a delete request to a service
     */
    private Operation createDeleteOp(String service) {
        Operation deleteOp =  Operation
                .createDelete(this, service)
                .setReferer(getUri())
                .addRequestHeader(Operation.TRANSACTION_REFLINK_HEADER, getSelfLink())
                .setTransactionId(null)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logWarning("Deletion of service %s failed: %s", service, e);
                    } else {
                        logInfo("Deletion of service %s succeeded", service);
                    }
                });
        setAuthorizationContext(deleteOp, getSystemAuthorizationContext());
        return deleteOp;
    }

    /**
     * Prepare an operation to check conflicts with a remote coordinator
     */
    private Operation createNotifyOp(String coordinator, String serviceLink,
            String header, Operation.CompletionHandler callback) {
        ConflictCheckRequest body = new ConflictCheckRequest();
        body.serviceLink = serviceLink;
        return Operation
                .createPatch(this, coordinator)
                .addRequestHeader(Operation.TRANSACTION_HEADER, header)
                .setBody(body)
                .setReferer(getUri())
                .addRequestHeader(Operation.TRANSACTION_REFLINK_HEADER, getSelfLink())
                .setTransactionId(null)
                .setCompletion(callback);
    }

}
