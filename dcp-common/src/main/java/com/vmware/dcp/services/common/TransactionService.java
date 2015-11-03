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

package com.vmware.dcp.services.common;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.OperationTransactionRecord;
import com.vmware.dcp.common.OperationJoin;
import com.vmware.dcp.common.OperationSequence;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.TaskState;
import com.vmware.dcp.common.UriUtils;

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
        COMMIT, ABORT,
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
        /**
         * Commit or Abort
         */
        public ResolutionKind kind;
        /**
         * Number of operations the coordinator must know about -- if operations issued
         * fully-asynchronously/concurrently.
         */
        public int pendingOperations;
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
         * Set of services opeartions within the transaction wrote (e.g., PUT, PATCH) state to
         */
        public Set<String> modifiedLinks;

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
         * Keeps track of a services that have failed.
         */
        public Set<String> failedLinks;
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
        OperationTransactionRecord record = put.getBody(OperationTransactionRecord.class);
        TransactionServiceState existing = getState(put);

        if (record.action == Action.GET) {
            existing.readLinks.add(put.getReferer().toString());
        } else {
            existing.modifiedLinks.add(put.getReferer().toString());
        }
        // This has the possibility of overwriting existing pending, but that's OK, because it means the service
        // evolved, either by (being asked to) commit/abort or having seen more operations -- in any case, this
        // "pending" is the most recent one, so we're good.
        if (record.pendingTransactions != null) {
            existing.servicesToCoordinators.put(put.getReferer().toString(),
                    record.pendingTransactions);
        }
        if (!record.isSuccessful) {
            if (existing.failedLinks == null) {
                existing.failedLinks = new HashSet<>();
            }
            existing.failedLinks.add(put.getReferer().toString());
        }
        setState(put, existing);
        put.complete();
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
        if (patch.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER) != null) {
            if (patch.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER)
                    .equals(Operation.TX_TRY_COMMIT)) {
                handleTryCommit(patch);
            } else if (patch.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER)
                    .equals(Operation.TX_ENSURE_COMMIT)) {
                handleEnsureCommit(patch);
            }
        } else {
            // we can't use PRAGMAs, because a client encodes action in the message, not in metadata
            ResolutionRequest resolution = patch.getBody(ResolutionRequest.class);
            // both commit and abort are now handled asynchronously, so complete ASAP
            if (resolution.kind == ResolutionKind.ABORT) {
                updateStage(patch, SubStage.ABORTED);
                patch.complete();
                handleAbort(patch);
            } else if (resolution.kind == ResolutionKind.COMMIT) {
                updateStage(patch, SubStage.RESOLVING);
                patch.complete();
                handleCommit(patch);
            } else if (resolution.kind == ResolutionKind.COMMITTED) {
                updateStage(patch, SubStage.COMMITTED);
                patch.complete();
            } else if (resolution.kind == ResolutionKind.ABORTED) {
                updateStage(patch, SubStage.ABORTED);
                patch.complete();
            } else {
                getHost().failRequestActionNotSupported(patch);
                patch.fail(new IllegalArgumentException(
                        "Unrecognized resolution kind: " + resolution.kind));
            }
        }
    }

    /**
     * Handle the case where a client requests to commit. This initiates conflict resolution, state updates etc.
     */
    private void handleCommit(Operation op) {
        TransactionServiceState existing = getState(op);

        if (existing.options.allowErrorsCauseAbort && !existing.failedLinks.isEmpty()) {
            handleAbort(op);
            return;
        }

        // This is to guard against passing empty state to our little concurrency calculus:)
        List<OperationJoin> operationJoins = new ArrayList<>();
        if (!setOfTryPreceedOps(existing).isEmpty()) {
            operationJoins.add(OperationJoin.create(setOfTryPreceedOps(existing)));
        }
        if (!setOfEnsurePreceedOps(existing).isEmpty()) {
            operationJoins.add(OperationJoin.create(setOfEnsurePreceedOps(existing)));
        }
        if (!createNotifyServicesToCommit(existing).isEmpty()) {
            operationJoins.add(OperationJoin.create(createNotifyServicesToCommit(existing)));
        }

        OperationSequence os = OperationSequence
                .create(operationJoins.toArray(new OperationJoin[operationJoins.size()]));
        os.setCompletion((operations, failures) -> {
            if (failures != null) {
                handleAbort(op);
                return;
            }
            selfPatch(ResolutionKind.COMMITTED);
        });
        os.sendWith(getHost());
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
     * Send a selfPatch as requested
     */
    private void selfPatch(ResolutionKind resolution) {
        ResolutionRequest resolve = new ResolutionRequest();
        resolve.kind = resolution;
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
     * Handle the case when a client requests to abort.
     */
    private void handleAbort(Operation op) {
        TransactionServiceState existing = getState(op);
        OperationJoin.create(createNotifyServicesToAbort(existing))
                .setCompletion((operations, failures) -> {
                    if (failures != null) {
                        logWarning("Transaction failed to abort: %s", failures.toString());
                        return;
                    }
                    selfPatch(ResolutionKind.ABORTED);
                }).sendWith(this);
    }

    /**
     * Handle the case when another coordinator is about to commit, and wants to learn about the status
     * of one of the coordinators (this) in its write set. Any commit ordering is allowed, as long as
     * it is maintained across all nodes.
     */
    private void handleTryCommit(Operation op) {
        // TODO: optimize by maintaining a flat, top-level set
        TransactionServiceState existing = getState(op);
        TransactionServiceState exchangeState = new TransactionServiceState();
        for (Set<String> serviceSet : existing.servicesToCoordinators.values()) {
            if (serviceSet.contains(op.getReferer().toString())) {
                // notify about deterministic resolution..
                exchangeState.taskSubStage = SubStage.RESOLVING_CIRCULAR;
                // ..and resolve deterministically
                if (!compareTo(op.getReferer())) {
                    existing.taskSubStage = SubStage.ABORTED;
                }
            }
        }
        // update state
        setState(op, existing);
        op.setBodyNoCloning(exchangeState);
        op.complete();
    }

    /**
     * Handle the case when another, peer coordinator is about to commit, and wants to learn about a coordinator (this)
     * in its read set. If this coordinator has committed, the peer has to abort; otherwise, this
     * has to ensure it commits afterwards.
     */
    private void handleEnsureCommit(Operation op) {
        TransactionServiceState existing = getState(op);
        TransactionServiceState exchangeState = new TransactionServiceState();
        existing.dependentLinks.add(op.getReferer().toString());
        // update state
        setState(op, existing);
        op.setBodyNoCloning(exchangeState);
        op.complete();
    }

    /**
     * Create operations to contact peer coordinators in the read set -- in an effort to set "mustCommitAfter".
     */
    private Collection<Operation> setOfTryPreceedOps(TransactionServiceState state) {
        // keep a cache of contacted coordinators, to minimize network overhead
        HashSet<String> cache = new HashSet<>();
        Collection<Operation> operations = new HashSet<>();

        for (String service : state.readLinks) {
            if (!state.servicesToCoordinators.containsKey(service)) {
                continue;
            }
            for (String coordinator : state.servicesToCoordinators.get(service)) {
                if ((cache.contains(coordinator)) || (coordinator.equals(getUri().toString()))) {
                    continue;
                } else {
                    cache.add(coordinator);
                }

                // a peer at RESOLVING (incl. RESOLVING_CIRCULAR) will append this to mustCommitAfter
                operations.add(createNotifyOp(UriUtils.buildUri(coordinator),
                        Operation.TX_TRY_COMMIT, null, (o, e) -> {
                            if (e == null) {
                                SubStage s = o.getBody(SubStage.class);
                                if (s == SubStage.COMMITTED) {
                                    state.dependentLinks.add(coordinator);
                                } else if (s == SubStage.RESOLVING_CIRCULAR) {
                                    if (!compareTo(o.getReferer())) {
                                        state.taskSubStage = SubStage.ABORTED;
                                    }
                                }
                            }
                        }));
            }
        }
        return operations;
    }

    /**
     * Contacts peer coordinators in the write set -- in an effort to ensure sequencing
     */
    private Collection<Operation> setOfEnsurePreceedOps(TransactionServiceState state) {
        // keep a cache of contacted coordinators, to minimize network overhead
        HashSet<String> cache = new HashSet<>();
        Collection<Operation> operations = new HashSet<>();

        for (String service : state.modifiedLinks) {
            if (!state.servicesToCoordinators.containsKey(service)) {
                continue;
            }
            for (String coordinator : state.servicesToCoordinators.get(service)) {
                if ((cache.contains(coordinator)) || (coordinator.equals(getUri().toString()))) {
                    continue;
                } else {
                    cache.add(coordinator);
                }
                operations.add(createNotifyOp(UriUtils.buildUri(coordinator),
                        Operation.TX_ENSURE_COMMIT, null, (o, e) -> {
                            if (e == null) {
                                SubStage s = o.getBody(SubStage.class);
                                if (s == SubStage.COMMITTED) {
                                    state.taskSubStage = SubStage.ABORTED;
                                } // if in resolving, peer appends this to commitAfter
                            }
                        }));
            }
        }
        return operations;
    }

    /**
     * Lexicographic comparison between this selfUri and the remote
     * @return true, this wins; false, remote wins
     */
    private boolean compareTo(URI remote) {
        return getUri().compareTo(remote) < 0;
    }

    /**
     * Prepare a tiny metadata request to all services to abort
     */
    private Collection<Operation> createNotifyServicesToAbort(TransactionServiceState state) {
        Collection<Operation> operations = new HashSet<>();
        for (String service : state.readLinks) {
            operations.add(createNotifyOp(UriUtils.buildUri(service), Operation.TX_ABORT));
        }
        for (String service : state.modifiedLinks) {
            operations.add(createNotifyOp(UriUtils.buildUri(service), Operation.TX_ABORT));
        }
        return operations;
    }

    /**
     * Send a tiny metadata request to all services to commit
     */
    private Collection<Operation> createNotifyServicesToCommit(TransactionServiceState state) {
        Collection<Operation> operations = state.readLinks.stream()
                .map(service -> createNotifyOp(UriUtils.buildUri(service), Operation.TX_COMMIT))
                .collect(Collectors.toSet());
        operations.addAll(state.modifiedLinks.stream()
                .map(service -> createNotifyOp(UriUtils.buildUri(service), Operation.TX_COMMIT))
                .collect(Collectors.toList()));
        return operations;
    }

    /**
     * Prepare a simple metadata request to a service
     */
    private Operation createNotifyOp(URI service, String header) {
        // no completion handler, worst case something will fail, nothing will change on part of the service
        // and transactional operations headed there will need to contact us prior to commit
        // (handler would not change this, we will solve this using a transaction GC service)
        return Operation
                .createPatch(service)
                .addRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER, header)
                // just an empty body
                .setBody(new TransactionServiceState())
                .setReferer(getUri());
    }

    /**
     * Prepare an operation to resolve precedence with a remote coordinator
     */
    private Operation createNotifyOp(URI service, String header, Object body,
            Operation.CompletionHandler callback) {
        return Operation
                .createPatch(service)
                .addRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER, header)
                .setReferer(getUri())
                .setBody(body)
                .setCompletion(callback);
    }

}
