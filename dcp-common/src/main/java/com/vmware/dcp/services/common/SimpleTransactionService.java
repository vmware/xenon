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

package com.vmware.dcp.services.common;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.logging.Level;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.OperationJoin;
import com.vmware.dcp.common.OperationProcessingChain;
import com.vmware.dcp.common.RequestRouter;
import com.vmware.dcp.common.Service;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceHost;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.TaskState;
import com.vmware.dcp.common.TaskState.TaskStage;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.QueryTask.Query;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.dcp.services.common.SimpleTransactionService.EndTransactionRequest.TransactionOutcome;

public class SimpleTransactionService extends StatefulService {
    public static class EnrollmentInfo {
        /**
         * True if the service state has been updated during this transaction
         */
        public boolean isUpdated;

        /**
         * The version of the service prior to enrolling in this transaction
         */
        public long originalVersion;
    }

    public static class SimpleTransactionServiceState extends ServiceDocument {

        public TaskState taskInfo;

        /**
         * Services that have enrolled in this transaction
         */
        public Map<String, EnrollmentInfo> enrolledServices;

        /**
         * Services that have been created in the context of this transaction
         */
        public Set<String> createdServicesLinks;

        /**
         * Services that have been deleted in the context of this transaction
         */
        public Set<String> deletedServicesLinks;
    }

    /**
     * Request for enrolling a service in this transaction
     */
    public static class EnrollRequest {
        public static final String KIND = Utils.buildKind(EnrollRequest.class);
        public String kind = KIND;
        public String serviceSelfLink;
        public Action action;
        public long previousVersion;
    }

    /**
     * Request for committing or aborting this transaction
     */
    public static class EndTransactionRequest {
        public static final String KIND = Utils.buildKind(EndTransactionRequest.class);

        public enum TransactionOutcome {
            COMMIT, ABORT
        }

        public String kind = KIND;
        public TransactionOutcome transactionOutcome;
    }

    public static class TxUtils {
        public static Operation buildEnrollRequest(ServiceHost host, String transactionId,
                String serviceSelfLink, Action action, long previousVersion) {
            EnrollRequest body = new EnrollRequest();
            body.serviceSelfLink = serviceSelfLink;
            body.action = action;
            body.previousVersion = previousVersion;
            return Operation
                    .createPatch(buildTransactionUri(host, transactionId))
                    .setBody(body);
        }

        public static Operation buildCommitRequest(ServiceHost host, String transactionId) {
            EndTransactionRequest body = new EndTransactionRequest();
            body.transactionOutcome = EndTransactionRequest.TransactionOutcome.COMMIT;
            return Operation
                    .createPatch(buildTransactionUri(host, transactionId))
                    .setBody(body);
        }

        public static Operation buildAbortRequest(ServiceHost host, String transactionId) {
            EndTransactionRequest body = new EndTransactionRequest();
            body.transactionOutcome = EndTransactionRequest.TransactionOutcome.ABORT;
            return Operation
                    .createPatch(buildTransactionUri(host, transactionId))
                    .setBody(body);
        }
    }

    /**
     * A request sent to an enrolled service at the end of this transaction to clear
     * the service' transaction id
     */
    public static class ClearTransactionRequest {
        public static final String KIND = Utils
                .buildKind(ClearTransactionRequest.class);
        public String kind;
        public TransactionOutcome transactionOutcome;
        public boolean isUpdated;
        public long originalVersion;
    }


    /**
     * Indicates a request to delete an enrolled service at the end of the transaction either
     * because the service has been created during the transaction and the transaction has
     * aborted, or because the service has been deleted during the transaction and the
     * transaction has been committed.
     */
    static final String PRAGMA_DIRECTIVE_DELETE_ON_TRANSACTION_END = "dcp-simpletx-delete-on-transaction-end";

    public static class TransactionalRequestFilter implements Predicate<Operation> {
        private Service service;

        public TransactionalRequestFilter(Service service) {
            this.service = service;
        }

        @Override
        public boolean test(Operation request) {
            ClearTransactionRequest clearTransactionRequest = getIfClearTransactionRequest(request);

            // TODO: generalize transaction requests protocol through headers
            if (clearTransactionRequest != null) {
                handleClearTransaction(request, this.service.getState(request),
                        clearTransactionRequest);
                return false;
            }

            if (validateTransactionConflictsAndMarkState(request, this.service.getState(request))) {
                request.fail(new IllegalStateException("transactional conflict"));
                return false;
            }

            if (request.getTransactionId() != null) {
                handleEnrollInTransaction(request);
                // we're not dropping the request, we will resume it on registration completion
                return false;
            }

            return true;
        }

        private ClearTransactionRequest getIfClearTransactionRequest(
                Operation request) {
            if (request.getTransactionId() == null || !request.hasBody()) {
                return null;
            }

            try {
                ClearTransactionRequest op = request
                        .getBody(ClearTransactionRequest.class);
                if (op == null || op.kind != ClearTransactionRequest.KIND) {
                    return null;
                }

                return op;
            } catch (Exception ex) {
                return null;
            }
        }

        private boolean validateTransactionConflictsAndMarkState(Operation request,
                ServiceDocument currentState) {
            if (currentState == null) {
                return false;
            }

            String requestTransactionId = request.getTransactionId();
            String currentStateTransactionId = currentState.documentTransactionId;

            // TODO: support 'multi-versioned' states: allow reading a previous state revision in case requestTransactionId is null or < currentStateTransactionId

            if (request.getAction() == Action.GET) {
                if (requestTransactionId == null) { // non-transactional read
                    if (currentStateTransactionId == null) {
                        return false;
                    } else {
                        // TODO: in the future we might want to allow reading the state before the pending transaction
                        logTransactionConflict(request, currentState);
                        return true;
                    }
                } else { // transactional read
                    if (currentStateTransactionId == null) {
                        currentState.documentTransactionId = requestTransactionId;
                        return false;
                    } else {
                        if (requestTransactionId.equals(currentStateTransactionId)) {
                            return false;
                        } else {
                            // TODO: in the future we might want to allow reading a previous state if requestTransactionId < currentStateTransactionId
                            logTransactionConflict(request, currentState);
                            return true;
                        }
                    }
                }
            } else {
                if (requestTransactionId == null) { // non-transactional write
                    if (currentStateTransactionId == null || request.hasPragmaDirective(PRAGMA_DIRECTIVE_DELETE_ON_TRANSACTION_END)) {
                        return false;
                    } else {
                        logTransactionConflict(request, currentState);
                        return true;
                    }
                } else { // transactional write
                    if (currentStateTransactionId == null) {
                        currentState.documentTransactionId = requestTransactionId;
                        return false;
                    } else {
                        if (requestTransactionId.equals(currentStateTransactionId)) {
                            return false;
                        } else {
                            logTransactionConflict(request, currentState);
                            return true;
                        }
                    }
                }
            }
        }

        private void handleClearTransaction(Operation request,
                ServiceDocument currentState,
                ClearTransactionRequest clearTransactionRequest) {
            if (currentState == null) {
                request.complete();
                return;
            }

            if (!request.getTransactionId().equals(currentState.documentTransactionId)) {
                request.fail(new IllegalStateException(String.format(
                        "Request to clear transaction %s from service %s but current transaction is: %s",
                        request.getTransactionId(), this.service.getSelfLink(),
                        currentState.documentTransactionId)));
                return;
            }

            if (clearTransactionRequest.transactionOutcome == TransactionOutcome.ABORT && clearTransactionRequest.isUpdated) {
                // restore previous state
                Query.Builder queryBuilder = Query.Builder.create();
                queryBuilder.addKindFieldClause(currentState.getClass());
                queryBuilder.addRangeClause(ServiceDocument.FIELD_NAME_VERSION, QueryTask.NumericRange.createLongRange(
                        clearTransactionRequest.originalVersion, clearTransactionRequest.originalVersion, true, true));
                QueryTask.Builder queryTaskBuilder = QueryTask.Builder.createDirectTask().setQuery(queryBuilder.build());
                queryTaskBuilder.setOptions(EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.INCLUDE_ALL_VERSIONS));
                QueryTask task = queryTaskBuilder.build();

                Operation post = Operation.createPost(UriUtils.buildUri(this.service.getHost(),
                        ServiceUriPaths.CORE_QUERY_TASKS)).setBody(task).setCompletion((o, e) -> {
                            if (e != null) {
                                request.fail(e);
                                return;
                            }
                            QueryTask rsp = o.getBody(QueryTask.class);
                            if (rsp.results.documentLinks.isEmpty()) {
                                request.fail(new IllegalStateException(String.format(
                                        "Could not find original state of service %s version %d to restore at the end of abort",
                                        this.service.getSelfLink(), clearTransactionRequest.originalVersion)));
                                return;
                            }
                            Object doc =  rsp.results.documents.get(rsp.results.documentLinks.get(0));
                            ServiceDocument state = Utils.fromJson(doc, currentState.getClass());
                            state.documentTransactionId = null;
                            this.service.setState(request, state);
                            request.complete();
                        });
                this.service.sendRequest(post);
            } else {
                currentState.documentTransactionId = null;
                request.complete();
            }
        }

        private void handleEnrollInTransaction(Operation request) {
            String serviceSelfLink = this.service.getSelfLink();
            if (Action.POST == request.getAction()) {
                ServiceDocument body = request.getBody(ServiceDocument.class);
                if (body.documentSelfLink == null) {
                    body.documentSelfLink = UUID.randomUUID().toString();
                }
                serviceSelfLink = UriUtils.buildUriPath(serviceSelfLink, body.documentSelfLink);
            }

            long servicePreviousVersion = this.service.getState(request) == null ? -1 :
                    this.service.getState(request).documentVersion;
            Operation enrollRequest = SimpleTransactionService.TxUtils
                    .buildEnrollRequest(this.service.getHost(),
                            request.getTransactionId(), serviceSelfLink,
                            request.getAction(), servicePreviousVersion)
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    request.fail(e);
                                    return;
                                }
                                this.service
                                        .getHost()
                                        .run(() -> {
                                            this.service
                                                    .handleRequest(
                                                            request,
                                                            OperationProcessingStage.EXECUTING_SERVICE_HANDLER);

                                        });
                            });
            this.service.sendRequest(enrollRequest);
        }

        private void logTransactionConflict(Operation request, ServiceDocument currentState) {
            this.service
                    .getHost()
                    .log(Level.INFO,
                            "Transaction %s conflicts on service %s: operation: %s, current state transaction: %s",
                            request.getTransactionId(), this.service.getSelfLink(),
                            request.getAction(),
                            currentState.documentTransactionId);
        }

    }

    public static URI buildTransactionUri(ServiceHost host, String selfLink) {
        return UriUtils.extendUri(
                UriUtils.buildUri(host, SimpleTransactionFactoryService.SELF_LINK), selfLink);
    }

    public SimpleTransactionService() {
        super(SimpleTransactionServiceState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public OperationProcessingChain getOperationProcessingChain() {
        if (super.getOperationProcessingChain() != null) {
            return super.getOperationProcessingChain();
        }

        RequestRouter myRouter = new RequestRouter();
        myRouter.register(
                Action.PATCH,
                new RequestRouter.RequestBodyMatcher<EnrollRequest>(
                        EnrollRequest.class, "kind",
                        EnrollRequest.KIND),
                this::handlePatchForEnroll, "Register service");
        myRouter.register(
                Action.PATCH,
                new RequestRouter.RequestBodyMatcher<EndTransactionRequest>(
                        EndTransactionRequest.class, "kind",
                        EndTransactionRequest.KIND),
                this::handlePatchForEndTransaction, "Commit or abort transaction");
        OperationProcessingChain opProcessingChain = new OperationProcessingChain();
        opProcessingChain.add(myRouter);
        setOperationProcessingChain(opProcessingChain);
        return opProcessingChain;
    }

    @Override
    public void handleStart(Operation start) {
        SimpleTransactionServiceState state = start.hasBody() ? start
                .getBody(SimpleTransactionServiceState.class) : new SimpleTransactionServiceState();

        if (state == null) {
            start.fail(new IllegalArgumentException("faild to parse provided state"));
            return;
        }
        if (state.taskInfo == null) {
            state.taskInfo = new TaskState();
            state.taskInfo.stage = TaskStage.STARTED;
        }
        if (state.enrolledServices == null) {
            state.enrolledServices = new HashMap<>();
        }
        if (state.createdServicesLinks == null) {
            state.createdServicesLinks = new HashSet<>();
        }
        if (state.deletedServicesLinks == null) {
            state.deletedServicesLinks = new HashSet<>();
        }

        start.setBody(state).complete();
    }

    void handlePatchForEnroll(Operation patch) {
        SimpleTransactionServiceState currentState = getState(patch);
        EnrollRequest body = patch.getBody(EnrollRequest.class);

        if (TaskStage.STARTED != currentState.taskInfo.stage) {
            patch.fail(new IllegalArgumentException(String.format(
                    "Transaction stage %s is not in the right stage", currentState.taskInfo.stage)));
            return;
        }

        if (body.serviceSelfLink == null) {
            patch.fail(new IllegalArgumentException("Cannot register null service selfLink"));
            return;
        }

        EnrollmentInfo enrollmentInfo = currentState.enrolledServices.get(body.serviceSelfLink);
        if (enrollmentInfo == null) {
            enrollmentInfo = new EnrollmentInfo();
            enrollmentInfo.isUpdated = body.action != Action.GET;
            enrollmentInfo.originalVersion = body.previousVersion;
            currentState.enrolledServices.put(body.serviceSelfLink, enrollmentInfo);
        } else {
            enrollmentInfo.isUpdated = enrollmentInfo.isUpdated || body.action != Action.GET;
        }
        if (body.action == Action.POST) {
            currentState.createdServicesLinks.add(body.serviceSelfLink);
        }
        if (body.action == Action.DELETE) {
            currentState.deletedServicesLinks.add(body.serviceSelfLink);
        }

        patch.complete();
    }

    void handlePatchForEndTransaction(Operation patch) {
        SimpleTransactionServiceState currentState = getState(patch);
        EndTransactionRequest body = patch.getBody(EndTransactionRequest.class);

        if (TaskStage.STARTED != currentState.taskInfo.stage) {
            patch.fail(new IllegalArgumentException(String.format(
                    "Transaction stage %s is not in the right stage", currentState.taskInfo.stage)));
            return;
        }

        switch (body.transactionOutcome) {
        case COMMIT:
            currentState.taskInfo.stage = TaskStage.FINISHED;
            break;
        case ABORT:
            currentState.taskInfo.stage = TaskStage.CANCELLED;
            break;
        default:
            patch.fail(new IllegalArgumentException(String.format(
                    "Unrecognized transaction outcome: %s", body.transactionOutcome)));
            return;
        }

        String transactionId = this.getSelfLink().substring(
                this.getSelfLink().lastIndexOf(UriUtils.URI_PATH_CHAR) + 1);
        Collection<Operation> deleteRequests = createDeleteRequests(currentState,
                body.transactionOutcome);
        Collection<Operation> clearTransactionRequests = createClearTransactionRequests(
                currentState, transactionId, body.transactionOutcome);
        if (deleteRequests != null && !deleteRequests.isEmpty()) {
            deleteServicesAndClearTransactions(patch, transactionId,
                    deleteRequests, clearTransactionRequests);
        } else if (clearTransactionRequests != null && !clearTransactionRequests.isEmpty()) {
            clearTransactions(patch, transactionId, clearTransactionRequests);
        } else {
            patch.complete();
        }
    }

    private void deleteServicesAndClearTransactions(Operation patch,
            String transactionId,
            Collection<Operation> deleteRequests, Collection<Operation> clearTransactionRequests) {
        OperationJoin.create(deleteRequests).setCompletion((ops, exs) -> {
            if (exs != null) {
                patch.fail(new IllegalStateException(String.format(
                        "Transaction %s failed to delete some services",
                        transactionId)));
                return;
            }

            if (clearTransactionRequests != null && !clearTransactionRequests.isEmpty()) {
                clearTransactions(patch, transactionId, clearTransactionRequests);
            } else {
                patch.complete();
            }
        }).sendWith(this);
    }

    private void clearTransactions(Operation patch, String transactionId,
            Collection<Operation> clearTransactionRequests) {
        OperationJoin.create(clearTransactionRequests).setCompletion((ops, exs) -> {
            if (exs != null) {
                patch.fail(new IllegalStateException(String.format(
                        "Transaction %s failed to clear from some services",
                        transactionId)));
                return;
            }

            patch.complete();
        }).sendWith(this);
    }

    private Collection<Operation> createClearTransactionRequests(
            SimpleTransactionServiceState currentState, String transactionId,
            EndTransactionRequest.TransactionOutcome transactionOutcome) {
        if (currentState.enrolledServices.isEmpty()) {
            return null;
        }

        Collection<Operation> requests = new ArrayList<Operation>(
                currentState.enrolledServices.size());
        for (String serviceSelfLink : currentState.enrolledServices.keySet()) {
            EnrollmentInfo enrollmentInfo = currentState.enrolledServices.get(serviceSelfLink);
            ClearTransactionRequest body = new ClearTransactionRequest();
            body.kind = ClearTransactionRequest.KIND;
            body.transactionOutcome = transactionOutcome;
            body.isUpdated = enrollmentInfo.isUpdated;
            body.originalVersion = enrollmentInfo.originalVersion;
            Operation op = Operation.createPatch(UriUtils.buildUri(getHost(), serviceSelfLink))
                    .setTransactionId(transactionId).setBody(body);
            requests.add(op);
        }

        return requests;
    }

    private Collection<Operation> createDeleteRequests(SimpleTransactionServiceState currentState,
            EndTransactionRequest.TransactionOutcome transactionOutcome) {
        Set<String> servicesToBDeleted = transactionOutcome == TransactionOutcome.COMMIT ? currentState.deletedServicesLinks
                : currentState.createdServicesLinks;
        if (servicesToBDeleted.isEmpty()) {
            return null;
        }

        Collection<Operation> requests = new ArrayList<Operation>(servicesToBDeleted.size());
        for (String serviceSelfLink : servicesToBDeleted) {
            Operation op = Operation.createDelete(UriUtils.buildUri(getHost(), serviceSelfLink));
            op.addPragmaDirective(PRAGMA_DIRECTIVE_DELETE_ON_TRANSACTION_END);
            requests.add(op);
            currentState.enrolledServices.remove(serviceSelfLink);
        }

        return requests;
    }
}
