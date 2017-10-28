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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationProcessingChain;
import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.SimpleTransactionService.EndTransactionRequest.TransactionOutcome;

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
     * Response of successful enrollment request
     */
    public static class EnrollResponse {
        public long transactionExpirationTimeMicros;
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
    static final String PRAGMA_DIRECTIVE_DELETE_ON_TRANSACTION_END = "xenon-simpletx-delete-on-transaction-end";

    static long DEFAULT_DURATION_MICROS = TimeUnit.MINUTES.toMicros(5);

    public static class TransactionalRequestFilter implements Filter {
        private Service service;
        private long transactionExpirationTimeMicros;

        public TransactionalRequestFilter(Service service) {
            this.service = service;
        }

        @Override
        public FilterReturnCode processRequest(Operation request, OperationProcessingContext context) {
            ClearTransactionRequest clearTransactionRequest = getIfClearTransactionRequest(request);

            // TODO: generalize transaction requests protocol through headers
            if (clearTransactionRequest != null) {
                FilterReturnCode rc = handleClearTransaction(request, this.service.getState(request),
                        clearTransactionRequest, context);
                return rc;
            }

            if (validateTransactionConflictsAndMarkState(request, this.service.getState(request))) {
                request.fail(new IllegalStateException("transactional conflict"));
                return FilterReturnCode.FAILED_STOP_PROCESSING;
            }

            if (request.getTransactionId() != null) {
                context.setSuspendConsumer(o -> {
                    handleEnrollInTransaction(request, context);
                });
                return FilterReturnCode.SUSPEND_PROCESSING;
            }

            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        private ClearTransactionRequest getIfClearTransactionRequest(
                Operation request) {
            if (request.getTransactionId() == null || !request.hasBody()) {
                return null;
            }

            try {
                ClearTransactionRequest op = request
                        .getBody(ClearTransactionRequest.class);
                if (op == null || !Objects.equals(op.kind, ClearTransactionRequest.KIND)) {
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

            if (currentStateTransactionId != null && this.transactionExpirationTimeMicros != 0) {
                long now = Utils.getSystemNowMicrosUtc();
                if (this.transactionExpirationTimeMicros <= now) {
                    // the transaction 'locking' this service has expired -
                    // release lock and continue processing
                    this.service.getHost().log(Level.INFO,
                            "Transaction %s has expired, releasing service %s",
                            currentStateTransactionId, this.service.getSelfLink());
                    currentState.documentTransactionId = null;
                    currentStateTransactionId = null;
                }
            }

            if (request.getAction() == Action.GET) {
                if (requestTransactionId == null) {
                    // non-transactional read
                    if (currentStateTransactionId == null) {
                        return false;
                    } else {
                        logTransactionConflict(request, currentState);
                        return true;
                    }
                } else {
                    // transactional read
                    if (currentStateTransactionId == null) {
                        logTransactionUpdate(buildLogTransactionUpdateMsg(request, currentState));
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
            } else {
                if (requestTransactionId == null) {
                    // non-transactional write
                    if (currentStateTransactionId == null ||
                            request.hasPragmaDirective(
                                    PRAGMA_DIRECTIVE_DELETE_ON_TRANSACTION_END)) {
                        return false;
                    } else {
                        logTransactionConflict(request, currentState);
                        return true;
                    }
                } else {
                    // transactional write
                    if (currentStateTransactionId == null) {
                        logTransactionUpdate(buildLogTransactionUpdateMsg(request, currentState));
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

        private FilterReturnCode handleClearTransaction(Operation request,
                ServiceDocument currentState,
                ClearTransactionRequest clearTransactionRequest,
                OperationProcessingContext context) {
            if (currentState == null) {
                request.complete();
                return FilterReturnCode.SUCCESS_STOP_PROCESSING;
            }

            if (!request.getTransactionId().equals(currentState.documentTransactionId)) {
                if (clearTransactionRequest.transactionOutcome == TransactionOutcome.COMMIT) {
                    String warning = String.format(
                            "Request to clear transaction %s from service %s but current transaction is: %s",
                            request.getTransactionId(), this.service.getSelfLink(),
                            currentState.documentTransactionId);
                    this.service.getHost().log(Level.WARNING, warning);
                }
                request.complete();
                return FilterReturnCode.SUCCESS_STOP_PROCESSING;
            }

            if (clearTransactionRequest.transactionOutcome == TransactionOutcome.ABORT
                    && clearTransactionRequest.isUpdated) {
                // restore previous state
                URI previousStateQueryUri = UriUtils.buildDocumentQueryUri(
                        this.service.getHost(),
                        this.service.getSelfLink(),
                        false,
                        false,
                        ServiceOption.PERSISTENCE);
                previousStateQueryUri = UriUtils.appendQueryParam(previousStateQueryUri,
                        ServiceDocument.FIELD_NAME_VERSION,
                        Long.toString(clearTransactionRequest.originalVersion));
                Operation previousStateGet = Operation.createGet(previousStateQueryUri)
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                context.resumeProcessingRequest(request, FilterReturnCode.FAILED_STOP_PROCESSING, e);
                                request.fail(e);
                                return;
                            }
                            ServiceDocument previousState = o.getBody(currentState.getClass());
                            this.service.getHost().log(Level.INFO,
                                    "Aborting transaction %s on service %s, current version %d, restoring version %d",
                                    request.getTransactionId(), this.service.getSelfLink(),
                                    currentState.documentVersion,
                                    clearTransactionRequest.originalVersion);
                            previousState.documentTransactionId = null;
                            this.service.setState(request, previousState);
                            context.resumeProcessingRequest(request, FilterReturnCode.SUCCESS_STOP_PROCESSING, null);
                            request.complete();
                        });
                context.setSuspendConsumer(o -> {
                    this.service.sendRequest(previousStateGet);
                });
                return FilterReturnCode.SUSPEND_PROCESSING;
            }

            currentState.documentTransactionId = null;
            request.complete();
            return FilterReturnCode.SUCCESS_STOP_PROCESSING;
        }

        private void handleEnrollInTransaction(Operation request, OperationProcessingContext context) {
            String serviceSelfLink = this.service.getSelfLink();
            if (Action.POST == request.getAction()) {
                ServiceDocument body = request.getBody(this.service.getStateType());
                if (body.documentSelfLink == null) {
                    body.documentSelfLink = UUID.randomUUID().toString();
                    request.setBody(body);
                    serviceSelfLink = UriUtils.buildUriPath(serviceSelfLink,
                            body.documentSelfLink);
                } else {
                    if (UriUtils.isChildPath(body.documentSelfLink, serviceSelfLink)) {
                        serviceSelfLink = body.documentSelfLink;
                    } else {
                        serviceSelfLink = UriUtils.buildUriPath(serviceSelfLink,
                                body.documentSelfLink);
                    }
                }
            }

            long servicePreviousVersion = this.service.getState(request) == null ? -1
                    : this.service.getState(request).documentVersion;
            Operation enrollRequest = SimpleTransactionService.TxUtils
                    .buildEnrollRequest(this.service.getHost(),
                            request.getTransactionId(), serviceSelfLink,
                            request.getAction(), servicePreviousVersion)
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    context.resumeProcessingRequest(request, FilterReturnCode.FAILED_STOP_PROCESSING, e);
                                    request.fail(e);
                                    return;
                                }
                                EnrollResponse enrollRespone = o.getBody(EnrollResponse.class);
                                this.transactionExpirationTimeMicros = enrollRespone.transactionExpirationTimeMicros;
                                context.resumeProcessingRequest(request, FilterReturnCode.CONTINUE_PROCESSING, null);
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

        private String buildLogTransactionUpdateMsg(Operation request,
                ServiceDocument currentState) {
            return String.format(
                    "Transaction %s set on service %s: operation: %s, previous transaction: %s",
                    request.getTransactionId(),
                    this.service.getSelfLink(),
                    request.getAction(),
                    currentState.documentTransactionId);
        }

        private void logTransactionUpdate(String msg) {
            this.service.getHost().log(Level.INFO, msg);
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
                new RequestRouter.RequestBodyMatcher<>(
                        EnrollRequest.class, "kind",
                        EnrollRequest.KIND),
                this::handlePatchForEnroll, "Register service");
        myRouter.register(
                Action.PATCH,
                new RequestRouter.RequestBodyMatcher<>(
                        EndTransactionRequest.class, "kind",
                        EndTransactionRequest.KIND),
                this::handlePatchForEndTransaction, "Commit or abort transaction");
        OperationProcessingChain opProcessingChain = OperationProcessingChain.create(myRouter);
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

        if (state.documentExpirationTimeMicros == 0) {
            state.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    DEFAULT_DURATION_MICROS);
        }

        start.setBody(state).complete();
    }

    void handlePatchForEnroll(Operation patch) {
        SimpleTransactionServiceState currentState = getState(patch);
        EnrollRequest body = patch.getBody(EnrollRequest.class);

        if (TaskStage.STARTED != currentState.taskInfo.stage) {
            patch.fail(new IllegalArgumentException(String.format(
                    "Transaction stage %s is not in the right stage",
                    currentState.taskInfo.stage)));
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

        EnrollResponse enrollResponse = new EnrollResponse();
        enrollResponse.transactionExpirationTimeMicros = currentState.documentExpirationTimeMicros;
        patch.setBody(enrollResponse).complete();
    }

    void handlePatchForEndTransaction(Operation patch) {
        SimpleTransactionServiceState currentState = getState(patch);
        EndTransactionRequest body = patch.getBody(EndTransactionRequest.class);

        if (TaskStage.STARTED != currentState.taskInfo.stage) {
            patch.fail(new IllegalArgumentException(String.format(
                    "Transaction stage %s is not in the right stage",
                    currentState.taskInfo.stage)));
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

        Collection<Operation> requests = new ArrayList<>(
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
            // mark as a transaction protocol request to deal with ServiceOption.STRICT_UPDATE_CHECKING
            op.addRequestHeader(Operation.TRANSACTION_HEADER, Operation.TX_ENSURE_COMMIT);
            requests.add(op);
        }

        return requests;
    }

    private Collection<Operation> createDeleteRequests(SimpleTransactionServiceState currentState,
            EndTransactionRequest.TransactionOutcome transactionOutcome) {
        Set<String> servicesToBDeleted = transactionOutcome == TransactionOutcome.COMMIT
                ? currentState.deletedServicesLinks
                : currentState.createdServicesLinks;
        if (servicesToBDeleted.isEmpty()) {
            return null;
        }

        Collection<Operation> requests = new ArrayList<>(servicesToBDeleted.size());
        for (String serviceSelfLink : servicesToBDeleted) {
            Operation op = Operation.createDelete(UriUtils.buildUri(getHost(), serviceSelfLink))
                    .setTransactionId(null);
            op.addPragmaDirective(PRAGMA_DIRECTIVE_DELETE_ON_TRANSACTION_END);
            requests.add(op);
            currentState.enrolledServices.remove(serviceSelfLink);
        }

        return requests;
    }
}
