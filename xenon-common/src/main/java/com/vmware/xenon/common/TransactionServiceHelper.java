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

import java.net.URI;
import java.util.EnumSet;
import java.util.List;

import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Stateless helpers for transactions
 */
public class TransactionServiceHelper {
    interface Handler {
        void handler(Operation op);
    }

    interface FailRequest {
        void failRequest(Operation op, Throwable e, boolean shouldRetry);
    }

    /**
     * Handles a GET request on a service that has pending transactions.
     *
     * If the request is transactional, we search for the latest version of the
     * service tagged with the transaction and return it if exists; if it
     * doesn't - we search for the latest non-shadowed version and return
     * it if exists (otherwise we fail the request).
     *
     * If the request is non-transactional we search for the latest
     * non-shadowed version and return it if exists, otherwise we fail the
     * request.
     */
    static void handleGetWithinTransaction(StatefulService s, Operation get,
            Handler h, FailRequest fr) {
        if (get.isWithinTransaction()) {
            Operation inTransactionQueryOp = buildLatestInTransactionQueryTaskOp(s,
                    get.getTransactionId()).setCompletion((o, e) -> {
                        if (e != null) {
                            get.fail(e);
                            return;
                        }

                        QueryTask response = o.getBody(QueryTask.class);
                        if (response.results.documentLinks.isEmpty()) {
                            Operation nonTransactionQueryOp = buildLatestNonTransactionQueryTaskOp(
                                    s).setCompletion((o2, e2) -> {
                                        if (e2 != null) {
                                            get.fail(e);
                                            return;
                                        }
                                        QueryTask nonTransactionResponse = o2
                                                .getBody(QueryTask.class);
                                        returnLatestOrFail(nonTransactionResponse, get, fr);
                                    });
                            s.sendRequest(nonTransactionQueryOp);
                        } else {
                            returnLatestOrFail(response, get, fr);
                        }
                    });
            s.sendRequest(inTransactionQueryOp);
        } else {
            Operation nonTransactionQueryOp = buildLatestNonTransactionQueryTaskOp(s)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            get.fail(e);
                            return;
                        }

                        QueryTask response = o.getBody(QueryTask.class);
                        returnLatestOrFail(response, get, fr);
                    });
            s.sendRequest(nonTransactionQueryOp);
        }
    }

    private static void returnLatestOrFail(QueryTask response, Operation get, FailRequest fr) {
        if (response.results.documentLinks.isEmpty()) {
            get.setStatusCode(Operation.STATUS_CODE_NOT_FOUND);
            fr.failRequest(get, new IllegalStateException("Latest state not found"), false);
            return;
        }

        String latest = response.results.documentLinks.get(0);
        Object obj = response.results.documents.get(latest);
        get.setBodyNoCloning(obj).complete();
    }

    private static Operation buildLatestInTransactionQueryTaskOp(StatefulService s, String txid) {
        Query.Builder queryBuilder = Query.Builder.create();
        queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, s.getSelfLink());
        queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, txid);

        QueryTask.Builder queryTaskBuilder = QueryTask.Builder.createDirectTask()
                .setQuery(queryBuilder.build());
        queryTaskBuilder.addOption(QueryOption.EXPAND_CONTENT);
        queryTaskBuilder.addOption(QueryOption.INCLUDE_ALL_VERSIONS);
        queryTaskBuilder.orderDescending(ServiceDocument.FIELD_NAME_VERSION, TypeName.LONG);
        QueryTask task = queryTaskBuilder.build();

        return Operation
                .createPost(UriUtils.buildUri(s.getHost(), ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(task);
    }

    private static Operation buildLatestNonTransactionQueryTaskOp(StatefulService s) {
        Query.Builder queryBuilder = Query.Builder.create();
        queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, s.getSelfLink());
        queryBuilder.addFieldClause(ServiceDocument.FIELD_NAME_TRANSACTION_ID, "*",
                MatchType.WILDCARD, Occurance.MUST_NOT_OCCUR);

        QueryTask.Builder queryTaskBuilder = QueryTask.Builder.createDirectTask()
                .setQuery(queryBuilder.build());
        queryTaskBuilder.addOption(QueryOption.EXPAND_CONTENT);
        queryTaskBuilder.addOption(QueryOption.INCLUDE_ALL_VERSIONS);
        queryTaskBuilder.orderDescending(ServiceDocument.FIELD_NAME_VERSION, TypeName.LONG);
        QueryTask task = queryTaskBuilder.build();

        return Operation
                .createPost(UriUtils.buildUri(s.getHost(), ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(task);
    }

    /**
     * Notify the transaction coordinator
     */
    static void notifyTransactionCoordinator(StatefulService s, Operation op, Throwable e) {
        Operation.TransactionContext operationsLogRecord = new Operation.TransactionContext();
        operationsLogRecord.action = op.getAction();
        operationsLogRecord.coordinatorLinks = s.getPendingTransactions();
        operationsLogRecord.isSuccessful = e == null;

        URI txCoordinator = UriUtils.buildTransactionUri(s.getHost(), op.getTransactionId());

        s.addPendingTransaction(txCoordinator.getPath());
        // set the transaction id to null as this is a PUT on the transaction service
        // and we don't want a cycle of notifications - this method will be called
        // again when PUT operation completion handler runs resulting in a loop
        s.sendRequest(Operation.createPut(txCoordinator).setBody(operationsLogRecord).setTransactionId(null));
    }

    /**
     * Notify the transaction coordinator of a new service
     */
    static void notifyTransactionCoordinatorOfNewService(FactoryService factoryService,
            Service childService, Operation op) {
        // some of the basic properties of the child service being created are not
        // yet set at the point we're intercepting the POST, so we need to set them here
        childService.setHost(factoryService.getHost());
        URI childServiceUri = op.getUri().normalize();
        String childServicePath = UriUtils.normalizeUriPath(childServiceUri.getPath()).intern();
        childService.setSelfLink(childServicePath);

        // TODO: remove cast by changing childService type at the origin (FactoryService)
        notifyTransactionCoordinator((StatefulService) childService, op, null);
    }

    /**
     * Check whether it's a transactional control operation (i.e., expose shadowed state, abort
     * etc.), and take appropriate action
     */
    static boolean handleOperationInTransaction(StatefulService s,
            Class<? extends ServiceDocument> st,
            Operation request) {
        if (request.getRequestHeader(Operation.TRANSACTION_HEADER) == null) {
            return false;
        }

        if (request.getRequestHeader(Operation.TRANSACTION_HEADER).equals(Operation.TX_TRY_COMMIT)
                ||
                request.getRequestHeader(Operation.TRANSACTION_HEADER)
                        .equals(Operation.TX_ENSURE_COMMIT)) {
            // this request is targeting a transaction service - let it 'fall through'
            return false;
        }

        if (request.getRequestHeader(Operation.TRANSACTION_HEADER).equals(
                Operation.TX_COMMIT)) {
            // commit should expose latest state, i.e., remove shadow and bump the version
            // and remove transaction from pending
            s.removePendingTransaction(request.getReferer().getPath());
            s.getHost().clearTransactionalCachedServiceState(s,
                    UriUtils.getLastPathSegment(request.getReferer().getPath()));

            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            QueryTask.Query txnIdClause = new QueryTask.Query().setTermPropertyName(
                    ServiceDocument.FIELD_NAME_TRANSACTION_ID)
                    .setTermMatchValue(UriUtils.getLastPathSegment(request.getReferer()));
            txnIdClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
            q.query.addBooleanClause(txnIdClause);
            QueryTask.Query selfLinkClause = new QueryTask.Query().setTermPropertyName(
                    ServiceDocument.FIELD_NAME_SELF_LINK)
                    .setTermMatchValue(s.getSelfLink());
            selfLinkClause.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
            q.query.addBooleanClause(selfLinkClause);
            q.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT,
                    QueryTask.QuerySpecification.QueryOption.INCLUDE_ALL_VERSIONS);
            QueryTask task = QueryTask.create(q).setDirect(true);
            URI uri = UriUtils.buildUri(s.getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
            Operation startPost = Operation
                    .createPost(uri)
                    .setBody(task)
                    .setCompletion((o, f) -> unshadowQueryCompletion(s, st, o, f, request));
            s.sendRequest(startPost);

        } else if (request.getRequestHeader(Operation.TRANSACTION_HEADER).equals(
                Operation.TX_ABORT)) {
            // abort should just remove transaction from pending
            s.removePendingTransaction(request.getReferer().getPath());
            s.getHost().clearTransactionalCachedServiceState(s,
                    UriUtils.getLastPathSegment(request.getReferer().getPath()));
            request.complete();
        } else {
            request.fail(new IllegalArgumentException(
                    "Transaction control message, but none of {commit, abort}"));
        }
        return true;
    }

    static void unshadowQueryCompletion(StatefulService s,
            Class<? extends ServiceDocument> st,
            Operation o, Throwable f,
            Operation original) {
        if (f != null) {
            s.logInfo(f.toString());
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
        ServiceDocument sd = Utils.fromJson((String) obj, st);
        sd.documentTransactionId = null;
        // ..and stick back in.
        s.setState(original, sd);
        original.complete();
    }

}
