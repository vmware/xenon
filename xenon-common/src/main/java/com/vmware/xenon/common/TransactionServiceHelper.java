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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TransactionService.ResolutionKind;
import com.vmware.xenon.services.common.TransactionService.ResolutionRequest;

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
     * Transaction-enabled path
     */
    static void handleGetWithinTransaction(StatefulService s, Operation get,
                                           Handler h, FailRequest fr) {

        QueryTask.Query selfLinkClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                .setTermMatchValue(s.getSelfLink());

        QueryTask.Query txClause = new QueryTask.Query();

        if (get.isWithinTransaction()) {
            // latest that has txid -- TODO: incorporate caching (DCP-1160)
            txClause.setTermPropertyName(ServiceDocument.FIELD_NAME_TRANSACTION_ID);
            txClause.setTermMatchValue(get.getTransactionId());
        } else {
            // latest that does not have txid -- TODO: incorporate caching (DCP-1160)
            txClause.setTermPropertyName(ServiceDocument.FIELD_NAME_TRANSACTION_ID);
            txClause.setTermMatchValue("*");
            txClause.setTermMatchType(MatchType.WILDCARD);
            txClause.occurance = Occurance.MUST_NOT_OCCUR;
        }
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT,
                QueryTask.QuerySpecification.QueryOption.INCLUDE_ALL_VERSIONS);
        q.query.addBooleanClause(selfLinkClause);
        q.query.addBooleanClause(txClause);

        QueryTask task = QueryTask.create(q).setDirect(true);
        URI uri = UriUtils.buildUri(s.getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
        Operation startPost = Operation
                .createPost(uri)
                .setBody(task)
                .setCompletion((o, f) -> handleTransactionQueryCompletion(s, o, f, get, h, fr));
        s.sendRequest(startPost);
    }

    /**
     * Process the latest version recovered
     */
    static void handleTransactionQueryCompletion(StatefulService s, Operation o, Throwable f,
                                                 Operation original, Handler h, FailRequest fr) {
        if (f != null) {
            s.logInfo(f.toString());
            original.fail(f);
            return;
        }

        QueryTask response = o.getBody(QueryTask.class);

        // If we are within a transaction, empty state means there are no shadowed versions, so
        // return previous visible. If we are not, however, this means a 404 -- there is no prior
        // visible state!
        if (response.results.documentLinks.isEmpty()) {
            if (original.isWithinTransaction()) {
                // TODO: This has the possibility of returning a version that has a different
                // transaction, if there are more than one pending -- depends on DCP 1160.
                h.handler(original);
            } else {
                original.setStatusCode(Operation.STATUS_CODE_NOT_FOUND);
                fr.failRequest(original, new IllegalStateException("Latest state not found"), false);
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
     * Notify the transaction coordinator
     */
    static void notifyTransactionCoordinator(Service s, Set<String> txCoordinatorLinks,
                                             Operation op, Throwable e) {
        Operation.TransactionContext operationsLogRecord = new Operation.TransactionContext();
        operationsLogRecord.action = op.getAction();
        operationsLogRecord.coordinatorLinks = txCoordinatorLinks;
        operationsLogRecord.isSuccessful = e == null;

        URI txCoordinator = UriUtils.buildTransactionUri(s.getHost(), op.getTransactionId());

        txCoordinatorLinks.add(txCoordinator.toString());

        s.sendRequest(Operation.createPut(txCoordinator).setBody(operationsLogRecord));
    }

    /**
     * Notify the transaction coordinator of a new service
     */
    static void notifyTransactionCoordinatorOfNewService(FactoryService factoryService, Service childService, Operation op) {
        // some of the basic properties of the child service being created are not
        // yet set at the point we're intercepting the POST, so we need to set them here
        childService.setHost(factoryService.getHost());
        URI childServiceUri = op.getUri().normalize();
        String childServicePath = UriUtils.normalizeUriPath(childServiceUri.getPath()).intern();
        childService.setSelfLink(childServicePath);

        notifyTransactionCoordinator(childService, new HashSet<>(), op, null);
    }

    static void abortTransactions(StatefulService service, Set<String> coordinators) {
        if (coordinators == null || coordinators.isEmpty()) {
            return;
        }
        ResolutionRequest resolution = new ResolutionRequest();
        resolution.kind = ResolutionKind.ABORT;
        for (String coordinator : coordinators) {
            service.sendRequest(Operation.createPatch(UriUtils.buildUri(coordinator))
                    .setBodyNoCloning(resolution));
        }
    }

    /**
     * Check whether it's a transactional control operation (i.e., expose shadowed state, abort
     * etc.), and take appropriate action
     */
    static boolean handleOperationInTransaction(StatefulService s,
                                                Class<? extends ServiceDocument> st,
                                                Set<String> txCoordinatorLinks, Operation request) {
        if (request.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER) == null) {
            return false;
        }

        if (request.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER).equals(
                Operation.TX_COMMIT)) {
            // commit should expose latest state, i.e., remove shadow and bump the version
            // and remove transaction from pending
            if (txCoordinatorLinks != null) {
                txCoordinatorLinks.remove(request.getReferer().toString());
            }

            QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
            q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_TRANSACTION_ID);
            q.query.setTermMatchValue(UriUtils.getLastPathSegment(request.getReferer()));
            q.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT,
                    QueryTask.QuerySpecification.QueryOption.INCLUDE_ALL_VERSIONS);
            QueryTask task = QueryTask.create(q).setDirect(true);
            URI uri = UriUtils.buildUri(s.getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
            Operation startPost = Operation
                    .createPost(uri)
                    .setBody(task)
                    .setCompletion((o, f) -> unshadowQueryCompletion(s, st, o, f, request));
            s.sendRequest(startPost);

        } else if (request.getRequestHeader(Operation.VMWARE_DCP_TRANSACTION_HEADER).equals(
                Operation.TX_ABORT)) {
            // abort should just remove transaction from pending
            if (txCoordinatorLinks != null) {
                txCoordinatorLinks.remove(request.getReferer().toString());
            }
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
