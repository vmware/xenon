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

import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.AlreadyClosedException;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.LuceneDocumentIndexService.DeleteQueryRuntimeContextRequest;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryRuntimeContext;

public class QueryPageService extends StatelessService {
    public static final String KIND = Utils.buildKind(QueryTask.class);

    private static final long DEFAULT_TTL_MICROS = TimeUnit.MINUTES.toMicros(1);

    private QuerySpecification spec;
    private String documentSelfLink;
    private String indexLink;
    private long documentExpirationTimeMicros;

    public QueryPageService(QuerySpecification spec, String indexLink) {
        super(QueryTask.class);
        this.spec = spec;
        this.indexLink = indexLink;
    }

    public static class LuceneQueryPage {
        public String previousPageLink;
        public ScoreDoc after;
        public Integer groupOffset;

        public LuceneQueryPage(String link, int groupOffset) {
            this.previousPageLink = link;
            this.groupOffset = groupOffset;
        }

        public LuceneQueryPage(String link, ScoreDoc after) {
            this.previousPageLink = link;
            this.after = after;
        }

        public boolean isFirstPage() {
            return this.previousPageLink == null;
        }
    }

    @Override
    public void authorizeRequest(Operation op) {
        // authorization will be applied on the result set
        op.complete();
    }

    @Override
    public void handleStart(Operation post) {
        ServiceDocument initState = post.getBody(ServiceDocument.class);

        this.documentSelfLink = initState.documentSelfLink;
        this.documentExpirationTimeMicros = initState.documentExpirationTimeMicros;

        long ttl = this.documentExpirationTimeMicros - Utils.getSystemNowMicrosUtc();
        if (ttl < 0) {
            logWarning("Task expiration is in the past, extending it");
            // task has already expired. Add some more time instead of failing
            ttl = Math.max(getHost().getMaintenanceIntervalMicros() * 2, DEFAULT_TTL_MICROS);
        }
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.setMaintenanceIntervalMicros(ttl);

        post.complete();
    }

    @Override
    public void handleGet(Operation get) {
        QuerySpecification clonedSpec = new QuerySpecification();
        // shallow copy specification
        this.spec.copyTo(clonedSpec);
        QueryTask task = QueryTask.create(clonedSpec);
        task.documentKind = KIND;
        task.documentSelfLink = this.documentSelfLink;
        task.documentExpirationTimeMicros = this.documentExpirationTimeMicros;
        task.taskInfo.stage = TaskStage.CREATED;
        task.taskInfo.isDirect = true;
        task.indexLink = this.indexLink;

        // the client can supply a URI parameter to modify the result limit. This
        // only affects the current GET operation and not the page service itself and any
        // future GETs from other clients. The generated nextPageLink, for this GET, will
        // of course be different than other operations with the original limit
        Integer limit = UriUtils.getODataLimitParamValue(get.getUri());
        if (limit != null) {
            task.querySpec.resultLimit = limit;
        }

        forwardToLucene(task, get);
    }

    @Override
    public void handleMaintenance(Operation op) {
        op.complete();
        // This service only lives as long as its parent QueryTask
        getHost().stopService(this);
    }

    private void forwardToLucene(QueryTask task, Operation get) {
        try {
            Operation localPatch = Operation.createPatch(UriUtils.buildUri(getHost(),
                    task.indexLink))
                    .setBodyNoCloning(task)
                    .setCompletion((o, e) -> {
                        if (e == null) {
                            task.results = (ServiceDocumentQueryResult) o.getBodyRaw();
                        }
                        handleQueryCompletion(task, e, get);
                    });

            sendRequest(localPatch);
        } catch (Exception e) {
            handleQueryCompletion(task, e, get);
        }
    }

    private void handleQueryCompletion(QueryTask task, Throwable e, Operation get) {
        if (e != null) {
            LuceneQueryPage ctx = (LuceneQueryPage) task.querySpec.context.nativePage;
            if (ctx.isFirstPage() && (e instanceof AlreadyClosedException)
                    && !getHost().isStopping()) {
                // The lucene index service periodically grooms index writers and index searchers.
                // When the system is under load, the grooming will occur more often, potentially
                // closing a writer that is indirectly used by a paginated query task.
                // Paginated queries cache the index searcher to use, when the very first page is
                // is retrieved. If we detect that failure occurred doing a query for the first page,
                // we re-open the searcher, still providing a consistent snapshot of the index across
                // all pages. If any page other than the first encounters failure however, we are forced
                // to fail the query task itself, and client has to retry.
                logWarning("Retrying query because index context is out of date");
                task.querySpec.context.nativeSearcher = null;
                forwardToLucene(task, get);
                return;
            }

            // fail the paginated query, client has to re-create the query task
            QueryTask t = new QueryTask();
            t.taskInfo.stage = TaskStage.FAILED;
            t.taskInfo.failure = Utils.toServiceErrorResponse(e);
            get.setBody(t).fail(e);
            return;
        }

        boolean singleUse = task.querySpec.options.contains(QueryOption.SINGLE_USE);
        if (singleUse) {
            getHost().stopService(this);
        }

        // null the native query context in the cloned spec, so it does not serialize on the wire,
        // and complete the request.
        QueryRuntimeContext context = task.querySpec.context;
        task.querySpec.context = null;
        task.taskInfo.stage = TaskStage.FINISHED;
        QueryTaskUtils.expandLinks(getHost(), task, get);

        if (singleUse && task.results.nextPageLink == null) {
            DeleteQueryRuntimeContextRequest request = new DeleteQueryRuntimeContextRequest();
            request.documentKind = DeleteQueryRuntimeContextRequest.KIND;
            request.context = context;

            Operation patch = Operation.createPatch(this, task.indexLink)
                    .setBodyNoCloning(request)
                    .setCompletion((op, ex) -> {
                        if (ex != null) {
                            logWarning("Failed to delete runtime context: %s", Utils.toString(ex));
                        }
                    });

            sendRequest(patch);
        }
    }
}
