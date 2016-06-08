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
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;

public class QueryPageService extends StatelessService {
    public static final String KIND = Utils.buildKind(QueryTask.class);

    private QuerySpecification spec;
    private String documentSelfLink;
    private String indexLink;

    public QueryPageService(QuerySpecification spec, String indexLink) {
        super(QueryTask.class);
        this.spec = spec;
        this.indexLink = indexLink;
    }

    public static class LuceneQueryPage {
        public String previousPageLink;
        public ScoreDoc after;

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

        long interval = initState.documentExpirationTimeMicros - Utils.getNowMicrosUtc();
        if (interval < 0) {
            logWarning("Task expiration is in the past, extending it");
            // task has already expired. Add some more time instead of failing
            interval = TimeUnit.SECONDS.toMicros(getHost().getMaintenanceIntervalMicros() * 2);
        }
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.setMaintenanceIntervalMicros(interval);

        post.complete();
    }

    @Override
    public void handleGet(Operation get) {
        QueryTask task = QueryTask.create(this.spec);
        task.documentKind = KIND;
        task.documentSelfLink = this.documentSelfLink;
        task.documentExpirationTimeMicros =
                getMaintenanceIntervalMicros() + Utils.getNowMicrosUtc();
        task.taskInfo.stage = TaskStage.CREATED;
        task.taskInfo.isDirect = true;
        task.indexLink = this.indexLink;

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
        } catch (Throwable e) {
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

        task.taskInfo.stage = TaskStage.FINISHED;
        QueryTaskUtils.expandLinks(getHost(), task, get);
    }
}
