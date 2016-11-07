/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.gson.JsonObject;

import com.vmware.xenon.services.common.QueryTask;

/**
 * A thin wrapper on top of {@link QueryTask} that converts query results to service documents.
 */
public final class QueryResultsProcessor {
    private final QueryTask task;
    private final ServiceDocumentQueryResult results;

    private QueryResultsProcessor(QueryTask task, ServiceDocumentQueryResult results) {
        this.task = task;
        this.results = results;
    }

    public static QueryResultsProcessor create(QueryTask task) {
        return new QueryResultsProcessor(task, task.results);
    }

    public static QueryResultsProcessor create(ServiceDocumentQueryResult results) {
        return new QueryResultsProcessor(null, results);
    }

    /**
     * Create a QueryResultsProcessor by trying to parse the Operation body as either QueryTask or
     * ServiceDocumentQueryResult or ODataFactoryQueryResult.
     * @param op
     * @return
     * @throws IllegalArgumentException if the body is not one of the results-holding documents
     */
    public static QueryResultsProcessor create(Operation op) {
        QueryTask maybeTask = op.getBody(QueryTask.class);

        if (Objects.equals(maybeTask.documentKind, QueryTask.KIND)) {
            return new QueryResultsProcessor(maybeTask, maybeTask.results);
        } else if (Objects.equals(maybeTask.documentKind, ODataFactoryQueryResult.KIND) ||
                Objects.equals(maybeTask.documentKind, ServiceDocumentQueryResult.KIND)) {
            return new QueryResultsProcessor(null, op.getBody(ServiceDocumentQueryResult.class));
        } else {
            throw new IllegalArgumentException(
                    "Cannot create QueryResultsProcessor from a " + maybeTask.documentKind
                            + " document");
        }
    }

    /**
     * Return the backing task, if any
     * @return
     */
    public QueryTask getQueryTask() {
        return this.task;
    }

    public boolean hasResults() {
        return this.results != null && this.results.documentCount != null && this.results.documentCount > 0;
    }

    /**
     * Return the backing task, if any, null otherwise
     * @return
     */
    public ServiceDocumentQueryResult getQueryResult() {
        return this.results;
    }

    /**
     * Returns a selected document doing any conversion if needed.
     *
     * @param selfLink
     * @param type
     * @param <T>
     * @return
     */
    public <T extends ServiceDocument> T selectedDocument(String selfLink, Class<T> type) {
        if (this.results == null || this.results.selectedDocuments == null) {
            return null;
        }

        Object o = this.results.selectedDocuments.get(selfLink);
        return convert(type, o);
    }

    /**
     * Return a results for a selfLink optionally converting it to desired typed.
     * @param selfLink
     * @param type
     * @param <T>
     * @return
     */
    public <T extends ServiceDocument> T document(String selfLink, Class<T> type) {
        if (this.results == null || this.results.documents == null) {
            return null;
        }
        Object o = this.results.documents.get(selfLink);
        return convert(type, o);
    }

    /**
     * Iterate over all selected documents converted to the desired type. The returned iterable is
     * not reusable.
     * @param type
     * @param <T>
     * @return
     */
    public <T extends ServiceDocument> Iterable<T> selectedDocuments(Class<T> type) {
        if (this.results == null || this.results.selectedDocuments == null) {
            return Collections.emptyList();
        }

        Stream<T> stream = this.results.selectedDocuments
                .values()
                .stream()
                .map(o -> convert(type, o));

        return stream::iterator;
    }

    public Iterable<String> selectedLinks() {
        if (this.results == null || this.results.selectedLinks == null) {
            return Collections.emptyList();
        }

        return this.results.selectedLinks;
    }

    public Iterable<String> documentLinks() {
        if (this.results == null || this.results.documentLinks == null) {
            return Collections.emptyList();
        }

        return this.results.documentLinks;
    }

    /**
     * Iterate over documents. The returned Iterable is not reusable.
     * @param type
     * @param <T>
     * @return
     */
    public <T extends ServiceDocument> Iterable<T> documents(Class<T> type) {
        if (this.results == null || this.results.documents == null) {
            return Collections.emptyList();
        }

        Stream<T> stream = this.results.documents
                .values()
                .stream()
                .map(o -> convert(type, o));

        return stream::iterator;
    }

    private <T extends ServiceDocument> T convert(Class<T> type, Object o) {
        if (o == null) {
            return null;
        }
        if (type.isInstance(o)) {
            return type.cast(o);
        } else if (o instanceof String || o instanceof JsonObject) {
            // assume json serialized string
            return Utils.fromJson(o, type);
        } else {
            throw new IllegalArgumentException(
                    String.format("Cannot convert %s to %s", o.getClass().getName(),
                            type.getName()));
        }
    }
}
