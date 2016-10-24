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
import java.util.stream.Stream;

import com.vmware.xenon.services.common.QueryTask;

/**
 * A thin wrapper on top of {@link QueryTask} that converts query results to service documents.
 */
public final class QueryResults {
    private final QueryTask task;

    public QueryResults(QueryTask task) {
        if (task == null) {
            throw new IllegalArgumentException("task is null");
        }

        this.task = task;
    }

    public QueryResults(Operation op) {
        this.task = op.getBody(QueryTask.class);
        if (!(op.getBodyRaw() instanceof QueryTask)) {
            if (!this.task.documentKind.equals(QueryTask.KIND)) {
                throw new IllegalArgumentException(
                        "Operation " + op.getUri() + " is not a QueryTask");
            }
        }
    }

    /**
     * Return the backing task.
     * @return
     */
    public QueryTask getQueryTask() {
        return this.task;
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
        if (this.task.results == null || this.task.results.selectedDocuments == null) {
            return null;
        }

        Object o = this.task.results.selectedDocuments.get(selfLink);
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
        if (this.task.results == null || this.task.results.documents == null) {
            return null;
        }
        Object o = this.task.results.documents.get(selfLink);
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
        if (this.task.results == null || this.task.results.selectedDocuments == null) {
            return Collections.emptyList();
        }

        Stream<T> stream = this.task.results.selectedDocuments
                .values()
                .stream()
                .map(o -> convert(type, o));

        return stream::iterator;
    }

    public Iterable<String> selectedLinks() {
        if (this.task.results == null || this.task.results.selectedLinks == null) {
            return Collections.emptyList();
        }

        return this.task.results.selectedLinks;
    }

    public Iterable<String> documentLinks() {
        if (this.task.results == null || this.task.results.documentLinks == null) {
            return Collections.emptyList();
        }

        return this.task.results.documentLinks;
    }

    /**
     * Iterate over documents. The returned Iterable is not reusable.
     * @param type
     * @param <T>
     * @return
     */
    public <T extends ServiceDocument> Iterable<T> documents(Class<T> type) {
        if (this.task.results == null || this.task.results.documents == null) {
            return Collections.emptyList();
        }

        Stream<T> stream = this.task.results.documents
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
        } else {
            // assume json serialized string
            return Utils.fromJson(o.toString(), type);
        }
    }
}
