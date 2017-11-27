/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;

/**
 * Keep a searcher with the associated metadata together.
 */
public final class IndexSearcherWithMeta extends IndexSearcher {

    private final ExecutorWithAffinity executor;

    private final int preferredPoolId;

    public IndexSearcherWithMeta(IndexReader r, ExecutorWithAffinity executor) {
        super(r);
        setSimilarity(getSimilarity(false));

        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        this.executor = executor;
        int poolId = executor.getCurrentPoolId();
        if (poolId == -1) {
            poolId = executor.nextValidPoolId();
        }
        this.preferredPoolId = poolId;
    }

    private <T> T handleExecutionException(ExecutionException e) throws IOException {
        if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
        } else if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
        } else {
            throw new RuntimeException(e.getMessage());
        }
    }

    private <T> T handleInterruptedException(InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
    }

    private <T> Future<T> resubmit(Callable<T> task) {
        return this.executor.resubmit(this.preferredPoolId, task);
    }

    public TopFieldDocs searchWithAffinity(Query query, int n, Sort sort, boolean doDocScores, boolean doMaxScore)
            throws IOException {
        if (this.executor.getCurrentPoolId() == this.preferredPoolId) {
            // we are lucky
            return super.search(query, n, sort, doDocScores, doMaxScore);
        } else {
            Future<TopFieldDocs> fut = resubmit(() -> super.search(query, n, sort, doDocScores, doMaxScore));
            try {
                return fut.get();
            } catch (InterruptedException e) {
                return handleInterruptedException(e);
            } catch (ExecutionException e) {
                return handleExecutionException(e);
            }
        }
    }

    public TopDocs searchWithAffinity(Query tq, int hitCount) throws IOException {
        if (this.executor.getCurrentPoolId() == this.preferredPoolId) {
            // we are lucky
            return super.search(tq, hitCount);
        } else {
            Future<TopDocs> fut = resubmit(() -> super.search(tq, hitCount));
            try {
                return fut.get();
            } catch (InterruptedException e) {
                return handleInterruptedException(e);
            } catch (ExecutionException e) {
                return handleExecutionException(e);
            }
        }
    }

    public TopDocs searchAfterWithAffinity(ScoreDoc after, Query tq, int hitCount) throws IOException {
        if (this.executor.getCurrentPoolId() == this.preferredPoolId) {
            // we are lucky
            return super.searchAfter(after, tq, hitCount);
        } else {
            Future<TopDocs> fut = resubmit(() -> super.searchAfter(after, tq, hitCount));
            try {
                return fut.get();
            } catch (InterruptedException e) {
                return handleInterruptedException(e);
            } catch (ExecutionException e) {
                return handleExecutionException(e);
            }
        }
    }

    public TopDocs searchAfterWithAffinity(ScoreDoc after, Query query, int n, Sort sort, boolean doDocScores,
            boolean doMaxScore)
            throws IOException {
        if (this.executor.getCurrentPoolId() == this.preferredPoolId) {
            // we are lucky
            return super.searchAfter(after, query, n, sort, doDocScores, doMaxScore);
        } else {
            Future<TopDocs> fut = resubmit(() -> super.searchAfter(after, query, n, sort, doDocScores, doMaxScore));
            try {
                return fut.get();
            } catch (InterruptedException e) {
                return handleInterruptedException(e);
            } catch (ExecutionException e) {
                return handleExecutionException(e);
            }
        }
    }

    public void docWithAffinity(int docId, DocumentStoredFieldVisitor visitor) throws IOException {
        if (this.executor.getCurrentPoolId() == this.preferredPoolId) {
            // we are lucky
            super.doc(docId, visitor);
        } else {
            Future<TopDocs> fut = resubmit(() -> {
                super.doc(docId, visitor);
                return null;
            });
            try {
                fut.get();
            } catch (InterruptedException e) {
                handleInterruptedException(e);
            } catch (ExecutionException e) {
                handleExecutionException(e);
            }
        }
    }

    public int countWithAffinity(Query query) throws IOException {
        if (this.executor.getCurrentPoolId() == this.preferredPoolId) {
            // we are lucky
            return super.count(query);
        } else {
            Future<Integer> fut = resubmit(() -> super.count(query));
            try {
                return fut.get();
            } catch (InterruptedException e) {
                return handleInterruptedException(e);
            } catch (ExecutionException e) {
                return handleExecutionException(e);
            }
        }
    }

    public <T> TopGroups<T> searchGroupedWithAffinity(GroupingSearch groupingSearch, Query tq, int groupOffset,
            int groupLimit)
            throws IOException {
        if (this.executor.getCurrentPoolId() == this.preferredPoolId) {
            // we are lucky
            return groupingSearch.search(this, tq, groupOffset, groupLimit);
        } else {
            Future<TopGroups<T>> fut = resubmit(() -> groupingSearch.search(this, tq, groupOffset, groupLimit));
            try {
                return fut.get();
            } catch (InterruptedException e) {
                return handleInterruptedException(e);
            } catch (ExecutionException e) {
                return handleExecutionException(e);
            }
        }
    }
}
