/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;

import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;


/**
 * Internal only helper used by the {@code LuceneDocumentIndexService} to search documents specific to a factory
 *
 * Each instance of this class is kept in a thread local variable in the index service.
 *
 * 1) Per factory index searchers
 * 2) Handover appropriate index searcher
 * 3) Helper method to detect factory specific search
 */
public class FactoryIndexSearcher {
    /**
     * Map of searchers per factory link
     */
    protected Map<String, IndexSearcher> searchers = new HashMap<>();

    /**
     * Map of pagniated searchers, indexed by creation time
     */
    protected TreeMap<Long, List<IndexSearcher>> searchersForPaginatedQueries = new TreeMap<>();

    /**
     * Searcher refresh time, per searcher (using hash code)
     */
    protected Map<Integer, Long> searcherUpdateTimesMicros = new HashMap<>();

    private final Map<String, Long> immutableParentLinks = new HashMap<>();

    private long getSearcherUpdateTime(IndexSearcher s, long queryStartTimeMicros) {
        if (s == null) {
            return 0L;
        }
        Long time = this.searcherUpdateTimesMicros.get(s.hashCode());
        if (time != null) {
            return time;
        }
        return queryStartTimeMicros;
    }

    public IndexSearcher createPaginatedQuerySearcher(long expirationMicros, IndexWriter w)
            throws IOException {
        if (w == null) {
            throw new IllegalStateException("Writer not available");
        }
        long now = Utils.getNowMicrosUtc();
        IndexSearcher s = new IndexSearcher(DirectoryReader.open(w, true, true));
        List<IndexSearcher> searchers = this.searchersForPaginatedQueries.get(expirationMicros);
        if (searchers == null) {
            searchers = new ArrayList<>();
        }
        searchers.add(s);
        this.searchersForPaginatedQueries.put(expirationMicros, searchers);
        this.searcherUpdateTimesMicros.put(s.hashCode(), now);
        return s;
    }

    public IndexSearcher createOrRefreshSearcher(String selfLink, int resultLimit, IndexWriter w,
                                                  boolean doNotRefresh,
                                                  LuceneDocumentIndexService.DocumentUpdateInfo du,
                                                  long writerUpdateTimeMicros)
            throws IOException {

        IndexSearcher s;
        boolean needNewSearcher = false;
        long now = Utils.getNowMicrosUtc();
        s = this.searchers.get(selfLink);
        long searcherUpdateTime = getSearcherUpdateTime(s, 0);
        if (s == null) {
            needNewSearcher = true;
        } else if (selfLink != null && resultLimit == 1) {
            if (du != null
                    && du.updateTimeMicros >= searcherUpdateTime) {
                needNewSearcher = !doNotRefresh;
            } else {
                String parent = UriUtils.getParentPath(selfLink);
                Long updateTime = this.immutableParentLinks.get(parent);
                if (updateTime != null && updateTime >= searcherUpdateTime) {
                    needNewSearcher = !doNotRefresh;
                }
            }
        } else if (searcherUpdateTime < writerUpdateTimeMicros) {
            needNewSearcher = !doNotRefresh;
        }

        if (s != null && !needNewSearcher) {
            return s;
        }

        if (s != null) {
            s.getIndexReader().close();
            this.searcherUpdateTimesMicros.remove(s.hashCode());
        }

        s = new IndexSearcher(DirectoryReader.open(w, true, true));

        this.searchers.put(selfLink, s);
        this.searcherUpdateTimesMicros.put(s.hashCode(), now);
        return s;
    }

    public static String detectAndFetchFactoryLink(QueryTask.QuerySpecification qs) {

        if (qs == null) {
            return null;
        }

        QueryTask.Query query = qs.query;

        if (query.term != null && query.term.matchType == QueryTask.QueryTerm.MatchType.PREFIX) {
            return query.term.matchValue;
        } else if (query.booleanClauses != null && query.booleanClauses.size() > 0) {
            for (QueryTask.Query subQuery : query.booleanClauses) {
                if (subQuery != null && subQuery.term != null && subQuery.term.matchType == QueryTask.QueryTerm.MatchType.PREFIX) {
                    return subQuery.term.matchValue;
                }
            }
        }

        return null;
    }
}