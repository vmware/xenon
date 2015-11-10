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

import static org.apache.lucene.search.NumericRangeQuery.newDoubleRange;
import static org.apache.lucene.search.NumericRangeQuery.newLongRange;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

import com.vmware.dcp.common.ServiceDocumentDescription;

/**
 * Convert {@link QueryTask.QuerySpecification} to native Lucene query.
 */
class LuceneQueryConverter {
    static Query convertToLuceneQuery(QueryTask.Query query) {
        if (query.occurance == null) {
            query.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
        }

        if (query.booleanClauses != null) {
            if (query.term != null) {
                throw new IllegalArgumentException(
                        "term and booleanClauses are mutually exclusive");
            }

            return convertToLuceneBooleanQuery(query);
        }

        if (query.term == null) {
            throw new IllegalArgumentException("One of term, booleanClauses must be provided");
        }

        QueryTask.QueryTerm term = query.term;
        validateTerm(term);
        if (term.matchType == null) {
            term.matchType = QueryTask.QueryTerm.MatchType.TERM;
        }

        if (query.term.range != null) {
            return convertToLuceneNumericRangeQuery(query);
        } else if (query.term.matchType == QueryTask.QueryTerm.MatchType.WILDCARD) {
            return convertToLuceneWildcardTermQuery(query);
        } else if (query.term.matchType == QueryTask.QueryTerm.MatchType.PHRASE) {
            return convertToLucenePhraseQuery(query);
        } else {
            return convertToLuceneSingleTermQuery(query);
        }
    }

    static Query convertToLuceneSingleTermQuery(QueryTask.Query query) {
        return new TermQuery(convertToLuceneTerm(query.term));
    }

    // For language agnostic, or advanced token parsing a Tokenizer from the LUCENE
    // analysis package should be used.
    // TODO consider compiling the regular expression.
    // Currently phrase queries are considered a rare, special case.
    static Query convertToLucenePhraseQuery(QueryTask.Query query) {
        String[] tokens = query.term.matchValue.split("\\W");
        PhraseQuery pq = new PhraseQuery();
        for (String token : tokens) {
            pq.add(new Term(query.term.propertyName, token));
        }
        return pq;
    }

    static Query convertToLuceneWildcardTermQuery(QueryTask.Query query) {
        return new WildcardQuery(convertToLuceneTerm(query.term));
    }

    static Query convertToLuceneNumericRangeQuery(QueryTask.Query query) {
        QueryTask.QueryTerm term = query.term;

        term.range.validate();
        if (term.range.type == ServiceDocumentDescription.TypeName.LONG) {
            Long min = term.range.min == null ? null : term.range.min.longValue();
            Long max = term.range.max == null ? null : term.range.max.longValue();
            return newLongRange(
                    term.propertyName, term.range.precisionStep, min, max,
                    term.range.isMinInclusive,
                    term.range.isMaxInclusive);
        } else if (term.range.type == ServiceDocumentDescription.TypeName.DOUBLE) {
            Double min = term.range.min == null ? null : term.range.min.doubleValue();
            Double max = term.range.max == null ? null : term.range.max.doubleValue();
            return newDoubleRange(
                    term.propertyName, min, max,
                    term.range.isMinInclusive, term.range.isMaxInclusive);
        } else if (term.range.type == ServiceDocumentDescription.TypeName.DATE) {
            Long min = term.range.min == null ? null : term.range.min.longValue();
            Long max = term.range.max == null ? null : term.range.max.longValue();
            // Date specifications must be in microseconds since epoch
            return newLongRange(
                    term.propertyName, min, max, term.range.isMinInclusive,
                    term.range.isMaxInclusive);
        } else {
            throw new IllegalArgumentException("Type is not supported:"
                    + term.range.type);
        }
    }

    static Query convertToLuceneBooleanQuery(QueryTask.Query query) {
        BooleanQuery parentQuery = new BooleanQuery();

        // Recursively build the boolean query. We allow arbitrary nesting and grouping.
        for (QueryTask.Query q : query.booleanClauses) {
            buildBooleanQuery(parentQuery, q);
        }
        return parentQuery;
    }

    static void buildBooleanQuery(BooleanQuery parent, QueryTask.Query clause) {
        Query lq = convertToLuceneQuery(clause);
        BooleanClause bc = new BooleanClause(lq, convertToLuceneOccur(clause.occurance));
        parent.add(bc);
    }

    static BooleanClause.Occur convertToLuceneOccur(QueryTask.Query.Occurance occurance) {
        if (occurance == null) {
            return BooleanClause.Occur.MUST;
        }

        switch (occurance) {
        case MUST_NOT_OCCUR:
            return BooleanClause.Occur.MUST_NOT;
        case MUST_OCCUR:
            return BooleanClause.Occur.MUST;
        case SHOULD_OCCUR:
            return BooleanClause.Occur.SHOULD;
        default:
            return BooleanClause.Occur.MUST;
        }
    }

    static Term convertToLuceneTerm(QueryTask.QueryTerm term) {
        return new Term(term.propertyName, term.matchValue);
    }

    static void validateTerm(QueryTask.QueryTerm term) {
        if (term.range == null && term.matchValue == null) {
            throw new IllegalArgumentException(
                    "One of term.matchValue, term.range is required");
        }

        if (term.range != null && term.matchValue != null) {
            throw new IllegalArgumentException(
                    "term.matchValue and term.range are exclusive of each other");
        }

        if (term.propertyName == null) {
            throw new IllegalArgumentException("term.propertyName is required");
        }
    }

    static SortField.Type convertToLuceneType(ServiceDocumentDescription.TypeName typeName) {
        if (typeName == null) {
            return SortField.Type.STRING;
        }

        switch (typeName) {

        case STRING:
            return SortField.Type.STRING;
        case BYTES:
            return SortField.Type.BYTES;
        case DOUBLE:
            return SortField.Type.DOUBLE;
        case ARRAY:
            return SortField.Type.CUSTOM;
        case LONG:
            return SortField.Type.LONG;

        default:
            return SortField.Type.STRING;
        }
    }

    static Sort convertToLuceneSort(QueryTask.QuerySpecification querySpecification) {

        validateSortTerm(querySpecification.sortTerm);

        if (querySpecification.sortOrder == null) {
            querySpecification.sortOrder = QueryTask.QuerySpecification.SortOrder.ASC;
        }

        boolean order =
                querySpecification.sortOrder != QueryTask.QuerySpecification.SortOrder.ASC;
        return new Sort(new SortField(querySpecification.sortTerm.propertyName,
                convertToLuceneType(querySpecification.sortTerm.propertyType), order));

    }

    static void validateSortTerm(QueryTask.QueryTerm term) {

        if (term.propertyType == null) {
            throw new IllegalArgumentException("term.propertyType is required");
        }

        if (term.propertyName == null) {
            throw new IllegalArgumentException("term.propertyName is required");
        }
    }

}
