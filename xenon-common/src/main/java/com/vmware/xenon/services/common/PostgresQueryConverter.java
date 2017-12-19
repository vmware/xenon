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

import java.util.HashSet;
import java.util.List;
import java.util.StringJoiner;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryRuntimeContext;

/**
 * Convert {@link QueryTask.QuerySpecification} to native Lucene query.
 */
final class PostgresQueryConverter {
    private PostgresQueryConverter() {
    }

    static String convert(QueryTask.Query query, QueryRuntimeContext context) {
        try {
            if (query.occurance == null) {
                query.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
            }

            String condition;

            // Special case for top level occurrence which was ignored otherwise
            if (query.booleanClauses != null) {
                if (query.term != null) {
                    throw new IllegalArgumentException(
                          "term and booleanClauses are mutually exclusive");
                }

                String booleanClauses = convertToLuceneQuery(query, context);

                if (query.occurance == null) {
                    condition = booleanClauses;
                } else {
                    switch (query.occurance) {
                    case MUST_NOT_OCCUR:
                        condition = String.format("(NOT (%s))", booleanClauses);
                        break;
                    case SHOULD_OCCUR:
                        condition = booleanClauses;
                        break;
                    case MUST_OCCUR:
                    default:
                        condition = booleanClauses;
                    }
                }
            } else {
                condition = convertToLuceneQuery(query, context);
            }

            Utils.logWarning("convert: %s\n%s", condition, Utils.toJsonHtml(query));
            return condition;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    static String convertToLuceneQuery(QueryTask.Query query, QueryRuntimeContext context) {
        if (query.occurance == null) {
            query.occurance = QueryTask.Query.Occurance.MUST_OCCUR;
        }

        if (query.booleanClauses != null) {
            if (query.term != null) {
                throw new IllegalArgumentException(
                      "term and booleanClauses are mutually exclusive");
            }

            return convertToLuceneBooleanQuery(query, context);
        }

        if (query.term == null) {
            throw new IllegalArgumentException("One of term, booleanClauses must be provided");
        }

        QueryTask.QueryTerm term = query.term;
        validateTerm(term);
        if (term.matchType == null) {
            term.matchType = QueryTask.QueryTerm.MatchType.TERM;
        }

        if (context != null && query.occurance != QueryTask.Query.Occurance.MUST_NOT_OCCUR
                && ServiceDocument.FIELD_NAME_KIND.equals(term.propertyName)) {
            if (context.kindScope == null) {
                // assume most queries contain 1 or 2 document kinds. Initialize with size 4
                // to prevent resizing when the second kind is added. The default size of 16
                // has never been filled up.
                context.kindScope = new HashSet<>(4);
            }
            context.kindScope.add(term.matchValue);
        }

        String condition;
        if (query.term.range != null) {
            condition = convertToLuceneNumericRangeQuery(query);
        } else if (query.term.matchType == QueryTask.QueryTerm.MatchType.WILDCARD) {
            condition = convertToLuceneWildcardTermQuery(query);
        } else if (query.term.matchType == QueryTask.QueryTerm.MatchType.PHRASE) {
            condition = convertToLucenePhraseQuery(query);
        } else if (query.term.matchType == QueryTask.QueryTerm.MatchType.PREFIX) {
            condition = convertToLucenePrefixQuery(query);
        } else {
            condition = convertToLuceneSingleTermQuery(query);
        }

//        System.out.println("condition: " + condition);
        return condition;
    }

    static String wrapField(String field) {
        if (ServiceDocument.isBuiltInDocumentField(field)) {
            return field.toLowerCase();
        }

        if (field.endsWith(".item")) {
            return String.format("data -> '%s'", escapeSqlString(field.replace(".item", "")));
        }

        if (field.contains(".")) {
            return String.format("data #>> '{%s}'", escapeSqlString(field.replace(".", ",")));
        }

        return String.format("data ->> '%s'", escapeSqlString(field));
    }

    static String escapeSqlLike(String p) {
        return p.replace("\\", "\\\\")
              .replace("[", "\\[")
              .replace("(", "\\(")
              .replace("_", "\\_")
              .replace("%", "\\%")
              .replace("'", "\\'");
    }

    static String convertToLuceneSingleTermQuery(QueryTask.Query query) {
        // TODO: find type
        if (query.term.propertyName.equals("textValue")) {
            return String.format("%s LIKE '%%%s%%' ESCAPE '\\'", wrapField(query.term.propertyName),
                  escapeSqlLike(query.term.matchValue));
        }

        if (query.term.propertyName.endsWith(".item")) {
            return String.format("%s ? '%s'", wrapField(query.term.propertyName),
                  escapeSqlString(query.term.matchValue));
        }

        return String.format("%s = '%s'", wrapField(query.term.propertyName),
              escapeSqlString(query.term.matchValue));
    }

    // For language agnostic, or advanced token parsing a Tokenizer from the LUCENE
    // analysis package should be used.
    // TODO consider compiling the regular expression.
    // Currently phrase queries are considered a rare, special case.
    static String convertToLucenePhraseQuery(QueryTask.Query query) {
        String[] tokens = query.term.matchValue.split("\\W");
        StringJoiner joiner = new StringJoiner(" and ");
        for (String token : tokens) {
            joiner.add(String.format("%s LIKE '%%%s%%' ESCAPE '\\'", wrapField(query.term.propertyName),
                    escapeSqlLike(token)));
        }
        return joiner.toString();
    }

    static String convertToLucenePrefixQuery(QueryTask.Query query) {
        // if the query is a prefix on a self link that matches all documents, then
        // special case the query to a MatchAllDocsQuery to avoid looking through
        // the entire index as the number of terms is equal to the size of the index
        if ((query.term.propertyName.equals(ServiceDocument.FIELD_NAME_SELF_LINK)) &&
                query.term.matchValue.equals(UriUtils.URI_PATH_CHAR)) {
            return "TRUE";
        }

        return String.format("%s LIKE '%s%%' ESCAPE '\\'", wrapField(query.term.propertyName),
              escapeSqlLike(query.term.matchValue));
    }

    static String convertToLuceneWildcardTermQuery(QueryTask.Query query) {
        // if the query is a wildcard on a self link that matches all documents, then
        // special case the query to a MatchAllDocsQuery to avoid looking through
        // the entire index as the number of terms is equal to the size of the index
        if ((query.term.propertyName.equals(ServiceDocument.FIELD_NAME_SELF_LINK)) &&
                query.term.matchValue.equals(UriUtils.URI_WILDCARD_CHAR)) {
            return "TRUE";
        }
        return String.format("%s LIKE '%s' ESCAPE '\\'", wrapField(query.term.propertyName),
              escapeSqlLike(query.term.matchValue).replace('*', '%'));
    }

    static String convertToLuceneNumericRangeQuery(QueryTask.Query query) {
        QueryTask.QueryTerm term = query.term;

        term.range.validate();
        if (term.range.type == ServiceDocumentDescription.TypeName.LONG) {
            return createLongRangeQuery(term.propertyName, term.range);
        } else if (term.range.type == ServiceDocumentDescription.TypeName.DOUBLE) {
            return createDoubleRangeQuery(term.propertyName, term.range);
        } else if (term.range.type == ServiceDocumentDescription.TypeName.DATE) {
            // Date specifications must be in microseconds since epoch
            return createLongRangeQuery(term.propertyName, term.range);
        } else {
            throw new IllegalArgumentException("Type is not supported:"
                  + term.range.type);
        }
    }

    static String convertToLuceneBooleanQuery(QueryTask.Query query, QueryRuntimeContext context) {
        String parentQuery = null;

        // Recursively build the boolean query. We allow arbitrary nesting and grouping.
        for (QueryTask.Query q : query.booleanClauses) {
            parentQuery = buildBooleanQuery(parentQuery, q, context);
        }
        if (parentQuery == null) {
            throw new IllegalArgumentException("Empty booleanClauses");
        }
        return "(" + parentQuery + ")";
    }

    static String buildBooleanQuery(String parent, QueryTask.Query clause, QueryRuntimeContext context) {
        String lq = convertToLuceneQuery(clause, context);

        if (clause.occurance == null) {
            if (parent == null) {
                return lq;
            }
            return String.format("%s AND %s", parent, lq);
        }

        switch (clause.occurance) {
        case MUST_NOT_OCCUR:
            if (parent == null) {
                return String.format("NOT %s", lq);
            }
            return String.format("%s AND NOT %s", parent, lq);
        case SHOULD_OCCUR:
            if (parent == null) {
                return String.format("%s", lq);
                //return String.format("(false OR %s)", lq);
            }
            return String.format("%s OR %s", parent, lq);
        case MUST_OCCUR:
        default:
            if (parent == null) {
                return lq;
            }
            // TODO: remove hack
            //if (parent.contains(" OR ")) {
            //    return String.format("(true OR %s) AND %s", parent, lq);
            //}
            return String.format("%s AND %s", parent, lq);
        }
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

    /*
    static SortField.Type convertToLuceneType(ServiceDocumentDescription.TypeName typeName) {
        if (typeName == null) {
            return SortField.Type.STRING;
        }

        switch (typeName) {
        case STRING:
            return SortField.Type.STRING;
        case DOUBLE:
            return SortField.Type.DOUBLE;
        case LONG:
            return SortField.Type.LONG;

        default:
            return SortField.Type.STRING;
        }
    }
    */

    static String convertToPostgresSort(QueryTask.QuerySpecification querySpecification, boolean isGroupSort) {
        QueryTask.QueryTerm sortTerm = isGroupSort ? querySpecification.groupSortTerm
                : querySpecification.sortTerm;

        QueryTask.QuerySpecification.SortOrder sortOrder = isGroupSort
                ? querySpecification.groupSortOrder
                : querySpecification.sortOrder;

        if (querySpecification.options.contains(QueryOption.TOP_RESULTS)) {
            if (querySpecification.resultLimit <= 0
                    || querySpecification.resultLimit == Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "resultLimit should be a positive integer less than MAX_VALUE");
            }
        }

        if (sortOrder == null) {
            if (isGroupSort) {
                querySpecification.groupSortOrder = QueryTask.QuerySpecification.SortOrder.ASC;
            } else {
                querySpecification.sortOrder = QueryTask.QuerySpecification.SortOrder.ASC;
            }
        }

        sortTerm.sortOrder = sortOrder;

        List<QueryTask.QueryTerm> additionalSortTerms = isGroupSort
                ? querySpecification.additionalGroupSortTerms
                : querySpecification.additionalSortTerms;

        if (additionalSortTerms == null) {
            return convertToPostgresSortField(sortTerm);
        }

        StringJoiner sortFields = new StringJoiner(",");
        sortFields.add(convertToPostgresSortField(sortTerm));

        int len = additionalSortTerms.size() + 1;
        for (int index = 1; index < len; index++) {
            sortFields.add(convertToPostgresSortField(additionalSortTerms.get(index - 1)));
        }

        //SortField[] sortFields = new SortField[additionalSortTerms.size() + 1];
        //sortFields[0] = convertToPostgresSortField(sortTerm);

//        for (int index = 1; index < sortFields.length; index++) {
//            sortFields[index] = convertToPostgresSortField(additionalSortTerms.get(index - 1));
//        }

        return sortFields.toString();
    }

    private static String convertToPostgresSortField(QueryTask.QueryTerm sortTerm) {
        validateSortTerm(sortTerm);

        if (sortTerm.sortOrder == null) {
            sortTerm.sortOrder = QueryTask.QuerySpecification.SortOrder.ASC;
        }

        boolean order = sortTerm.sortOrder != QueryTask.QuerySpecification.SortOrder.ASC;

        /*
        String sortField;
        SortField.Type type = convertToLuceneType(sortTerm.propertyType);

        switch (type) {
        case LONG:
        case DOUBLE:
            sortField = sortTerm.propertyName;
            break;
        default:
            sortField = LuceneIndexDocumentHelper.createSortFieldPropertyName(sortTerm.propertyName);
            break;
        }
        */
        String sortField = wrapField(sortTerm.propertyName);
        sortField += order ? " DESC" : " ASC";
        return sortField;
    }

    static void validateSortTerm(QueryTask.QueryTerm term) {
        if (term.propertyType == null) {
            throw new IllegalArgumentException("term.propertyType is required");
        }

        if (term.propertyName == null) {
            throw new IllegalArgumentException("term.propertyName is required");
        }
    }

    private static String createLongRangeQuery(String propertyName, QueryTask.NumericRange<?> range) {
        // The range query constructed below is based-off
        // lucene documentation as per the link:
        // https://lucene.apache.org/core/6_0_0/core/org/apache/lucene/document/LongPoint.html
        Long min = range.min == null ? Long.MIN_VALUE : range.min.longValue();
        Long max = range.max == null ? Long.MAX_VALUE : range.max.longValue();
        if (!range.isMinInclusive) {
            min = Math.addExact(min, 1);
        }
        if (!range.isMaxInclusive) {
            max = Math.addExact(max, -1);
        }
        if (min == max) {
            return String.format("%s = '%s'", wrapField(propertyName), max);
        }
        if (min > max) {
            // TODO: Why need to swap while using BETWEEN?
            long t = min;
            min = max;
            max = t;
        }
        return String.format("%s BETWEEN '%s' AND '%s'", wrapField(propertyName), min, max);
    }

    private static String createDoubleRangeQuery(String propertyName, QueryTask.NumericRange<?> range) {
        // The range query constructed below is based-off
        // lucene documentation as per the link:
        // https://lucene.apache.org/core/6_0_0/core/org/apache/lucene/document/DoublePoint.html
        Double min = range.min == null ? Double.NEGATIVE_INFINITY : range.min.doubleValue();
        Double max = range.max == null ? Double.POSITIVE_INFINITY : range.max.doubleValue();
        if (!range.isMinInclusive) {
            min = Math.nextUp(min);
        }
        if (!range.isMaxInclusive) {
            max = Math.nextDown(max);
        }
        if (min == max) {
            return String.format("%s = '%s'", wrapField(propertyName), max);
        }
        if (min > max) {
            // TODO: Why need to swap while using BETWEEN?
            double t = min;
            min = max;
            max = t;
        }
        return String.format("%s BETWEEN '%s' AND '%s'", wrapField(propertyName), min, max);
    }

    static String escapeSqlString(String s) {
        return s.replace("'", "''");
    }

}
