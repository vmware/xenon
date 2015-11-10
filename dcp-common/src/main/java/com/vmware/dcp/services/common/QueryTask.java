/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentDescription.TypeName;
import com.vmware.dcp.common.ServiceDocumentQueryResult;
import com.vmware.dcp.common.TaskState;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.QueryTask.Query.Occurance;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification.SortOrder;
import com.vmware.dcp.services.common.QueryTask.QueryTerm.MatchType;

public class QueryTask extends ServiceDocument {

    public static final String KIND = Utils.buildKind(QueryTask.class);

    public static class QuerySpecification {
        public static final String FIELD_NAME_CHARACTER = ".";
        public static final String FIELD_NAME_REGEXP = "\\" + FIELD_NAME_CHARACTER;
        public static final String COLLECTION_FIELD_SUFFIX = "item";

        /**
         * Infrastructure use only (not serialized)
         */
        public static class QueryRuntimeContext {
            public transient Object nativeQuery;
            public transient Object nativePage;
            public transient Object nativeSearcher;
            public transient Object nativeSort;
            public transient QueryFilter filter;
        }

        public enum QueryOption {
            /**
             * Query results are updated in real time, by using {@code QueryFilter} instance on the index.
             * Any update that satisfies the query filter will cause the results to be updated and a self
             * PATCH to be sent on the service.
             */
            CONTINUOUS,

            /**
             * Query results will return the number of documents that satisfy the query and populate the
             * the {@link results.documentCount} field. The results will not contain links or documents
             */
            COUNT,

            /**
             * The query will execute on the current view of the index, potentially missing recent updates.
             * This improves performance but does not guarantee latest results.
             */
            DO_NOT_REFRESH,

            /**
             * Query results will include the state documents in the {@link results.documents} collection
             */
            EXPAND_CONTENT,

            /**
             * The query will execute over all document versions, not just the latest per self link. Each
             * document self link will be annotated with the version
             */
            INCLUDE_ALL_VERSIONS,

            /**
             * Query results will include document versions marked deleted
             */
            INCLUDE_DELETED,

            /**
             * Query results will be sorted by the specified sort field
             */
            SORT,

            /**
             * Infrastructure use only. Query originated from a query task service
             */
            TASK,
        }

        public enum SortOrder {
            ASC, DESC
        }

        /*
         * Query definition
         */
        public Query query = new Query();

        public QueryTerm sortTerm;

        public SortOrder sortOrder;

        /**
         * The optional resultLimit field is used to enable query results pagination. When
         * resultLimit is set, the query task will not return any results when finished, but will
         * include a nextPageLink field. A client can then issue a GET request on the nextPageLink
         * to get the first page of results. A nextPageLink field will be included in GET response
         * documents until all query results have been consumed.
         */
        public Integer resultLimit;

        /**
         * The optional expectedResultCount field will enable query retries until
         * expectedResultCount is met or the QueryTask expires. taskInfo.stage will remain in the
         * STARTED phase until such time.
         */
        public Long expectedResultCount;
        public EnumSet<QueryOption> options = EnumSet.noneOf(QueryOption.class);
        public ServiceOption targetIndex;

        /**
         * Infrastructure use only
         */
        public transient QueryRuntimeContext context = new QueryRuntimeContext();

        public static String buildCompositeFieldName(String... fieldNames) {
            StringBuilder sb = new StringBuilder();
            for (String s : fieldNames) {
                if (s == null) {
                    continue;
                }
                sb.append(s).append(FIELD_NAME_CHARACTER);
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        public static String buildCollectionItemName(String fieldName) {
            return fieldName + FIELD_NAME_CHARACTER + COLLECTION_FIELD_SUFFIX;
        }

        /**
         * Convert the given value to a normalized string representation that is used for
         * both generating indexed values and query criteria used to match against those
         * indexes.
         *
         * @return A string value that can be used to both index and query, or NULL if the given
         */
        public static String toMatchValue(Object value) {
            if (value == null) {
                return null;
            } else if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Boolean) {
                return toMatchValue((boolean) value);
            } else if (value instanceof URI) {
                return toMatchValue((URI) value);
            } else if (value instanceof Enum) {
                return toMatchValue((Enum<?>) value);
            } else {
                return value.toString();
            }
        }

        public static String toMatchValue(boolean value) {
            return value ? "true" : "false";
        }

        public static String toMatchValue(URI value) {
            return value == null ? null : value.toString();
        }

        public static String toMatchValue(Enum<?> value) {
            return value == null ? null : value.name();
        }

        /**
         * Use the Query.Builder instead.
         */
        @Deprecated
        public static void buildListValueClause(QueryTask q, String propName,
                Collection<String> values) {
            QueryTask.Query inClause = new QueryTask.Query();
            for (String value : values) {
                QueryTask.Query clause = new QueryTask.Query()
                        .setTermPropertyName(propName)
                        .setTermMatchValue(value);

                clause.occurance = Occurance.SHOULD_OCCUR;
                inClause.addBooleanClause(clause);
                if (values.size() == 1) {
                    // if we only have one value then change it to single value clause.
                    inClause = clause;
                    inClause.occurance = Occurance.MUST_OCCUR;
                }
            }

            q.querySpec.query.addBooleanClause(inClause);
        }

        public static QueryTask addExpandOption(QueryTask queryTask) {
            queryTask.querySpec.options = EnumSet
                    .of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
            return queryTask;
        }
    }

    public static class PostProcessingSpecification {
        public enum GroupOperation {
            SUM, AVG, MIN
        }

        /**
         * Query term that picks the property that the group operation will run over its values
         */
        public QueryTerm selectionTerm;
    }

    public static class NumericRange<T extends Number & Comparable<? super T>> {
        public TypeName type;
        public T min;
        public T max;

        public boolean isMinInclusive;
        public boolean isMaxInclusive;
        public int precisionStep = 4;

        public static NumericRange<Long> createLongRange(Long min, Long max,
                boolean isMinInclusive, boolean isMaxInclusive) {
            NumericRange<Long> nr = new NumericRange<Long>();
            nr.type = TypeName.LONG;
            nr.isMaxInclusive = isMaxInclusive;
            nr.isMinInclusive = isMinInclusive;
            nr.max = max;
            nr.min = min;
            return nr;
        }

        public static NumericRange<Double> createDoubleRange(Double min, Double max,
                boolean isMinInclusive, boolean isMaxInclusive) {
            NumericRange<Double> nr = new NumericRange<Double>();
            nr.type = TypeName.DOUBLE;
            nr.isMaxInclusive = isMaxInclusive;
            nr.isMinInclusive = isMinInclusive;
            nr.max = max;
            nr.min = min;
            return nr;
        }

        public void validate() throws IllegalArgumentException {
            if (this.max == null && this.min == null) {
                throw new IllegalArgumentException("max and min can not both be null");
            }

            if (this.max != null) {
                if (this.min != null && this.max.compareTo(this.min) < 0) {
                    throw new IllegalArgumentException("max must be greater than min");
                }
            }

            if (this.min != null) {
                if (this.max != null && this.min.compareTo(this.max) > 0) {
                    throw new IllegalArgumentException("max must be greater than min");
                }
            }

            if (this.type == null) {
                throw new IllegalArgumentException("type must be specified");
            }
        }

        public static NumericRange<?> createLessThanRange(Number max) {
            if (max instanceof Double) {
                return createDoubleRange(Double.MIN_VALUE, (Double) max, true, false);
            }

            return createLongRange(Long.MIN_VALUE, (Long) max, true, false);
        }

        public static NumericRange<?> createLessThanOrEqualRange(Number max) {
            if (max instanceof Double) {
                return createDoubleRange(Double.MIN_VALUE, (Double) max, true, true);
            }

            return createLongRange(Long.MIN_VALUE, (Long) max, true, true);
        }

        public static NumericRange<?> createGreaterThanRange(Number min) {
            if (min instanceof Double) {
                return createDoubleRange((Double) min, Double.MAX_VALUE, false, true);
            }

            return createLongRange((Long) min, Long.MAX_VALUE, false, true);
        }

        public static NumericRange<?> createGreaterThanOrEqualRange(Number min) {
            if (min instanceof Double) {
                return createDoubleRange((Double) min, Double.MAX_VALUE, true, true);
            }

            return createLongRange((Long) min, Long.MAX_VALUE, true, true);
        }

        public static NumericRange<?> createEqualRange(Number num) {
            if (num instanceof Double) {
                return createDoubleRange((Double) num, (Double) num, true, true);
            }
            return createLongRange((Long) num, (Long) num, true, true);
        }
    }

    public static class QueryTerm {
        public enum MatchType {
            WILDCARD, TERM, PHRASE
        }

        public String propertyName;
        public TypeName propertyType;
        public String matchValue;
        public MatchType matchType;
        public NumericRange<?> range;
    }

    public static class Query {
        public enum Occurance {
            MUST_OCCUR, MUST_NOT_OCCUR, SHOULD_OCCUR
        }

        /**
         * Builder class for constructing {@linkplain Query DCP queries}.
         */
        public static final class Builder {
            private final Query query;

            private Builder(Occurance occurance) {
                this.query = new Query();
                this.query.occurance = occurance;
            }

            /**
             * Constructs a query that {@linkplain Occurance#MUST_OCCUR must occur} in matched documents.
             * @return a reference to this object.
             */
            public static Builder create() {
                return new Builder(Occurance.MUST_OCCUR);
            }

            /**
             * Constructs a query with the given {@linkplain Occurance occurance}.
             * @param occurance the occurance.
             * @return a reference to this object.
             */
            public static Builder create(Occurance occurance) {
                return new Builder(occurance);
            }

            /**
             * Add a clause which matches the {@linkplain ServiceDocument#documentKind document kind} of the provided class.
             * @param documentClass the service document class.
             * @return a reference to this object.
             */
            public Builder addKindFieldClause(Class<? extends ServiceDocument> documentClass) {
                return addFieldClause(FIELD_NAME_KIND, Utils.buildKind(documentClass));
            }

            /**
             * Add a clause with the specified occurance which matches the
             * {@linkplain ServiceDocument#documentKind document kind} of the provided class.
             * @param documentClass the service document class.
             * @param occurance the occurance for this clause.
             * @return a reference to this object.
             */
            public Builder addKindFieldClause(Class<? extends ServiceDocument> documentClass, Occurance occurance) {
                return addFieldClause(FIELD_NAME_KIND, Utils.buildKind(documentClass), occurance);
            }

            /**
             * Add a clause which matches a collection item.
             * @param collectionFieldName the collection field name.
             * @param itemName the item name in the collection to match.
             * @return a reference to this object.
             */
            public Builder addCollectionItemClause(String collectionFieldName, String itemName) {
                return addFieldClause(
                        QuerySpecification.buildCollectionItemName(collectionFieldName),
                        itemName);
            }

            /**
             * Add a clause with the specified occurance which matches a collection item.
             * @param collectionFieldName the collection field name.
             * @param itemName the item name in the collection to match.
             * @param occurance the occurance for this clause.
             * @return a reference to this object.
             */
            public Builder addCollectionItemClause(String collectionFieldName, String itemName, Occurance occurance) {
                return addFieldClause(
                        QuerySpecification.buildCollectionItemName(collectionFieldName),
                        itemName,
                        occurance);
            }

            /**
             * Add a clause which matches a property with at least one of several specified
             * values (analogous to a SQL "IN" statement).
             * @param fieldName the field name.
             * @param itemNames the item names in the collection to match.
             * @return a reference to this object.
             */
            public Builder addInClause(String fieldName, Collection<String> itemNames) {
                return addInClause(fieldName, itemNames, Occurance.MUST_OCCUR);
            }

            /**
             * Add a clause with the given occurance which matches a property with at least one of several specified
             * values (analogous to a SQL "IN" statement).
             * @param fieldName the field name.
             * @param itemNames the item names in the collection to match.
             * @param occurance the occurance for this clause.
             * @return a reference to this object.
             */
            public Builder addInClause(String fieldName, Collection<String> itemNames, Occurance occurance) {
                if (itemNames.size() == 1) {
                    return addFieldClause(
                            fieldName,
                            itemNames.iterator().next());
                }

                Query.Builder inClause = Query.Builder.create(occurance);
                for (String itemName : itemNames) {
                    inClause.addFieldClause(fieldName, itemName, Occurance.SHOULD_OCCUR);
                }

                return addClause(inClause.build());
            }

            /**
             * Add a clause which matches a collection containing at least one of several specified
             * values (analogous to a SQL "IN" statement).
             * @param collectionFieldName the collection field name.
             * @param itemNames the item names in the collection to match.
             * @return a reference to this object.
             */
            public Builder addInCollectionItemClause(String collectionFieldName,
                    Collection<String> itemNames) {
                String collectionItemFieldName = QuerySpecification.buildCollectionItemName(
                        collectionFieldName);
                return addInClause(collectionItemFieldName, itemNames);
            }

            /**
             * Add a clause with the given occurance which matches a collection containing at least one of several
             * specified values (analogous to a SQL "IN" statement).
             * @param collectionFieldName the collection field name.
             * @param itemNames the item names in the collection to match.
             * @param occurance the occurance for this clause.
             * @return a reference to this object.
             */
            public Builder addInCollectionItemClause(String collectionFieldName,
                    Collection<String> itemNames, Occurance occurance) {
                String collectionItemFieldName = QuerySpecification.buildCollectionItemName(
                        collectionFieldName);
                return addInClause(collectionItemFieldName, itemNames, occurance);
            }

            /**
             * Add a clause which matches a nested field value.
             * @param parentFieldName the top level field name.
             * @param nestedFieldName the nested field name.
             * @param nestedFieldValue the nested field value to match.
             * @return a reference to this object.
             */
            public Builder addCompositeFieldClause(String parentFieldName, String nestedFieldName, String nestedFieldValue) {
                return addFieldClause(
                        QuerySpecification.buildCompositeFieldName(parentFieldName, nestedFieldName),
                        nestedFieldValue);
            }

            /**
             * Add a clause with the given occurance which matches a nested field value.
             * @param parentFieldName the top level field name.
             * @param nestedFieldName the nested field name.
             * @param nestedFieldValue the nested field value to match.
             * @param occurance the occurance for this clause.
             * @return a reference to this object.
             */
            public Builder addCompositeFieldClause(String parentFieldName, String nestedFieldName,
                    String nestedFieldValue, Occurance occurance) {
                return addFieldClause(
                        QuerySpecification.buildCompositeFieldName(parentFieldName, nestedFieldName),
                        nestedFieldValue,
                        occurance);
            }

            /**
             * Add a {@link Occurance#MUST_OCCUR} clause which matches a top level field name using
             * {@link MatchType#TERM}.
             * @param fieldName the top level field name.
             * @param fieldValue the field value to match.
             * @return a reference to this object.
             */
            public Builder addFieldClause(String fieldName, String fieldValue) {
                return addFieldClause(fieldName, fieldValue, MatchType.TERM, Occurance.MUST_OCCUR);
            }

            /**
             * Add a {@link Occurance#MUST_OCCUR} clause which matches a top level field name using
             * {@link MatchType#TERM}.
             * @param fieldName the top level field name.
             * @param fieldValue the field value to match.
             * @return a reference to this object.
             */
            public Builder addFieldClause(String fieldName, Object fieldValue) {
                return addFieldClause(fieldName,
                        QuerySpecification.toMatchValue(fieldValue),
                        MatchType.TERM,
                        Occurance.MUST_OCCUR);
            }

            /**
             * Add a {@link Occurance#MUST_OCCUR} clause which matches a top level field name with the provided
             * {@link MatchType}
             * @param fieldName the top level field name.
             * @param fieldValue the field value to match.
             * @param matchType the match type.
             * @return a reference to this object.
             */
            public Builder addFieldClause(String fieldName, String fieldValue, MatchType matchType) {
                return addFieldClause(fieldName, fieldValue, matchType, Occurance.MUST_OCCUR);
            }

            /**
             * Add a clause which matches a top level field name using {@link MatchType#TERM} with the provided
             * {@link Occurance}.
             * @param fieldName the top level field name.
             * @param fieldValue the field value to match.
             * @param occurance the {@link Occurance} for this clause.
             * @return a reference to this object.
             */
            public Builder addFieldClause(String fieldName, String fieldValue, Occurance occurance) {
                return addFieldClause(fieldName, fieldValue, MatchType.TERM, occurance);
            }

            /**
             * Add a clause which matches a top level field name with the provided {@link MatchType} and
             * {@link Occurance}.
             * @param fieldName the top level field name.
             * @param fieldValue the field value to match.
             * @param matchType the match type.
             * @param occurance the {@link Occurance} for this clause.
             * @return a reference to this object.
             */
            public Builder addFieldClause(String fieldName, String fieldValue, MatchType matchType,
                    Occurance occurance) {
                Query clause = new Query()
                        .setTermPropertyName(fieldName)
                        .setTermMatchValue(fieldValue)
                        .setTermMatchType(matchType);
                clause.occurance = occurance;
                this.query.addBooleanClause(clause);
                return this;
            }

            /**
             * Add a clause which matches a {@link com.vmware.dcp.services.common.QueryTask.NumericRange} for a given numeric field.
             * @param fieldName the top level numeric field name.
             * @param range a numeric range.
             * @return a reference to this object.
             */
            public Builder addRangeClause(String fieldName, NumericRange<?> range) {
                Query clause = new Query()
                        .setTermPropertyName(fieldName)
                        .setNumericRange(range);
                this.query.addBooleanClause(clause);
                return this;
            }

            /**
             * Add a clause to this query.
             * @param clause a clause.
             * @return a reference to this object.
             */
            public Builder addClause(Query clause) {
                this.query.addBooleanClause(clause);
                return this;
            }

            public Builder addClauses(Query clause1, Query clause2) {
                this.query.addBooleanClause(clause1)
                        .addBooleanClause(clause2);
                return this;
            }

            public Builder addClauses(Query clause1, Query clause2, Query clause3) {
                this.query.addBooleanClause(clause1)
                        .addBooleanClause(clause2)
                        .addBooleanClause(clause3);
                return this;
            }

            public Builder addClauses(Query firstClause, Query... otherClauses) {
                this.query.addBooleanClause(firstClause);
                for (Query clause : otherClauses) {
                    this.query.addBooleanClause(clause);
                }
                return this;
            }

            /**
             * Return the constructed {@link com.vmware.dcp.services.common.QueryTask.Query} object.
             * @return the query object.
             */
            public Query build() {
                return this.query;
            }
        }

        public Occurance occurance = Occurance.MUST_OCCUR;

        /**
         * A single term definition.
         *
         * The {@code booleanClauses} property must be null if this property is specified.
         */
        public QueryTerm term;

        /**
         * A boolean query definition, composed out multiple sub queries.
         *
         * The {@code term} property must be null if this property is specified.
         */
        public List<Query> booleanClauses;

        public Query setTermPropertyName(String name) {
            allocateTerm();
            this.term.propertyName = name;
            return this;
        }

        public Query setTermMatchValue(String matchValue) {
            allocateTerm();
            this.term.matchValue = matchValue;
            return this;
        }

        public Query setTermMatchType(MatchType matchType) {
            allocateTerm();
            this.term.matchType = matchType;
            return this;
        }

        public Query setNumericRange(NumericRange<?> range) {
            allocateTerm();
            this.term.range = range;
            return this;
        }

        private void allocateTerm() {
            if (this.term != null) {
                return;
            }
            this.term = new QueryTerm();
        }

        public Query addBooleanClause(Query clause) {
            if (this.booleanClauses == null) {
                this.booleanClauses = new ArrayList<>();
                this.term = null;
            }
            this.booleanClauses.add(clause);
            return this;
        }
    }

    public TaskState taskInfo = new TaskState();

    /**
     * Describes the query
     */
    public QuerySpecification querySpec;

    /**
     * Describes any post processing on the query results (such summations, averages) The generation
     * of a time series is also a post processing
     */
    public PostProcessingSpecification postProcessingSpec;

    public ServiceDocumentQueryResult results;

    /**
     * The index service to query documents for. Unless otherwise specified, we default to the
     * document index.
     */
    public String indexLink = ServiceUriPaths.CORE_DOCUMENT_INDEX;

    public static QueryTask create(QuerySpecification q) {
        QueryTask qt = new QueryTask();
        qt.querySpec = q;
        return qt;
    }

    public QueryTask setDirect(boolean enable) {
        this.taskInfo.isDirect = enable;
        return this;
    }

    /**
     * Rfc7519Builder class for constructing {@linkplain com.vmware.dcp.services.common.QueryTask query tasks}.
     */
    public static class Builder {
        private final QueryTask queryTask;
        private final QuerySpecification querySpec;

        private Builder(boolean isDirect) {
            this.queryTask = new QueryTask();
            this.querySpec = new QuerySpecification();
            this.queryTask.querySpec = this.querySpec;
            this.queryTask.taskInfo.isDirect = isDirect;
        }

        /**
         * Constructs an asynchronous query task.
         * @return a reference to this object.
         */
        public static Builder create() {
            return new Builder(false);
        }

        /**
         * Constructs a synchronous query task.
         * @return a reference to this object.
         */
        public static Builder createDirectTask() {
            return new Builder(true);
        }

        /**
         * Set the maximum number of results to return.
         * @param resultLimit the result limit.
         * @return a reference to this object.
         */
        public Builder setResultLimit(int resultLimit) {
            this.querySpec.resultLimit = resultLimit;
            return this;
        }

        /**
         * Set the expected number of results.
         * @param expectedResultCount the expected result count.
         * @return a reference to this object.
         */
        public Builder setExpectedResultCount(long expectedResultCount) {
            this.querySpec.expectedResultCount = expectedResultCount;
            return this;
        }

        /**
         * Order results in ascending order by the given {@code fieldName}.
         * @param fieldName the field name to order results by.
         * @param fieldType the field type.
         * @return a reference to this object.
         */
        public Builder orderAscending(String fieldName, TypeName fieldType) {
            return order(fieldName, fieldType, SortOrder.ASC);
        }

        /**
         * Order results in descending order by the given {@code fieldName}.
         * @param fieldName the field name to order results by.
         * @param fieldType the field type.
         * @return a reference to this object.
         */
        public Builder orderDescending(String fieldName, TypeName fieldType) {
            return order(fieldName, fieldType, SortOrder.DESC);
        }

        private Builder order(String fieldName, TypeName fieldType, SortOrder sortOrder) {
            QueryTerm sortTerm = new QueryTerm();
            sortTerm.propertyName = fieldName;
            sortTerm.propertyType = fieldType;
            this.querySpec.sortTerm = sortTerm;
            this.querySpec.sortOrder = sortOrder;
            return this;
        }

        /**
         * Set {@linkplain com.vmware.dcp.services.common.QueryTask.QuerySpecification.QueryOption query options}.
         * @param queryOptions the set of query options.
         * @return a reference to this object.
         */
        public Builder setOptions(EnumSet<QueryOption> queryOptions) {
            this.querySpec.options = queryOptions;
            return this;
        }

        /**
         * Set the {@link com.vmware.dcp.services.common.QueryTask.Query} for this task.
         * @param query the query to execute.
         * @return a reference to this object.
         */
        public Builder setQuery(Query query) {
            this.querySpec.query = query;
            return this;
        }

        /**
         * Set the index service to query for. Defaults to
         * {@link ServiceUriPaths#CORE_DOCUMENT_INDEX}
         * @param indexLink the index service link.
         * @return a reference to this object.
         */
        public Builder setIndexLink(String indexLink) {
            this.queryTask.indexLink = indexLink;
            return this;
        }

        /**
         * Set the {@link com.vmware.dcp.services.common.QueryTask.PostProcessingSpecification} which will be executed on the query results.
         * @param spec the post processing specification.
         * @return a reference to this object.
         */
        public Builder setPostProcessingSpec(PostProcessingSpecification spec) {
            this.queryTask.postProcessingSpec = spec;
            return this;
        }

        /**
         * Return the constructed {@link com.vmware.dcp.services.common.QueryTask} object.
         * @return the query task object.
         */
        public QueryTask build() {
            return this.queryTask;
        }
    }
}
