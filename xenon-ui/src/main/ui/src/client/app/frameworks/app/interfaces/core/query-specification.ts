import { BooleanClause, QueryTerm } from '../index';

export interface QuerySpecification {
    /*
     * Query definition
     */
    query: BooleanClause;

    /**
     * A set of options that determine query behavior
     */
    options?: string[];

    /**
     * Property names of fields annotated with PropertyUsageOption.LINK. Used in combination with
     * QueryOption#SELECT_LINKS
     */
    linkTerms?: QueryTerm[];

    /**
     * Property name to use for primary sort. Used in combination with QueryOption#SORT
     */
    sortTerm?: QueryTerm;

    /**
     * Property name to use for group sort. Used in combination with QueryOption#GROUP_BY
     */
    groupSortTerm?: QueryTerm;

    /**
     * Property name to use for grouping. Used in combination with QueryOption#GROUP_BY
     */
    groupByTerm?: QueryTerm;

    /**
     * Primary sort order. Used in combination with QueryOption#SORT
     */
    sortOrder?: string;

    /**
     * Group sort order. Used in combination with QueryOption#GROUP_BY
     */
    groupSortOrder?: string;

    /**
     * Used for query results pagination. When specified,
     * the query task documentLinks and documents will remain empty, but when results are available
     * the nextPageLink field will be set. A client can then issue a GET request on the nextPageLink
     * to get the first page of results. If additional results are available, each result page will
     * have its nextPageLink set.
     */
    resultLimit?: number;

    /**
     * Used for grouped result pagination, limiting the number of entries in
     * ServiceDocumentQueryResult#nextPageLinksPerGroup
     */
    groupResultLimit?: number;

    /**
     * The query is retried until the result count matches the
     * specified value or the query expires.
     */
    expectedResultCount?: number;
}
