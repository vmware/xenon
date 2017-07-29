import { ServiceDocument } from '../index';

export interface ServiceDocumentQueryResult extends ServiceDocument {
    /**
     * Collection of self links associated with each document found. The self link acts as the
     * primary key for a document.
     */
    documentLinks: string[];

    /**
     * If the query included QueryOption.EXPAND, this map populated with the JSON serialized service
     * state document associated with each link
     */
    documents?: {[key: string]: any};

    /**
     * If the query included QueryOption.SELECT_LINKS, this set is populated with the
     * unique link values, selected across all documents in the results. The {@link #selectedLinksPerDocument}
     * is structured around the document self link and link field names, so it will contain
     * keys with the same link value. This field, given it is a set, contains the unique values
     */
    selectedLinks?: string[];

    /**
     * If the query included QueryOption.SELECT_LINKS, this map is populated with the link
     * names and values, for each link in the results. For example, if the query results
     * include a document link /services/one, with a document that has a field "parentLink"
     * and value "/parents/two", this map will look like so:
     * { "selectedLinks" : { "/services/one" : {"parentLink" : "parents/two" } } }
     *
     * For fields that are collections of links, marked with PropertyUsageOption.LINKS, the map
     * will contain all of the collection items, prefixed by a unique identifier, like so:
     *  { "selectedLinks" : { "/services/one" : {
     *    "parentLinks.item.0" : "parents/two", "parentLinks.item.1" : "parents/three" }
     *   } }
     */
    selectedLinksPerDocument?: {[key: string]: any};

    /**
     * If the query included QueryOption.EXPAND_LINKS, this map is populated with the JSON
     * serialized service state for all unique selected link values.
     */
    selectedDocuments?: {[key: string]: string};

    /**
     * Set to the number of documents that satisfy the query.
     */
    documentCount?: number;

    /**
     * Valid only if QuerySpecification.resultLimit is specified.
     * In which case, a GET request to this link will retrieve the previous page of query results.
     * This link will expire roughly at the same time as the original QueryTask.
     */
    prevPageLink?: string;

    /**
     * Valid only if QuerySpecification.resultLimit is specified.
     * In which case, a GET request to this link will retrieve the next page of query results.
     * This link will expire roughly at the same time as the original QueryTask.
     */
    nextPageLink?: string;

    /**
     * Valid only on top level result for queries with QueryOption.GROUP_BY. Contains
     * query pages with the first set of results for each group value.
     */
    nextPageLinksPerGroup?: {[key: string]: string};

    /**
     * Duration of the query execution.
     */
    queryTimeMicros: number;
}
