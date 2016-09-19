import { QuerySpecification, ServiceDocument, TaskState } from '../index';

export interface QueryTask extends ServiceDocument {
    /**
     * A list of authorization context links which can access this service.
     */
    tenantLinks?: string[];

    /**
     * Task state
     */
    taskInfo: TaskState;

    /**
     * Describes the query
     */
    querySpec: QuerySpecification;

    /**
     * Query results
     */
    results?: any;

    /**
     * The index service to query documents for. Unless otherwise specified, we default to the
     * document index.
     */
    indexLink?: string;

    /**
     * The node selector to use when QueryOption#BROADCAST is set
     */
    nodeSelectorLink?: string;
}
