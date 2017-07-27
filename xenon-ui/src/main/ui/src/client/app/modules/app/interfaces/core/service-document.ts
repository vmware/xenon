export interface ServiceDocument {
    /**
     * Monotonically increasing number indicating the number of updates the local child service
     * has processed in the current host session or since creation, if the service is durable
     *
     * Infrastructure use only
     */
    documentVersion?: number;

    /**
     * Document update time in microseconds since UNIX epoch
     *
     * Infrastructure use only
     */
    documentUpdateTimeMicros?: number;

    /**
     * Expiration time in microseconds since UNIX epoch. If a document is found to be expired a
     * running child service will be deleted and the document will be marked deleted in the index
     */
    documentExpirationTimeMicros?: number;

    /**
     * Identity of the node that is assigned this document. Assignment is done through the node
     * group partitioning service and it can change between versions
     *
     * Infrastructure use only
     */
    documentOwner?: string;

    /**
     * The relative URI path of the service managing this document
     */
    documentSelfLink?: string;

    /**
     * Link to the document that served as the source for this document. The source document is
     * retrieved during a logical clone operation when this instance was created through a POST to a
     * service factory
     */
    documentSourceLink?: string;

    /**
     * A structured string identifier for the document PODO type
     *
     * Infrastructure use only
     */
    documentKind?: string;

    /**
     * Monotonically increasing number associated with a document owner. Each time ownership
     * changes a protocol guarantees new owner will assign a higher epoch number and get consensus
     * between peers before proceeding with replication.
     *
     * This field is not used for non replicated services
     *
     * Infrastructure use only
     */
    documentEpoch?: number;

    /**
     * Refers to the transaction coordinator: if this particular state version is part of a
     * pending transaction whose coordinator is `/core/transactions/[txid]`, it lets the system
     * hide this version from queries, concurrent transactions and operations -- if they don't
     * explicitly provide this id.
     */
    documentTransactionId?: string;

    /**
     * The relative URI path of the user principal who owns this document
     *
     * Infrastructure use only
     */
    documentAuthPrincipalLink?: string;

    /**
     * Update action that produced the latest document version
     *
     * Infrastructure use only
     */
    documentUpdateAction?: string;
}
