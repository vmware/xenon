export enum ServiceOption {
    /**
     * Service runtime tracks statistics on operation completion and allows child service and
     * external clients to track custom statistics, per instance. Statistics are available
     * through the /stats URI suffix, and served by an utility services associated with each
     * child service. Statistics are not replicated but can be gathered across all instances
     * using broadcast GET requests.
     */
    INSTRUMENTATION,

    /**
     * Service runtime periodically invokes the handleMaintenance() handler making sure only one
     * maintenance operation is pending per child service. If a maintenance operation is not
     * complete by the next maintenance interval a warning is logged.
     */
    PERIODIC_MAINTENANCE,

    /**
     * Service runtime forwards the update state to the local document index service state. The
     * document index independently tracks multiple versions of the service state and indexes
     * fields using indexing options specified in the service document template (see
     * {@code getDocumentTemplate}
     */
    PERSISTENCE,

    /**
     * Service state updates are replicated among peer instances on other nodes. The default
     * replication group is used if no group is specified. Updates are replicated into phases
     * and use the appropriate protocol depending on other options.  See
     * OWNER_SELECTION on how it affects replication.
     *
     */
    REPLICATION,

    /**
     * Service runtime performs a node selection process, per service, and forwards all updates
     * to the child service on the selected node.
     *
     * This option enables the mechanism for strong consensus
     * and leader election.
     *
     * Ownership is tracked in the indexed state versions and remains fixed given a stable node
     * group. To enable scale out, only the child service on the owner node performs work.
     *
     * The runtime will route requests to the owner, regardless to which node receives a client
     * request.
     *
     * These service handlers are invoked only
     * on the child service on the owner node:
     * handleStart, handleMaintenance, handleGet, handleDelete, handlePut, handlePatch
     *
     * child services (replicas) on the other nodes will see replicated updates, as part of the
     * consensus protocol but there will be no service handler up-call.
     * Updates are committed on the owner, and the client sees success on the operation only
     * if quorum number of peers accept the updated state. If the node group has been partitioned
     * or multiple peers have failed, this option makes the service unavailable, since no updates
     * will be accepted.
     *
     * Requires: REPLICATION
     *
     * Not compatible with: CONCURRENT_UPDATE_HANDLING
     */
    OWNER_SELECTION,

    /**
     * Document update operations are conditional: the client must provide the expected
     * signature and/or version.
     *
     * If the service is durable and a signature is available in the current state, then the
     * request body must match the signature. The version is ignored.
     *
     * If there is no signature in the current state, then the version from the current state
     * must match the version in the request body.
     *
     * Requires: REPLICATION
     *
     * Not compatible with: CONCURRENT_UPDATE_HANDLING
     */
    STRICT_UPDATE_CHECKING,

    /**
     * Service runtime provides a HTML interactive UI through custom resource files associated
     * with the service class. The runtime serves the resource files from disk in response to
     * request to the /ui URI suffix
     */
    HTML_USER_INTERFACE,

    /**
     * Advanced option, not recommended.
     *
     * Service runtime disables local concurrency management and allows multiple update to be
     * processed concurrently. This should be used with great care since it does not compose
     * with most other options and can lead to inconsistent state updates. The default service
     * behavior serializes updates so only one update operation is logically pending. Service
     * handlers can issue asynchronous operation and exit immediately but the service runtime
     * still keeps other updates queued, until the operation is completed. GET operations are
     * allowed to execute concurrently with updates, using the latest committed version of the
     * service state
     *
     * Not compatible with: STRICT_UPDATE_CHECKING, PERSISTENCE, REPLICATION, OWNER_SELECTION
     */
    CONCURRENT_UPDATE_HANDLING,

    /**
     * Service runtime allows a GET to execute concurrently with updates.
     *
     * This option is enabled by default. Disabling this option helps in situations where,
     * for example, an operation processing filter reads the current state and then
     * conditionally updates it, relying on the state not being updated by some other
     * operation in the meantime.
     */
    CONCURRENT_GET_HANDLING,

    /**
     * Service factory will convert a POST to a PUT if a child service is already present, and
     * forward it to the service. The service must handle PUT requests and should perform
     * validation on the request body. The child service can enable STRICT_UPDATE_CHECKING to
     * prevent POSTs from modifying state unless the version and signature match
     */
    IDEMPOTENT_POST,

    /**
     * Runtime will load factory child services the first time a client attempts to access
     * them. Replication services might load due to synchronization, when joining node groups.
     *
     * Requires: FACTORY_ITEM (services created through factories)
     *
     */
    ON_DEMAND_LOAD,

    /**
     * Service will queue operation in last in first out order. If limit on operation queue is
     * exceeded, oldest operation in the queue will be cancelled to make room for the most
     * recent one
     */
    LIFO_QUEUE,

    /**
     * Service owns a portion of the URI name space for the service host. It can register for
     * a single URI path prefix and all requests that start with the prefix will be routed to
     * it. The service self link will be the prefix path, for the purpose of life cycle
     * REST operations
     *
     * Not compatible with: PERSISTENCE, REPLICATION
     *
     */
    URI_NAMESPACE_OWNER,

    /**
     * Set by runtime. Service is associated with another service providing functionality for
     * one of the utility REST APIs.
     */
    UTILITY,

    /**
     * Set by runtime. Service creates new instances of services through POST and uses queries
     * to return the active child services, on GET.
     */
    FACTORY,

    /**
     * Set by runtime. Service was created through a factory
     */
    FACTORY_ITEM,

    /**
     * Set by runtime. Service is currently assigned ownership of the replicated document. Any
     * work initiated through an update should only happen on this instance
     */
    DOCUMENT_OWNER,

    /**
     * Set by runtime. Service has one or more pending transactions.
     */
    TRANSACTION_PENDING,

    NONE
}
