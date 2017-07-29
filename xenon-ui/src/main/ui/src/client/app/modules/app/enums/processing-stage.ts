export enum ProcessingStage {
    /**
     * Service object is instantiated. This is the initial stage
     */
    CREATED,

    /**
     * Service is attached to service host
     */
    INITIALIZING,

    /**
     * If the service is durable, and state was available in the document store, it has been
     * loaded and made available in the initial post
     */
    LOADING_INITIAL_STATE,

    /**
     * Synchronizing with peers
     */
    SYNCHRONIZING,

    /**
     * Service handleCreate is invoked. Runtime proceeds when the create Operation
     * is completed
     */
    EXECUTING_CREATE_HANDLER,

    /**
     * Service handleStart is invoked. Runtime proceeds when the start Operation
     * is completed
     */
    EXECUTING_START_HANDLER,

    /**
     * Initial state has been indexed
     */
    INDEXING_INITIAL_STATE,

    /**
     * Service is ready for operation processing. Any operations received while in the STARTED
     * or INITIALIZED stage will be dequeued.
     */
    AVAILABLE,

    /**
     * Service is paused due to memory pressure. Its detached from the service host and its
     * runtime context is persisted to disk.
     */
    PAUSED,

    /**
     * Service is stopped and its resources have been released
     */
    STOPPED
}
