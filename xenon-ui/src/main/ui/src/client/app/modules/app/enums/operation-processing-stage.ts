export enum OperationProcessingStage {
    /**
     * Loading state and linking it to the operation
     */
    LOADING_STATE,

    /**
     * Processing operation processing chain filters
     */
    PROCESSING_FILTERS,

    /**
     * Executing service handler
     */
    EXECUTING_SERVICE_HANDLER
}
