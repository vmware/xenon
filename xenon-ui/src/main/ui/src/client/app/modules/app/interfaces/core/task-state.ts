export interface TaskState {
    /**
     * Current stage of the query
     */
    stage?: string;

    /**
     * Value indicating whether task should complete the creation POST only after its complete.
     * Client enables this at the risk of waiting for the POST and consuming a connection. It should
     * not be enabled for tasks that do long running I/O with other services
     */
    isDirect?: boolean;

    /**
     * Failure description for tasks that terminate in FAILED stage
     */
    failure?: any;

    /**
     * Duration of the query execution.
     */
    durationMicros?: number;
}
