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

package com.vmware.xenon.common;

import java.net.URI;
import java.util.EnumSet;

import com.vmware.xenon.common.ServiceStats.ServiceStat;

public interface Service {
    enum Action {
        GET, POST, PATCH, PUT, DELETE, OPTIONS
    }

    /**
     * Service options. Service author toggles various options in the service constructor declaring
     * the requirements to the framework
     */
    enum ServiceOption {
        /**
         * Service runtime tracks statistics on operation completion and allows service instance and
         * external clients to track custom statistics, per instance. Statistics are available
         * through the /stats URI suffix, and served by an utility services associated with each
         * service instance. Statistics are not replicated but can be gathered across all instances
         * using broadcast GET requests.
         */
        INSTRUMENTATION,

        /**
         * Service runtime periodically invokes the handleMaintenance() handler making sure only one
         * maintenance operation is pending per service instance. If a maintenance operation is not
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
         * OWNER_SELECTION and ENFORCE_QUORUM options on how they affect replication.
         *
         */
        REPLICATION,

        /**
         * Service runtime performs a node selection process, per service, and forwards all updates
         * to the service instance on the selected node. Ownership is tracked in the indexed state
         * versions and remains fixed as long as the current owner is healthy. To enable scale out
         * only the service instance on the owner node performs work. All instances will receive the
         * updated state but the service handler is only invoked on the instance in the owner node.
         * This option causes replication to happen after the owner service has validated the update
         * and waits for quorum number of peers to accept the replicated state, before completing
         * the operation to the client. If less than quorum peers accept the update, the owner will
         * still proceed with committing the update and completion the operation successfully. To
         * enforce strict quorum and avoid value divergence see the {@code ENFORCE_QUORUM}
         * option
         *
         * Requires: REPLICATION
         *
         * Not compatible with: CONCURRENT_UPDATE_HANDLING
         */
        OWNER_SELECTION,

        /**
         * Modifies the replication protocol in a single way: Updates are committed on the owner, and
         * the client sees success on the operation only if quorum number of peers accept the updated
         * state. If the node group has been partitioned or multiple peers have failed, this option
         * makes the service unavailable, since no updates will be accepted. Enabling this option
         * provides strong consistency guarantees for service updates. It also affects availability,
         * since the service will fail all updates unless quorum or more nodes are available.
         *
         * Requires: REPLICATION, OWNER_SELECTION
         *
         * Not compatible with: CONCURRENT_UPDATE_HANDLING
         */
        ENFORCE_QUORUM,

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
         * Not compatible with: STRICT_UPDATE_CHECKING, PERSISTENCE, REPLICATION, ENFORCE_QUORUM
         */
        CONCURRENT_UPDATE_HANDLING,

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

        NONE
    }

    enum ProcessingStage {
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
         * Service is visible to other services (its URI is registered) and can process self
         * issued-operations and durable store has its state available for access. Operations issued
         * by other services or clients are queued
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
        STOPPED,

    }

    public enum OperationProcessingStage {
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

    String STAT_NAME_REQUEST_COUNT = "requestCount";
    String STAT_NAME_PRE_AVAILABLE_OP_COUNT = "preAvailableReceivedOperationCount";
    String STAT_NAME_FAILURE_COUNT = "failureCount";

    String STAT_NAME_REQUEST_OUT_OF_ORDER_COUNT = "requestOutOfOrderCount";
    String STAT_NAME_STATE_PERSIST_LATENCY = "statePersistLatencyMicros";
    String STAT_NAME_OPERATION_QUEUEING_LATENCY = "operationQueueingLatencyMicros";
    String STAT_NAME_SERVICE_HANDLER_LATENCY = "operationHandlerProcessingLatencyMicros";
    String STAT_NAME_OPERATION_DURATION = "operationDuration";
    String STAT_NAME_MAINTENANCE_COUNT = "maintenanceCount";
    String STAT_NAME_NODE_GROUP_CHANGE_MAINTENANCE_COUNT = "maintenanceForNodeGroupChangeCount";
    String STAT_NAME_NODE_GROUP_CHANGE_PENDING_MAINTENANCE_COUNT = "pendingMaintenanceForNodeGroupChangeCount";
    String STAT_NAME_MAINTENANCE_COMPLETION_DELAYED_COUNT = "maintenanceCompletionDelayedCount";
    String STAT_NAME_CACHE_MISS_COUNT = "stateCacheMissCount";
    String STAT_NAME_CACHE_CLEAR_COUNT = "stateCacheClearCount";
    String STAT_NAME_VERSION_CONFLICT_COUNT = "stateVersionConflictCount";
    String STAT_NAME_VERSION_IN_CONFLICT = "stateVersionInConflict";
    String STAT_NAME_PAUSE_COUNT = "pauseCount";
    String STAT_NAME_RESUME_COUNT = "resumeCount";

    /**
     * Estimate on run time context cost in bytes, per service instance. Services should not use instanced
     * fields, so, other than queuing context and utility service usage, the memory overhead should be small
     */
    int MAX_SERIALIZED_SIZE_BYTES = 8192;

    /**
     * Default operation queue limit
     */
    int OPERATION_QUEUE_DEFAULT_LIMIT = 10000;

    void handleStart(Operation startPost);

    /**
     * Infrastructure use. Invoked by host to determine if a request can be scheduled for processing
     * immediately, or if it was queued by the service.
     *
     * @return True if the request was queued or false if the request should be scheduled for
     *         processing immediately
     */
    boolean queueRequest(Operation op);

    /**
     * Infrastructure use. Invoked by host to retrieve a pending request.
     */
    Operation dequeueRequest();

    /**
     * Infrastructure use. Invoked by host to execute a service handler for a request
     */
    void handleRequest(Operation op);

    /**
     * Invoked by a service or an operation processing chain filter to process a request
     * starting at a specific stage
     */
    void handleRequest(Operation op, OperationProcessingStage opProcessingStage);

    /**
     * Sends a request using the default service client associated with the host
     */
    void sendRequest(Operation op);

    /**
     * Invoked by the utility service for requests to the service /config suffix
     */
    void handleConfigurationRequest(Operation request);

    /**
     * Invoked by the host periodically, if ServiceOption.PERIODIC_MAINTENANCE is set.
     *
     * An implementation of this method that needs to interact with the state of this service must
     * do so as if it were a client of this service. That is: the state of the service should be
     * retrieved by requesting a GET; and the state of the service should be mutated by submitting a
     * PATCH, PUT or DELETE.
     */
    void handleMaintenance(Operation post);

    void setMaintenanceIntervalMicros(long micros);

    long getMaintenanceIntervalMicros();

    ServiceHost getHost();

    String getSelfLink();

    URI getUri();

    OperationProcessingChain getOperationProcessingChain();

    ProcessingStage getProcessingStage();

    EnumSet<ServiceOption> getOptions();

    boolean hasOption(ServiceOption option);

    void toggleOption(ServiceOption option, boolean enable);

    /**
     * Sets the URI path to a node selector instance. A node selector service is associated with a node
     * group and picks the nodes eligible for replicating updates.
     *
     * The default node selector services uses a consistent hashing scheme and
     * picks among all available nodes.
     */
    void setPeerNodeSelectorPath(String link);

    /**
     * If replication is enabled, returns the URI path for the replication selector associated with the service
     */
    String getPeerNodeSelectorPath();

    ServiceStat getStat(String name);

    void adjustStat(String name, double delta);

    void adjustStat(ServiceStat stat, double delta);

    void setStat(String name, double newValue);

    void setStat(ServiceStat stat, double newValue);

    void setHost(ServiceHost serviceHost);

    void setSelfLink(String path);

    void setOperationProcessingChain(OperationProcessingChain opProcessingChain);

    void setProcessingStage(ProcessingStage initialized);

    ServiceDocument setInitialState(String jsonState, Long initialVersion);

    void setState(Operation op, ServiceDocument newState);

    <T extends ServiceDocument> T getState(Operation op);

    Service getUtilityService(String uriPath);

    Class<? extends ServiceDocument> getStateType();

    ServiceDocument getDocumentTemplate();

}
