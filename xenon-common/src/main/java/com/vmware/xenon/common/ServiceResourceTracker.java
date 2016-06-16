/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceHost.ServiceHostState.MemoryLimitType;
import com.vmware.xenon.services.common.ServiceContextIndexService;

/**
 * Monitors service resources, and takes action, during periodic maintenance
 */
class ServiceResourceTracker {

    /**
     * This class is used for keeping cached transactional state of services under
     * active optimistic transactions.
     */
    static class CachedServiceStateKey {
        private String servicePath;
        private String transactionId;

        CachedServiceStateKey(String servicePath, String transactionId) {
            this.servicePath = servicePath;
            this.transactionId = transactionId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.servicePath, this.transactionId);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }

            if (o instanceof CachedServiceStateKey) {
                CachedServiceStateKey that = (CachedServiceStateKey) o;
                return Objects.equals(this.servicePath, that.servicePath)
                        && Objects.equals(this.transactionId, that.transactionId);
            }

            return false;
        }

        @Override
        public String toString() {
            return String.format("CachedServiceStateKey{servicePath: %s, transactionId: %s}",
                    this.servicePath, this.transactionId);
        }
    }

    /**
     * For performance reasons, this map is owned and directly operated by the host
     */
    private final Map<String, Service> attachedServices;

    /**
     * For performance reasons, this map is owned and directly operated by the service host
     */
    private final Map<String, Service> pendingPauseServices;

    /**
     * Tracks if a factory link ever had one of its children paused
     */
    private final ConcurrentSkipListSet<String> serviceFactoriesUnderMemoryPressure = new ConcurrentSkipListSet<>();

    /**
     * Tracks cached service state. Cleared periodically during maintenance
     */
    private final Map<String, ServiceDocument> cachedServiceStates = new ConcurrentHashMap<>();

    /**
     * Tracks cached service state. Cleared periodically during maintenance
     */
    private final Map<CachedServiceStateKey, ServiceDocument> cachedTransactionalServiceStates = new ConcurrentHashMap<>();

    private final ServiceHost host;

    public static ServiceResourceTracker create(ServiceHost host, Map<String, Service> services,
            Map<String, Service> pendingPauseServices) {
        ServiceResourceTracker srt = new ServiceResourceTracker(host, services,
                pendingPauseServices);
        return srt;
    }

    public ServiceResourceTracker(ServiceHost host, Map<String, Service> services,
            Map<String, Service> pendingPauseServices) {
        this.attachedServices = services;
        this.pendingPauseServices = pendingPauseServices;
        this.host = host;
    }

    public void updateCachedServiceState(Service s, ServiceDocument st, Operation op) {
        if (!isTransactional(op)) {
            synchronized (s.getSelfLink()) {
                ServiceDocument cachedState = this.cachedServiceStates.put(s.getSelfLink(), st);
                if (cachedState != null && cachedState.documentVersion > st.documentVersion) {
                    // restore cached state, discarding update, if the existing version is higher
                    this.cachedServiceStates.put(s.getSelfLink(), cachedState);
                }
            }
            return;
        }

        CachedServiceStateKey key = new CachedServiceStateKey(s.getSelfLink(),
                op.getTransactionId());
        synchronized (key.toString()) {
            ServiceDocument cachedState = this.cachedTransactionalServiceStates.put(key, st);
            if (cachedState != null && cachedState.documentVersion > st.documentVersion) {
                // restore cached state, discarding update, if the existing version is higher
                this.cachedTransactionalServiceStates.put(key, cachedState);
            }
        }
    }

    public ServiceDocument getCachedServiceState(String servicePath, Operation op) {
        ServiceDocument state = null;
        if (isTransactional(op)) {
            CachedServiceStateKey key = new CachedServiceStateKey(servicePath,
                    op.getTransactionId());
            state = this.cachedTransactionalServiceStates.get(key);
        }

        if (state == null) {
            // either the operational is not transactional or no transactional state found -
            // look for the state in the non-transactional map
            state = this.cachedServiceStates.get(servicePath);
        }

        if (state == null) {
            return null;
        }

        if (state.documentExpirationTimeMicros > 0
                && state.documentExpirationTimeMicros < state.documentUpdateTimeMicros) {
            // state expired, clear from cache
            stopService(servicePath, true, op);
            return null;
        }

        return state;
    }

    private void stopService(String servicePath, boolean isExpired, Operation op) {
        Service s = this.host.findService(servicePath, true);
        if (s == null) {
            return;
        }
        if (isExpired && s.hasOption(ServiceOption.PERSISTENCE)) {
            // the index service tracks expiration of persisted services
            return;
        }
        // Issue DELETE to stop the service. The PRAGMA is not required, since
        // we know the service is in memory only, but we are using it to maintain
        // symmetry with the expiration induced deletes from the index
        Operation deleteExp = Operation.createDelete(s.getUri())
                .disableFailureLogging(true)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                .setReplicationDisabled(true)
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (s.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                        this.host.getManagementService()
                                .adjustStat(Service.STAT_NAME_ODL_STOP_COUNT, 1);
                    }
                });
        this.host.sendRequest(deleteExp);

        clearCachedServiceState(servicePath, op);
    }

    public void clearCachedServiceState(String servicePath, Operation op) {
        if (!isTransactional(op)) {
            ServiceDocument doc = this.cachedServiceStates.remove(servicePath);
            Service s = this.host.findService(servicePath, true);
            if (s == null) {
                return;
            }
            if (doc != null) {
                updateCacheClearStats(s);
            }
            return;
        }

        clearTransactionalCachedServiceState(servicePath, op.getTransactionId());
    }

    public void clearTransactionalCachedServiceState(String servicePath, String transactionId) {
        CachedServiceStateKey key = new CachedServiceStateKey(servicePath,
                transactionId);
        ServiceDocument doc = this.cachedTransactionalServiceStates.remove(key);
        Service s = this.host.findService(servicePath, true);
        if (s == null) {
            return;
        }
        if (doc != null) {
            updateCacheClearStats(s);
        }
    }

    private void updateCacheClearStats(Service s) {
        s.adjustStat(Service.STAT_NAME_CACHE_CLEAR_COUNT, 1);
        this.host.getManagementService().adjustStat(
                Service.STAT_NAME_SERVICE_CACHE_CLEAR_COUNT, 1);
        if (s.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
            this.host.getManagementService().adjustStat(
                    Service.STAT_NAME_ODL_CACHE_CLEAR_COUNT, 1);
        }
    }

    /**
     * Estimates how much memory is used by host caches, queues and based on the memory limits
     * takes appropriate action: clears cached service state, temporarily stops services
     */
    public void performMaintenance(long now, long deadlineMicros) {
        ServiceHostState hostState = this.host.getStateNoCloning();
        long memoryLimitLowMB = this.host.getServiceMemoryLimitMB(ServiceHost.ROOT_PATH,
                MemoryLimitType.HIGH_WATERMARK);

        long memoryInUseMB = hostState.serviceCount
                * ServiceHost.DEFAULT_SERVICE_INSTANCE_COST_BYTES;
        memoryInUseMB /= (1024 * 1024);

        boolean shouldPause = memoryLimitLowMB <= memoryInUseMB;

        int pauseServiceCount = 0;
        for (Service service : this.attachedServices.values()) {
            // skip factory services, they do not have state
            if (service.hasOption(ServiceOption.FACTORY)) {
                continue;
            }

            ServiceDocument s = this.cachedServiceStates.get(service.getSelfLink());
            boolean cacheCleared = s == null;

            if (s != null) {
                if (!ServiceHost.isServiceIndexed(service)) {
                    // we do not clear cache or stop in memory services but we do check expiration
                    if (s.documentExpirationTimeMicros > 0
                            && s.documentExpirationTimeMicros < now) {
                        stopService(service.getSelfLink(), true, null);
                    }
                    continue;
                }

                if (service.hasOption(ServiceOption.TRANSACTION_PENDING)) {
                    // don't clear cache for services under active transactions, for perf reasons.
                    // transactional cached state will be cleared at the end of transaction
                    continue;
                }

                if ((hostState.serviceCacheClearDelayMicros + s.documentUpdateTimeMicros) < now) {
                    clearCachedServiceState(service.getSelfLink(), null);
                    cacheCleared = true;
                }

                if (hostState.lastMaintenanceTimeUtcMicros
                        - s.documentUpdateTimeMicros < service
                                .getMaintenanceIntervalMicros() * 2) {
                    // Skip pause for services that have been active within a maintenance interval
                    continue;
                }
            }

            // we still want to clear a cache for periodic services, so check here, after the cache clear
            if (service.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
                // Services with periodic maintenance stay resident, for now. We might stop them in the future
                // if they have long periods
                continue;
            }

            if (!service.hasOption(ServiceOption.FACTORY_ITEM)) {
                continue;
            }

            if (this.host.isServiceStarting(service, service.getSelfLink())) {
                continue;
            }

            if (cacheCleared && service.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                // instead of pausing on demand load services, simply stop them when they idle
                stopService(service.getSelfLink(), false, null);
                continue;
            }

            if (!shouldPause) {
                continue;
            }

            if (!cacheCleared) {
                // if we're going to pause it, clear state from cache if not already cleared
                clearCachedServiceState(service.getSelfLink(), null);
                // and check again if ON_DEMAND_LOAD then we need to stop
                if (service.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                    // instead of pausing on demand load services, simply stop them when they idle
                    stopService(service.getSelfLink(), false, null);
                    continue;
                }
            }

            Service existing = this.pendingPauseServices.put(service.getSelfLink(), service);
            if (existing == null) {
                pauseServiceCount++;
            }

            String factoryPath = UriUtils.getParentPath(service.getSelfLink());
            if (factoryPath != null) {
                this.serviceFactoriesUnderMemoryPressure.add(factoryPath);
            }

            if (deadlineMicros < Utils.getNowMicrosUtc()) {
                break;
            }
        }

        if (pauseServiceCount == 0) {
            return;
        }

        // Make sure our service count matches the list contents, they could drift. Using size()
        // on a concurrent data structure is costly so we do this only when pausing services
        synchronized (hostState) {
            hostState.serviceCount = this.attachedServices.size();
        }

        pauseServices();
    }

    private void pauseServices() {
        if (this.host.isStopping()) {
            return;
        }

        ServiceHostState hostState = this.host.getStateNoCloning();
        int servicePauseCount = 0;
        for (Service s : this.pendingPauseServices.values()) {
            if (s.getProcessingStage() != ProcessingStage.AVAILABLE) {
                continue;
            }

            try {
                s.setProcessingStage(ProcessingStage.PAUSED);
            } catch (Throwable e) {
                this.host.log(Level.INFO, "Failure setting stage to %s for %s: %s",
                        ProcessingStage.PAUSED,
                        s.getSelfLink(), e.getMessage());
                continue;
            }
            servicePauseCount++;
            String path = s.getSelfLink();

            // ask object index to store service object. It should be tiny since services
            // should hold no instanced fields. We avoid service stop/start by doing this
            this.host.sendRequest(ServiceContextIndexService.createPost(this.host, path, s)
                    .setReferer(this.host.getUri()).setCompletion((o, e) -> {
                        if (e != null && !this.host.isStopping()) {
                            this.host.log(Level.WARNING, "Failure indexing service for pause: %s",
                                    Utils.toString(e));
                            resumeService(path, s);
                            return;
                        }

                        Service serviceEntry = this.pendingPauseServices.remove(path);
                        if (serviceEntry == null && !this.host.isStopping()) {
                            this.host.log(Level.INFO, "aborting pause for %s", path);
                            resumeService(path, s);
                            // this means service received a request and is active. Its OK, the index will have
                            // a stale entry that will get deleted next time we query for this self link.
                            this.host.processPendingServiceAvailableOperations(s, null, false);
                            return;
                        }

                        synchronized (hostState) {
                            if (null != this.attachedServices.remove(path)) {
                                hostState.serviceCount--;
                            }
                        }
                        this.host.getManagementService().adjustStat(Service.STAT_NAME_SERVICE_PAUSE_COUNT, 1);
                    }));
        }
        this.host.log(Level.INFO, "Paused %d services, attached: %d, cached: %d", servicePauseCount,
                hostState.serviceCount, this.cachedServiceStates.size());
    }

    private void resumeService(String path, Service resumedService) {
        if (this.host.isStopping()) {
            return;
        }
        resumedService.setHost(this.host);
        resumedService.setProcessingStage(ProcessingStage.AVAILABLE);
        ServiceHostState hostState = this.host.getStateNoCloning();
        synchronized (hostState) {
            if (!this.attachedServices.containsKey(path)) {
                this.attachedServices.put(path, resumedService);
                hostState.serviceCount++;
            }
        }
    }

    boolean checkAndResumePausedService(Operation inboundOp) {
        String key = inboundOp.getUri().getPath();
        if (ServiceHost.isHelperServicePath(key)) {
            key = UriUtils.getParentPath(key);
        }

        String factoryPath = UriUtils.getParentPath(key);
        Service factoryService = null;
        if (factoryPath != null) {
            factoryService = this.host.findService(factoryPath);
        }
        if (factoryService != null
                && !this.serviceFactoriesUnderMemoryPressure.contains(factoryPath)) {
            if (!factoryService.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                // minor optimization: if the service factory has never experienced a pause for one of the child
                // services, do not bother querying the blob index. A node might never come under memory
                // pressure so this lookup avoids the index query.
                return false;
            }
        }

        String path = key;

        if (factoryService == null) {
            this.host.failRequestServiceNotFound(inboundOp);
            return true;
        }

        if (this.host.isStopping()
                && inboundOp.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                && inboundOp.getAction() == Action.DELETE) {
            // do not attempt to resume services if they are paused or in the process of being paused
            inboundOp.complete();
            return true;
        }

        if (inboundOp.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)) {
            Service service = this.pendingPauseServices.remove(path);
            if (service != null) {
                // Abort pause
                this.host.log(Level.INFO, "Aborting service pause for %s", path);
                resumeService(path, service);
                return false;
            }

            if (inboundOp.getExpirationMicrosUtc() < Utils.getNowMicrosUtc()) {
                this.host.log(Level.WARNING, "Request to %s has expired", path);
                return false;
            }

            if (this.host.isStopping()) {
                return false;
            }

            service = this.attachedServices.get(path);
            if (service != null && service.getProcessingStage() == ProcessingStage.PAUSED) {
                this.host.log(Level.INFO, "Service attached, but paused, aborting pause for %s",
                        path);
                resumeService(path, service);
                return false;
            }

            long pendingPauseCount = this.pendingPauseServices.size();
            if (pendingPauseCount == 0) {
                return this.host.checkAndOnDemandStartService(inboundOp, factoryService);
            }

            // there is a small window between pausing a service, and the service being indexed in the
            // blob store, where an operation coming in might find the service missing from the blob index and from
            // attachedServices map.
            this.host.schedule(
                    () -> {
                        this.host.log(Level.INFO,
                                "Retrying index lookup for %s, pending pause: %d",
                                path, pendingPauseCount);
                        checkAndResumePausedService(inboundOp);
                    }, 1, TimeUnit.SECONDS);
            return true;
        }

        inboundOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);

        Operation query = ServiceContextIndexService
                .createGet(this.host, path)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.log(Level.WARNING,
                                        "Failure checking if service paused: " + Utils.toString(e));
                                this.host.handleRequest(null, inboundOp);
                                return;
                            }

                            if (!o.hasBody()) {
                                // service is not paused
                                this.host.handleRequest(null, inboundOp);
                                return;
                            }

                            Service resumedService = (Service) o.getBodyRaw();
                            resumeService(path, resumedService);

                            this.host.getManagementService().adjustStat(Service.STAT_NAME_SERVICE_RESUME_COUNT, 1);
                            this.host.handleRequest(null, inboundOp);
                        });

        this.host.sendRequest(query.setReferer(this.host.getUri()));
        return true;
    }

    public void close() {
        this.pendingPauseServices.clear();
        this.cachedServiceStates.clear();
    }

    private boolean isTransactional(Operation op) {
        return op != null && op.getTransactionId() != null
                && this.host.getTransactionServiceUri() != null;
    }

}
