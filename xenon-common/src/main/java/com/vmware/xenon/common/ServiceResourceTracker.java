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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;

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
     * Tracks if a factory link ever had one of its children paused
     */
    private final ConcurrentSkipListSet<String> serviceFactoriesUnderMemoryPressure = new ConcurrentSkipListSet<>();

    /**
     * Tracks cached service state. Cleared periodically during maintenance
     */
    private final ConcurrentMap<String, ServiceDocument> cachedServiceStates = new ConcurrentHashMap<>();

    /**
     * Tracks last access time for PERSISTENT services. The access time is used for a few things:
     * 1. Deciding if the cache state of the service needs to be cleared based
     *    on {@link ServiceHost#getServiceCacheClearDelayMicros()}.
     * 2. Deciding if the service needs to be stopped or paused when memory pressure is high.
     *
     * We don't bother tracking access time for StatefulServices that are non-persistent.
     * This is because the cached state for non-persistent stateful services is never cleared and
     * they do not get paused.
     */
    private final ConcurrentMap<String, Long> persistedServiceLastAccessTimes = new ConcurrentHashMap<>();

    /**
     * Tracks cached service state. Cleared periodically during maintenance
     */
    private final ConcurrentMap<CachedServiceStateKey, ServiceDocument> cachedTransactionalServiceStates = new ConcurrentHashMap<>();

    private final ServiceHost host;

    private boolean isServiceStateCaching = true;

    private long startTimeMicros;

    public static ServiceResourceTracker create(ServiceHost host, Map<String, Service> services,
            Map<String, Service> pendingPauseServices) {
        ServiceResourceTracker srt = new ServiceResourceTracker(host, services,
                pendingPauseServices);
        return srt;
    }

    public ServiceResourceTracker(ServiceHost host, Map<String, Service> services,
            Map<String, Service> pendingPauseServices) {
        this.attachedServices = services;
        this.host = host;
    }

    private void checkAndInitializeStats() {
        if (this.startTimeMicros > 0) {
            return;
        }
        this.startTimeMicros = Utils.getNowMicrosUtc();
        if (this.host.getManagementService() == null) {
            this.host.log(Level.WARNING, "Management service not found, stats will not be available");
            return;
        }

        long inUseMem = this.host.getState().systemInfo.totalMemoryByteCount
                - this.host.getState().systemInfo.freeMemoryByteCount;
        long freeMem = this.host.getState().systemInfo.maxMemoryByteCount - inUseMem;
        long freeDisk = this.host.getState().systemInfo.freeDiskByteCount;

        createTimeSeriesStat(
                ServiceHostManagementService.STAT_NAME_AVAILABLE_MEMORY_BYTES_PREFIX,
                freeMem);

        createTimeSeriesStat(
                ServiceHostManagementService.STAT_NAME_AVAILABLE_DISK_BYTES_PREFIX,
                freeDisk);

        createTimeSeriesStat(
                ServiceHostManagementService.STAT_NAME_CPU_USAGE_PCT_PREFIX,
                0);

        // guess initial thread count
        createTimeSeriesStat(
                ServiceHostManagementService.STAT_NAME_THREAD_COUNT_PREFIX,
                Utils.DEFAULT_THREAD_COUNT);
    }

    private void createTimeSeriesStat(String name, double v) {
        createDayTimeSeriesStat(name, v);
        createHourTimeSeriesStat(name, v);
    }

    private void createDayTimeSeriesStat(String name, double v) {
        Service mgmtService = this.host.getManagementService();
        ServiceStat st = new ServiceStat();
        st.name = name + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        st.timeSeriesStats = new TimeSeriesStats((int) TimeUnit.DAYS.toHours(1),
                TimeUnit.HOURS.toMillis(1),
                EnumSet.of(AggregationType.AVG));
        mgmtService.setStat(st, v);
    }

    private void createHourTimeSeriesStat(String name, double v) {
        Service mgmtService = this.host.getManagementService();
        ServiceStat st = new ServiceStat();
        st.name = name + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        st.timeSeriesStats = new TimeSeriesStats((int) TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MINUTES.toMillis(1),
                EnumSet.of(AggregationType.AVG));
        mgmtService.setStat(st, v);
    }

    private void updateStats(long now) {
        SystemHostInfo shi = this.host.updateSystemInfo(false);
        Service mgmtService = this.host.getManagementService();
        checkAndInitializeStats();

        // The JVM reports free memory in a indirect way, relative to the current "total". But the
        // true free memory is the estimated used memory subtracted from the JVM heap max limit
        long freeMemory = shi.maxMemoryByteCount
                - (shi.totalMemoryByteCount - shi.freeMemoryByteCount);
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_AVAILABLE_MEMORY_BYTES_PER_HOUR,
                freeMemory);
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_AVAILABLE_MEMORY_BYTES_PER_DAY,
                freeMemory);
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_AVAILABLE_DISK_BYTES_PER_HOUR,
                shi.freeDiskByteCount);
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_AVAILABLE_DISK_BYTES_PER_DAY,
                shi.freeDiskByteCount);

        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        if (!threadBean.isCurrentThreadCpuTimeSupported()) {
            return;
        }

        long totalTime = 0;
        // we assume a low number of threads since the runtime uses just a thread per core, plus
        // a small multiple of that dedicated to I/O threads. So the thread CPU usage calculation
        // should have a small overhead
        long[] threadIds = threadBean.getAllThreadIds();
        for (long threadId : threadIds) {
            totalTime += threadBean.getThreadCpuTime(threadId);
        }

        double runningTime = now - this.startTimeMicros;
        if (runningTime <= 0) {
            return;
        }

        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_THREAD_COUNT_PER_DAY,
                threadIds.length);
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_THREAD_COUNT_PER_HOUR,
                threadIds.length);

        totalTime = TimeUnit.NANOSECONDS.toMicros(totalTime);
        double pctUse = totalTime / runningTime;
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_CPU_USAGE_PCT_PER_HOUR,
                pctUse);
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_CPU_USAGE_PCT_PER_DAY,
                pctUse);
    }

    public void setServiceStateCaching(boolean enable) {
        this.isServiceStateCaching = enable;
    }

    public void updateCachedServiceState(Service s,
             ServiceDocument st, Operation op) {

        if (ServiceHost.isServiceIndexed(s) && !isTransactional(op)) {
            this.persistedServiceLastAccessTimes.put(s.getSelfLink(), Utils.getNowMicrosUtc());
        }

        // if caching is disabled on the serviceHost, then we don't bother updating the cache
        // for persisted services. If it's a non-persisted service, then we DO update the cache.
        if (ServiceHost.isServiceIndexed(s) && !this.isServiceStateCaching) {
            return;
        }

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

    /**
     * called only for stateful services
     */
    public ServiceDocument getCachedServiceState(Service s, Operation op) {
        String servicePath = s.getSelfLink();
        ServiceDocument state = null;

        if (isTransactional(op)) {
            CachedServiceStateKey key = new CachedServiceStateKey(servicePath,
                    op.getTransactionId());
            state = this.cachedTransactionalServiceStates.get(key);
        } else {
            if (ServiceHost.isServiceIndexed(s)) {
                this.persistedServiceLastAccessTimes.put(servicePath,
                        this.host.getStateNoCloning().lastMaintenanceTimeUtcMicros);
            }
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
        // Issue DELETE to stop the service
        Operation deleteExp = Operation.createDelete(s.getUri())
                .disableFailureLogging(true)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                .setReplicationDisabled(true)
                .setReferer(this.host.getUri())
                .setCompletion((o, e) -> {
                    if (s.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                        this.host.getManagementService()
                                        .adjustStat(
                                                ServiceHostManagementService.STAT_NAME_ODL_STOP_COUNT,
                                                1);
                    }
                });

        this.host.sendRequest(deleteExp);

        clearCachedServiceState(servicePath, op);
    }

    public void clearCachedServiceState(String servicePath, Operation op) {
        this.clearCachedServiceState(servicePath, op, false);
    }

    private void clearCachedServiceState(String servicePath, Operation op, boolean keepLastAccessTime) {

        if (!isTransactional(op)) {
            if (!keepLastAccessTime) {
                this.persistedServiceLastAccessTimes.remove(servicePath);
            }

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
                ServiceHostManagementService.STAT_NAME_SERVICE_CACHE_CLEAR_COUNT, 1);
        if (s.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
            this.host.getManagementService().adjustStat(
                    ServiceHostManagementService.STAT_NAME_ODL_CACHE_CLEAR_COUNT, 1);
        }
    }

    /**
     * Estimates how much memory is used by host caches, queues and based on the memory limits
     * takes appropriate action: clears cached service state, temporarily stops services
     */
    public void performMaintenance(long now, long deadlineMicros) {
        updateStats(now);
        ServiceHostState hostState = this.host.getStateNoCloning();

        int pauseServiceCount = 0;
        for (Service service : this.attachedServices.values()) {
            // skip factory services, they do not have state
            if (service.hasOption(ServiceOption.FACTORY)) {
                continue;
            }

            ServiceDocument s = this.cachedServiceStates.get(service.getSelfLink());
            Long lastAccessTime = this.persistedServiceLastAccessTimes.get(service.getSelfLink());
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

                if (lastAccessTime == null) {
                    lastAccessTime = s.documentUpdateTimeMicros;
                }

                if ((hostState.serviceCacheClearDelayMicros + lastAccessTime) < now) {
                    // The cached entry is old and should be cleared.
                    // Note that we are not going to clear the lastAccessTime here
                    // because we will need it in future maintenance runs to determine
                    // if the service should be paused/ stopped.
                    clearCachedServiceState(service.getSelfLink(), null, true);
                    cacheCleared = true;
                }
            }

            // If neither the cache nor the lastAccessTime maps contain an entry
            // for the Service, and it's a PERSISTENT service, then probably the
            // service is just starting up. Skipping service pause...
            if (lastAccessTime == null && ServiceHost.isServiceIndexed(service)) {
                continue;
            }

            if (lastAccessTime != null &&
                    hostState.lastMaintenanceTimeUtcMicros - lastAccessTime < service
                    .getMaintenanceIntervalMicros() * 2) {
                // Skip pause for services that have been active within a maintenance interval
                continue;
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

            if (!service.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                continue;
            }

            if (this.host.isServiceStarting(service, service.getSelfLink())) {
                continue;
            }

            if (this.host.hasPendingServiceAvailableCompletions(service.getSelfLink())) {
                this.host.log(Level.INFO,
                        "Pending available completions, skipping pause/stop on %s",
                        service.getSelfLink());
                continue;
            }

            boolean hasSoftState = hasServiceSoftState(service);
            if (cacheCleared && !hasSoftState) {
                // if it's an on-demand-load service with no subscribers or stats,
                // instead of pausing it, simply stop them when the service is idle.
                // if the on-demand-load service does have subscribers/stats, then continue with
                // pausing so that we don't lose any "soft" state
                stopService(service.getSelfLink(), false, null);
                continue;
            }

            if (!cacheCleared) {
                // if we're going to pause it, clear state from cache if not already cleared
                clearCachedServiceState(service.getSelfLink(), null);
                // and check again if ON_DEMAND_LOAD with no subscriptions, then we need to stop
                if (!hasSoftState) {
                    stopService(service.getSelfLink(), false, null);
                    continue;
                }
            }

            String factoryPath = UriUtils.getParentPath(service.getSelfLink());
            if (factoryPath != null) {
                this.serviceFactoriesUnderMemoryPressure.add(factoryPath);
            }

            pauseServiceCount++;
            pauseService(service);

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

        this.host.log(Level.FINE,
                "Attempt pause on %d services, attached: %d, cached: %d, persistedServiceLastAccessTimes: %d",
                pauseServiceCount, hostState.serviceCount,
                this.cachedServiceStates.size(),
                this.persistedServiceLastAccessTimes.size());
    }

    private void pauseService(Service s) {
        if (this.host.isStopping()) {
            return;
        }

        ServiceHostState hostState = this.host.getStateNoCloning();

        if (s.getProcessingStage() != ProcessingStage.AVAILABLE) {
            return;
        }

        String path = s.getSelfLink();
        CompletionHandler indexCompletion = (o, e) -> {
            if (e != null) {
                resumeService(path, s);
                abortPause(s, path, e);
                return;
            }

            synchronized (hostState) {
                if (null != this.attachedServices.remove(path)) {
                    hostState.serviceCount--;
                }
            }
            this.persistedServiceLastAccessTimes.remove(path);
            this.host.getManagementService().adjustStat(
                    ServiceHostManagementService.STAT_NAME_SERVICE_PAUSE_COUNT, 1);
        };

        try {
            // Atomically pause and serialize into a context we can store in the index
            ServiceRuntimeContext src = s.setProcessingStage(ProcessingStage.PAUSED);
            // Pause was successful, issue indexing request
            Operation indexPut = Operation
                    .createPut(this.host, ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX)
                    .setReferer(this.host.getUri())
                    .setBodyNoCloning(src)
                    .setCompletion(indexCompletion);
            this.host.sendRequest(indexPut);
        } catch (Throwable e) {
            abortPause(s, path, e);
        }
    }

    void abortPause(Service s, String path, Throwable e) {
        if (this.host.isStopping()) {
            return;
        }
        if (e != null && !(e instanceof CancellationException)) {
            this.host.log(Level.WARNING,
                    "Failure pausing service %s: %s",
                    path,
                    e.toString());
        }
        Operation op = s.dequeueRequest();
        if (op != null) {
            this.host.handleRequest(null, op);
        }
        this.host.processPendingServiceAvailableOperations(s, null, false);
    }

    boolean checkAndResumeService(Operation inboundOp) {
        String key = inboundOp.getUri().getPath();
        if (ServiceHost.isHelperServicePath(key)) {
            key = UriUtils.getParentPath(key);
        }

        String factoryPath = UriUtils.getParentPath(key);
        FactoryService factoryService = null;
        if (factoryPath != null) {
            factoryService = (FactoryService)this.host.findService(factoryPath);
        }

        if (factoryService != null
                && !this.serviceFactoriesUnderMemoryPressure.contains(factoryPath)) {
            // minor optimization: if the service factory has never experienced a pause for one of the child
            // services, do not bother querying the blob index. A node might never come under memory
            // pressure so this lookup avoids the index query.
            if (factoryService.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
                inboundOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);
            } else {
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
            if (inboundOp.getExpirationMicrosUtc() < Utils.getNowMicrosUtc()) {
                this.host.log(Level.WARNING, "Request to %s has expired", path);
                return false;
            }

            if (this.host.isStopping()) {
                return false;
            }

            this.host.log(Level.WARNING, "(%d) ODL check for %s", inboundOp.getId(), path);
            return this.host.checkAndOnDemandStartService(inboundOp, factoryService);
        }

        inboundOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);
        OperationContext inputContext = OperationContext.getOperationContext();
        Operation resumePut = Operation.createPut(this.host,
                ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX);
        resumePut.setBodyNoCloning(ServiceRuntimeContext.create(key));
        resumePut.setCompletion((o, e) -> {
            OperationContext.setFrom(inputContext);
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

            // the service context index resume the service before completing the GET, and deleting
            // the index entry for the paused service.

            // re-submit the operation that caused the resume
            this.host.handleRequest(null, inboundOp);
        });
        resumePut.setAuthorizationContext(this.host.getSystemAuthorizationContext());
        this.host.sendRequest(resumePut.setReferer(this.host.getUri()));
        return true;
    }

    void resumeService(String path, Service resumedService) {
        if (this.host.isStopping()) {
            return;
        }
        resumedService.setHost(this.host);

        ServiceHostState hostState = this.host.getStateNoCloning();
        synchronized (hostState) {
            if (!this.attachedServices.containsKey(path)) {
                this.attachedServices.put(path, resumedService);
                hostState.serviceCount++;
            }
        }

        resumedService.setProcessingStage(ProcessingStage.AVAILABLE);

        if (ServiceHost.isServiceIndexed(resumedService)) {
            this.persistedServiceLastAccessTimes.put(
                    resumedService.getSelfLink(), Utils.getNowMicrosUtc());
        }

        this.host.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_SERVICE_RESUME_COUNT, 1);
    }

    public void close() {
        this.cachedServiceStates.clear();
        this.persistedServiceLastAccessTimes.clear();
    }

    private boolean isTransactional(Operation op) {
        return op != null && op.getTransactionId() != null
                && this.host.getTransactionServiceUri() != null;
    }

    private boolean hasServiceSoftState(Service service) {
        if (!service.hasOption(ServiceOption.ON_DEMAND_LOAD)) {
            return false;
        }

        UtilityService subUtilityService = (UtilityService) service
                .getUtilityService(ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS);
        UtilityService statsUtilityService = (UtilityService) service
                .getUtilityService(ServiceHost.SERVICE_URI_SUFFIX_STATS);
        boolean hasSoftState = false;
        if (subUtilityService != null && subUtilityService.hasSubscribers()) {
            hasSoftState = true;
        }
        if (statsUtilityService != null && statsUtilityService.hasStats()) {
            hasSoftState = true;
        }
        return hasSoftState;
    }
}
