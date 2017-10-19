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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceClient.ConnectionPoolMetrics;
import com.vmware.xenon.common.ServiceHost.ServiceHostState;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Monitors service resources, and takes action, during periodic maintenance
 */
public class ServiceResourceTracker {

    /**
     * This class is used for keeping cached transactional state of services under
     * active optimistic transactions.
     */
    private static final class CachedServiceStateKey {
        private final String servicePath;
        private final String transactionId;

        CachedServiceStateKey(String servicePath, String transactionId) {
            this.servicePath = servicePath;
            this.transactionId = transactionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CachedServiceStateKey that = (CachedServiceStateKey) o;

            if (this.servicePath != null ? !this.servicePath.equals(that.servicePath) : that.servicePath != null) {
                return false;
            }

            return this.transactionId != null ?
                    this.transactionId.equals(that.transactionId) :
                    that.transactionId == null;
        }

        @Override
        public int hashCode() {
            int result = this.servicePath != null ? this.servicePath.hashCode() : 0;
            result = 31 * result + (this.transactionId != null ? this.transactionId.hashCode() : 0);
            return result;
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
     * Tracks cached service state. Cleared periodically during maintenance
     */
    private final ConcurrentMap<String, ServiceDocument> cachedServiceStates = new ConcurrentHashMap<>();

    /**
     * Tracks last access time for PERSISTENT services. The access time is used for a few things:
     * 1. Deciding if the cache state of the service needs to be cleared based
     *    on {@link ServiceHost#getServiceCacheClearDelayMicros()}.
     * 2. Deciding if the service needs to be stopped when memory pressure is high.
     *
     * We don't bother tracking access time for StatefulServices that are non-persistent.
     * This is because the cached state for non-persistent stateful services is never cleared.
     */
    private final ConcurrentMap<String, Long> persistedServiceLastAccessTimes = new ConcurrentHashMap<>();

    /**
     * Tracks cached service state. Cleared periodically during maintenance
     */
    private final ConcurrentMap<CachedServiceStateKey, ServiceDocument> cachedTransactionalServiceStates = new ConcurrentHashMap<>();

    private final ServiceHost host;

    private boolean isServiceStateCaching = true;

    private long startTimeMicros;

    private ThreadMXBean threadBean;

    private Service mgmtService;

    public static ServiceResourceTracker create(ServiceHost host, Map<String, Service> services) {
        ServiceResourceTracker srt = new ServiceResourceTracker(host, services);
        return srt;
    }

    public ServiceResourceTracker(ServiceHost host, Map<String, Service> services) {
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

        createTimeSeriesStat(
                ServiceHostManagementService.STAT_NAME_HTTP11_CONNECTION_COUNT_PREFIX,
                0);

        createTimeSeriesStat(
                ServiceHostManagementService.STAT_NAME_HTTP2_CONNECTION_COUNT_PREFIX,
                0);

        getManagementService().setStat(ServiceHostManagementService.STAT_NAME_THREAD_COUNT,
                Utils.DEFAULT_THREAD_COUNT);

    }

    void createTimeSeriesStat(String name, double v) {
        Service service = getManagementService();
        EnumSet<AggregationType> types = EnumSet.of(AggregationType.AVG);
        ServiceStat dayStat = ServiceStatUtils.getOrCreateDailyTimeSeriesStat(service, name, types);
        ServiceStat hourStat = ServiceStatUtils.getOrCreateHourlyTimeSeriesHistogramStat(service, name, types);
        service.setStat(dayStat, v);
        service.setStat(hourStat, v);
    }

    private void updateStats(long now) {
        this.host.updateMemoryAndDiskInfo();
        ServiceHostState hostState = this.host.getStateNoCloning();
        SystemHostInfo shi = hostState.systemInfo;

        Service mgmtService = getManagementService();
        if (mgmtService == null) {
            return;
        }

        checkAndInitializeStats();
        mgmtService.setStat(ServiceHostManagementService.STAT_NAME_SERVICE_COUNT,
                hostState.serviceCount);

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

        if (this.threadBean == null) {
            this.threadBean = ManagementFactory.getThreadMXBean();
        }
        if (!this.threadBean.isCurrentThreadCpuTimeSupported()) {
            return;
        }

        long totalTime = 0;
        // we assume a low number of threads since the runtime uses just a thread per core, plus
        // a small multiple of that dedicated to I/O threads. So the thread CPU usage calculation
        // should have a small overhead
        long[] threadIds = this.threadBean.getAllThreadIds();
        for (long threadId : threadIds) {
            totalTime += this.threadBean.getThreadCpuTime(threadId);
        }

        double runningTime = now - this.startTimeMicros;
        if (runningTime <= 0) {
            return;
        }

        createTimeSeriesStat(ServiceHostManagementService.STAT_NAME_JVM_THREAD_COUNT_PREFIX,
                threadIds.length);

        totalTime = TimeUnit.NANOSECONDS.toMicros(totalTime);
        double pctUse = totalTime / runningTime;
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_CPU_USAGE_PCT_PER_HOUR,
                pctUse);
        mgmtService.setStat(
                ServiceHostManagementService.STAT_NAME_CPU_USAGE_PCT_PER_DAY,
                pctUse);

        ConnectionPoolMetrics http11TagInfo = this.host.getClient()
                .getConnectionPoolMetrics(false);
        if (http11TagInfo != null) {
            createTimeSeriesStat(
                    ServiceHostManagementService.STAT_NAME_HTTP11_PENDING_OP_COUNT_PREFIX,
                    http11TagInfo.pendingRequestCount);
            createTimeSeriesStat(
                    ServiceHostManagementService.STAT_NAME_HTTP11_CONNECTION_COUNT_PREFIX,
                    http11TagInfo.inUseConnectionCount);
        }

        ConnectionPoolMetrics http2TagInfo = this.host.getClient()
                .getConnectionPoolMetrics(true);
        if (http2TagInfo != null) {
            createTimeSeriesStat(
                    ServiceHostManagementService.STAT_NAME_HTTP2_PENDING_OP_COUNT_PREFIX,
                    http2TagInfo.pendingRequestCount);
            createTimeSeriesStat(
                    ServiceHostManagementService.STAT_NAME_HTTP2_CONNECTION_COUNT_PREFIX,
                    http2TagInfo.inUseConnectionCount);
        }

        ForkJoinPool executor = (ForkJoinPool) this.host.getExecutor();
        if (executor != null) {
            createTimeSeriesStat(
                    ServiceHostManagementService.STAT_NAME_EXECUTOR_QUEUE_DEPTH,
                    executor.getQueuedSubmissionCount());
        }

        ScheduledThreadPoolExecutor scheduledExecutor = (ScheduledThreadPoolExecutor) this.host.getScheduledExecutor();
        if (scheduledExecutor != null) {
            createTimeSeriesStat(
                    ServiceHostManagementService.STAT_NAME_SCHEDULED_EXECUTOR_QUEUE_DEPTH,
                    scheduledExecutor.getQueue().size());
        }
    }

    private Service getManagementService() {
        if (this.mgmtService == null) {
            this.mgmtService = this.host.getManagementService();
        }
        return this.mgmtService;
    }

    public void setServiceStateCaching(boolean enable) {
        this.isServiceStateCaching = enable;
    }

    public void resetCachedServiceState(Service s, ServiceDocument st, Operation op) {
        updateCachedServiceState(s, st, op, false);
    }

    public void updateCachedServiceState(Service s, ServiceDocument st, Operation op) {
        updateCachedServiceState(s, st, op, true);
    }

    private void updateCachedServiceState(Service s, ServiceDocument st, Operation op, boolean checkVersion) {
        if (ServiceHost.isServiceIndexed(s) && !isTransactional(op)) {
            this.persistedServiceLastAccessTimes.put(s.getSelfLink(), Utils.getNowMicrosUtc());
        }

        // we cache the state only if:
        // (1) the service is non-persistent, or:
        // (2) the service is persistent, the operation is not replicated and state caching is enabled
        boolean cacheState = !ServiceHost.isServiceIndexed(s);
        cacheState |= !op.isFromReplication() && this.isServiceStateCaching;

        if (!cacheState) {
            return;
        }

        if (!isTransactional(op)) {
            synchronized (s.getSelfLink()) {
                ServiceDocument cachedState = this.cachedServiceStates.put(s.getSelfLink(), st);
                if (checkVersion && cachedState != null && cachedState.documentVersion > st.documentVersion) {
                    // restore cached state, discarding update, if the existing version is higher
                    this.cachedServiceStates.put(s.getSelfLink(), cachedState);
                }
            }
            return;
        }

        CachedServiceStateKey key = new CachedServiceStateKey(s.getSelfLink(),
                op.getTransactionId());

        this.cachedTransactionalServiceStates.compute(key, (k, cachedState) -> {
            if (cachedState != null && cachedState.documentVersion > st.documentVersion) {
                // No update if the existing version is higher
                return cachedState;
            } else {
                return st;
            }
        });
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
        }

        if (state == null) {
            // either the operation is not transactional or no transactional state found -
            // look for the state in the non-transactional map
            state = this.cachedServiceStates.get(servicePath);
        }

        if (state == null) {
            updateCacheMissStats();
            return null;
        }

        if (!ServiceHost.isServiceIndexed(s) &&
                state.documentExpirationTimeMicros > 0 &&
                state.documentExpirationTimeMicros < Utils.getNowMicrosUtc()) {
            // service is not persistent and expired - stop and clear it from cache
            // (persistent services expiration is checked by the index service)
            stopServiceAndClearFromCache(s, state);
            return null;
        }

        if (ServiceHost.isServiceIndexed(s) && !isTransactional(op)) {
            this.persistedServiceLastAccessTimes.put(servicePath,
                    Utils.getNowMicrosUtc());
        }

        updateCacheHitStats();
        return state;
    }

    private void stopServiceAndClearFromCache(Service s, ServiceDocument state) {
        // Issue DELETE to stop the service and clear it from cache
        Operation deleteExp = Operation.createDelete(this.host, s.getSelfLink())
                .setBody(state)
                .disableFailureLogging(true)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING)
                .setReferer(this.host.getUri());

        this.host.sendRequest(deleteExp);
    }

    public void clearCachedServiceState(Service s, Operation op) {
        String servicePath = s.getSelfLink();

        if (!isTransactional(op)) {
            this.persistedServiceLastAccessTimes.remove(servicePath);

            ServiceDocument doc = this.cachedServiceStates.remove(servicePath);
            if (doc != null) {
                updateCacheClearStats();
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
            updateCacheClearStats();
        }
    }

    public void updateCacheMissStats() {
        this.host.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_SERVICE_CACHE_MISS_COUNT, 1);
    }

    private void updateCacheClearStats() {
        this.host.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_SERVICE_CACHE_CLEAR_COUNT, 1);
    }

    private void updateCacheHitStats() {
        this.host.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_SERVICE_CACHE_HIT_COUNT, 1);
    }

    /**
     * Estimates how much memory is used by host caches, queues and based on the memory limits
     * takes appropriate action: clears cached service state, temporarily stops services
     */
    public void performMaintenance(long now, long deadlineMicros) {
        updateStats(now);
        ServiceHostState hostState = this.host.getStateNoCloning();
        int stopServiceCount = 0;

        for (Service service : this.attachedServices.values()) {
            ServiceDocument state = this.cachedServiceStates.get(service.getSelfLink());

            if (!ServiceHost.isServiceIndexed(service)) {
                // service is not persistent - just check if it's expired, and
                // if so - stop and clear from cache
                if (state != null &&
                        state.documentExpirationTimeMicros > 0 &&
                        state.documentExpirationTimeMicros < now) {
                    stopServiceAndClearFromCache(service, state);
                }
                continue;
            }

            // service is persistent - check whether it's exempt from cache
            // eviction (e.g. it has soft state)
            if (serviceExemptFromCacheEviction(service)) {
                continue;
            }

            Long lastAccessTime = this.persistedServiceLastAccessTimes.get(service.getSelfLink());
            if (serviceActive(lastAccessTime, service, now)) {
                // Skip stop for services that have been recently active
                continue;
            }

            // stop service and update count
            stopServiceAndClearFromCache(service, state);
            stopServiceCount++;

            if (deadlineMicros < Utils.getSystemNowMicrosUtc()) {
                break;
            }
        }

        if (hostState.serviceCount < 0) {
            // Make sure our service count matches the list contents, they could drift. Using size()
            // on a concurrent data structure is costly so we do this only when pausing services or
            // the count is negative
            synchronized (hostState) {
                hostState.serviceCount = this.attachedServices.size();
            }
        }

        if (stopServiceCount == 0) {
            return;
        }

        this.host.log(Level.FINE,
                "Attempt stop on %d services, attached: %d, cached: %d, persistedServiceLastAccessTimes: %d",
                stopServiceCount, hostState.serviceCount,
                this.cachedServiceStates.size(),
                this.persistedServiceLastAccessTimes.size());
    }

    private boolean serviceExemptFromCacheEviction(Service service) {
        if (service.hasOption(ServiceOption.FACTORY)) {
            // skip factory services, they do not have state
            return true;
        }

        if (service.hasOption(ServiceOption.TRANSACTION_PENDING)) {
            // don't clear cache for services under active transactions, for perf reasons.
            // transactional cached state will be cleared at the end of transaction
            return true;
        }

        if (service.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
            // Services with periodic maintenance stay resident, for now. We might stop them in the future
            // if they have long periods
            return true;
        }

        if (this.host.isServiceStarting(service, service.getSelfLink())) {
            // service is just starting - doesn't make sense to evict it from cache
            return true;
        }

        if (this.host.hasPendingServiceAvailableCompletions(service.getSelfLink())) {
            // service has pending available completions - keep in memory
            return true;
        }

        if (service.getSelfLink().startsWith(ServiceUriPaths.CORE_AUTHZ)) {
            // we keep authz resources (users, groups, roles, etc.) resident in memory
            // for perf reasons
            return true;
        }

        if (hasServiceSoftState(service)) {
            // service has soft state like subscriptions or stats - keep in memory
            return true;
        }

        // service is not exempt from cache eviction
        return false;
    }

    private boolean serviceActive(Long lastAccessTime, Service s, long now) {
        if (lastAccessTime == null) {
            return false;
        }

        long cacheClearDelayMicros = s.getCacheClearDelayMicros();
        if (ServiceHost.isServiceImmutable(s)) {
            cacheClearDelayMicros = 0;
        }

        boolean active = (cacheClearDelayMicros + lastAccessTime) > now;
        if (!active) {
            this.host.log(Level.FINE,
                    "Considering stopping service %s, isOwner: %b, because it was inactive for %f seconds",
                    s.getSelfLink(), this.host.isDocumentOwner(s),
                    TimeUnit.MICROSECONDS.toSeconds(now - lastAccessTime));
        }

        return active;
    }

    public boolean checkAndOnDemandStartService(Operation inboundOp) {
        String key = inboundOp.getUri().getPath();
        if (ServiceHost.isHelperServicePath(key)) {
            key = UriUtils.getParentPath(key);
        }

        String factoryPath = UriUtils.getParentPath(key);
        FactoryService factoryService = null;
        if (factoryPath != null) {
            Service parentService = this.host.findService(factoryPath);
            if (!(parentService instanceof FactoryService)) {
                Operation.failServiceNotFound(inboundOp,
                        ServiceErrorResponse.ERROR_CODE_SERVICE_PARENT_NOT_A_FACTORY,
                        "URI path appears invalid, parent is not a factory service");
                return true;
            }
            factoryService = (FactoryService) parentService;
        }

        if (factoryService == null) {
            Operation.failServiceNotFound(inboundOp);
            return true;
        }

        inboundOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);

        String path = key;

        if (this.host.isStopping()
                && inboundOp.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                && inboundOp.getAction() == Action.DELETE) {
            inboundOp.complete();
            return true;
        }

        if (inboundOp.getExpirationMicrosUtc() < Utils.getSystemNowMicrosUtc()) {
            this.host.log(Level.WARNING, "Request to %s has expired", path);
            return false;
        }

        if (this.host.isStopping()) {
            return false;
        }

        this.host.log(Level.FINE, "(%d) ODL check for %s", inboundOp.getId(), path);
        return checkAndOnDemandStartService(inboundOp, factoryService);
    }

    boolean checkAndOnDemandStartService(Operation inboundOp, Service parentService) {
        if (!parentService.hasOption(ServiceOption.FACTORY)) {
            Operation.failServiceNotFound(inboundOp);
            return true;
        }

        FactoryService factoryService = (FactoryService) parentService;

        String servicePath = inboundOp.getUri().getPath();
        if (ServiceHost.isHelperServicePath(servicePath)) {
            servicePath = UriUtils.getParentPath(servicePath);

        }
        String finalServicePath = servicePath;
        boolean doProbe = inboundOp.hasPragmaDirective(
                Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY);

        if (!factoryService.hasOption(ServiceOption.REPLICATION)
                && inboundOp.getAction() == Action.DELETE) {
            // do a probe (GET) to avoid starting a service on a DELETE request. We only do this
            // for non replicated services since its safe to do a local only probe. By doing a GET
            // first, we avoid the following race on local services:
            // DELETE -> starts service to determine if it exists
            // client issues POST for same self link while service is starting during ODL start
            // client sees conflict, even if the service never existed
            doProbe = true;
        }

        if (!doProbe) {
            this.host.log(Level.FINE, "Skipping probe - starting service %s on-demand due to %s %d (isFromReplication: %b, isSynchronizeOwner: %b, isSynchronizePeer: %b)",
                    finalServicePath, inboundOp.getAction(), inboundOp.getId(),
                    inboundOp.isFromReplication(), inboundOp.isSynchronizeOwner(), inboundOp.isSynchronizePeer());
            startServiceOnDemand(inboundOp, parentService, factoryService, finalServicePath);
            return true;
        }

        // we should not use startService for checking if a service ever existed. This can cause a race with
        // a client POST creating the service for the first time, when they use
        // PRAGMA_QUEUE_FOR_AVAILABILITY. Instead do an attempt to load state for the service path
        Operation getOp = Operation
                .createGet(inboundOp.getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)
                .transferRefererFrom(inboundOp)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        inboundOp.fail(e);
                        return;
                    }

                    if (!o.hasBody()) {
                        // the index will return success, but no body if service is not found
                        this.host.checkPragmaAndRegisterForAvailability(finalServicePath,
                                inboundOp);
                        return;
                    }

                    // service state exists, proceed with starting service
                    this.host.log(Level.FINE, "Starting service %s on-demand due to %s %d (isFromReplication: %b, isSynchronizeOwner: %b, isSynchronizePeer: %b)",
                            finalServicePath, inboundOp.getAction(), inboundOp.getId(),
                            inboundOp.isFromReplication(), inboundOp.isSynchronizeOwner(), inboundOp.isSynchronizePeer());
                    startServiceOnDemand(inboundOp, parentService, factoryService,
                            finalServicePath);
                });

        Service indexService = this.host.getDocumentIndexService();
        if (indexService == null) {
            inboundOp.fail(new CancellationException("Index service is null"));
            return true;
        }
        indexService.handleRequest(getOp);
        return true;
    }

    void startServiceOnDemand(Operation inboundOp, Service parentService,
            FactoryService factoryService, String finalServicePath) {
        Operation onDemandPost = Operation.createPost(this.host, finalServicePath);

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (e instanceof CancellationException) {
                    // local stop of idle service raced with client request to load it. Retry.
                    this.host.log(Level.WARNING, "Stop of idle service %s detected, retrying",
                            inboundOp
                            .getUri().getPath());
                    this.host.scheduleCore(() -> {
                        checkAndOnDemandStartService(inboundOp, parentService);
                    }, 1, TimeUnit.SECONDS);
                    return;
                }

                Action a = inboundOp.getAction();
                ServiceErrorResponse response = o.getErrorResponseBody();

                if (response != null) {
                    // Since we do a POST to start the service,
                    // we can get back a 409 status code i.e. the service has already been started or was
                    // deleted previously. Differentiate based on action, if we need to fail or succeed
                    if (response.statusCode == Operation.STATUS_CODE_CONFLICT) {
                        if (!ServiceHost.isServiceCreate(inboundOp)
                                && response.errorCode == ServiceErrorResponse.ERROR_CODE_SERVICE_ALREADY_EXISTS) {
                            // service exists, action is not attempt to recreate, so complete as success
                            this.host.log(Level.WARNING,
                                    "Failed to start service %s because it already exists. Resubmitting request %s %d",
                                    finalServicePath, inboundOp.getAction(), inboundOp.getId());
                            this.host.handleRequest(null, inboundOp);
                            return;
                        }

                        if (response.errorCode == ServiceErrorResponse.ERROR_CODE_STATE_MARKED_DELETED) {
                            if (a == Action.DELETE) {
                                // state marked deleted, and action is to delete again, return success
                                inboundOp.complete();
                            } else if (a == Action.POST) {
                                // POSTs will fail with conflict since we must indicate the client is attempting a restart of a
                                // existing service.
                                this.host.failRequestServiceAlreadyStarted(finalServicePath, null,
                                        inboundOp);
                            } else {
                                // All other actions fail with NOT_FOUND making it look like the service
                                // does not exist (or ever existed)
                                Operation.failServiceNotFound(inboundOp,
                                        ServiceErrorResponse.ERROR_CODE_STATE_MARKED_DELETED);
                            }
                            return;
                        }
                    }

                    // if the service we are trying to DELETE never existed, we swallow the 404 error.
                    // This is for consistency in behavior with services already resident in memory.
                    if (inboundOp.getAction() == Action.DELETE &&
                            response.statusCode == Operation.STATUS_CODE_NOT_FOUND) {
                        inboundOp.complete();
                        return;
                    }

                    // there is a possibility the user requests we queue and wait for service to show up
                    if (response.statusCode == Operation.STATUS_CODE_NOT_FOUND) {
                        this.host.log(Level.WARNING,
                                "Failed to start service %s with 404 status code.", finalServicePath);
                        this.host.checkPragmaAndRegisterForAvailability(finalServicePath,
                                inboundOp);
                        return;
                    }
                }

                this.host.log(Level.SEVERE,
                        "Failed to start service %s with statusCode %d",
                        finalServicePath, o.getStatusCode());
                inboundOp.setBodyNoCloning(o.getBodyRaw()).setStatusCode(o.getStatusCode());
                inboundOp.fail(e);
                return;
            }
            // proceed with handling original client request, service now started
            this.host.log(Level.FINE,
                    "Successfully started service %s. Resubmitting request %s %d",
                    finalServicePath, inboundOp.getAction(), inboundOp.getId());
            this.host.handleRequest(null, inboundOp);
        };

        onDemandPost.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK)
                .transferRefererFrom(inboundOp)
                .setExpiration(inboundOp.getExpirationMicrosUtc())
                .setReplicationDisabled(true)
                .setCompletion(c);
        if (inboundOp.isSynchronizeOwner()) {
            onDemandPost.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER);
        }

        Service childService;
        try {
            childService = factoryService.createServiceInstance();
            childService.toggleOption(ServiceOption.FACTORY_ITEM, true);
        } catch (Throwable e1) {
            inboundOp.fail(e1);
            return;
        }

        if (inboundOp.getAction() == Action.DELETE) {
            onDemandPost.disableFailureLogging(true);
            inboundOp.disableFailureLogging(true);
        }

        // bypass the factory, directly start service on host. This avoids adding a new
        // version to the index and various factory processes that are invoked on new
        // service creation
        this.host.startService(onDemandPost, childService);
    }

    void retryOnDemandLoadConflict(Operation op, Service s) {
        if (!ServiceHost.isServiceIndexed(s)) {
            // service is stopped but it's not persistent, so it doesn't start/stop on-demand.
            // no point in retrying the operation.
            op.fail(new CancellationException("Service has stopped"));
            return;
        }

        op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);

        this.host.log(Level.WARNING,
                "ODL conflict: retrying %s (%d %s) on %s",
                op.getAction(), op.getId(), op.getContextId(),
                op.getUri().getPath());

        long interval = Math.max(TimeUnit.SECONDS.toMicros(1),
                this.host.getMaintenanceIntervalMicros());
        this.host.scheduleCore(() -> {
            this.host.handleRequest(null, op);
        }, interval, TimeUnit.MICROSECONDS);
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
