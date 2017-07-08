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

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;

/**
 * Sequences service periodic maintenance
 */
class ServiceMaintenanceTracker {
    /**
     * Simple epsilon that is subtracted from absolute future expiration time. Maintenance
     * task scheduling is prone to JVM and OS thread scheduling variance, in addition to
     * misbehaving service handlers.
     * in the future, if more accuracy is required, a predictive scheme can be used
     */
    public static final long SCHEDULING_EPSILON_MICROS =
            TimeUnit.MILLISECONDS.toMicros(10);

    public static ServiceMaintenanceTracker create(ServiceHost host) {
        ServiceMaintenanceTracker smt = new ServiceMaintenanceTracker();
        smt.host = host;
        return smt;
    }

    private ServiceHost host;

    private ConcurrentHashMap<String, Long> trackedServices = new ConcurrentHashMap<>();
    private ConcurrentSkipListMap<Long, Set<String>> nextExpiration = new ConcurrentSkipListMap<>();

    public void schedule(Service s, long now) {
        long interval = s.getMaintenanceIntervalMicros();
        if (interval == 0) {
            interval = this.host.getMaintenanceIntervalMicros();
        }

        if (interval < this.host.getMaintenanceCheckIntervalMicros()) {
            this.host.setMaintenanceCheckIntervalMicros(interval);
        }

        long nextExpirationMicros = Math.max(now, now + interval - SCHEDULING_EPSILON_MICROS);
        String selfLink = s.getSelfLink();

        synchronized (this) {
            // To avoid double scheduling the same self-link
            // we lookup the self-link in our trackedServices map and remove
            // it before adding the new schedule.
            Long expiration = this.trackedServices.get(selfLink);
            if (expiration != null) {
                Set<String> services = this.nextExpiration.get(expiration);
                if (services != null) {
                    services.remove(selfLink);
                }
            }

            this.trackedServices.put(selfLink, nextExpirationMicros);
            Set<String> services = this.nextExpiration.get(nextExpirationMicros);
            if (services == null) {
                services = new HashSet<>();
                this.nextExpiration.put(nextExpirationMicros, services);
            }
            services.add(selfLink);
        }
    }

    public void performMaintenance(Operation op, long deadline) {
        long now;
        while ((now = Utils.getSystemNowMicrosUtc()) < deadline) {
            if (this.host.isStopping()) {
                op.fail(new CancellationException("Host is stopping"));
                return;
            }

            Entry<Long, Set<String>> e;

            // the nextExpiration map is a concurrent data structure, but since each value is a Set, we want
            // to make sure modifications to that Set are not lost if a concurrent add() is happening
            synchronized (this) {
                // get any services set to expire within the current maintenance interval
                e = this.nextExpiration.firstEntry();
                if (e == null || e.getKey() >= now) {
                    // no service requires maintenance, yet
                    return;
                }
                this.nextExpiration.pollFirstEntry();
            }

            Long expiration = e.getKey();
            Set<String> services = e.getValue();

            for (String servicePath : services) {
                Service s = this.host.findService(servicePath);

                boolean skipMaintenance =
                        (s == null) ||
                        (s.getProcessingStage() != ProcessingStage.AVAILABLE) ||
                        (!s.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) ||
                        (s.hasOption(ServiceOption.OWNER_SELECTION) &&
                                !s.hasOption(ServiceOption.DOCUMENT_OWNER));

                if (skipMaintenance) {
                    synchronized (this) {
                        // Another request scheduling this service's maintenance could
                        // have occurred. So double check the expiration time, if it
                        // matches the current expiration window, then remove it.
                        Long serviceExpiration = this.trackedServices.get(servicePath);
                        if (serviceExpiration.equals(expiration)) {
                            this.trackedServices.remove(servicePath);
                        }
                    }
                    continue;
                }

                performServiceMaintenance(servicePath, s);
            }
        }
    }

    private void performServiceMaintenance(String servicePath, Service s) {
        long[] start = new long[1];
        ServiceMaintenanceRequest body = ServiceMaintenanceRequest.create();
        body.reasons.add(MaintenanceReason.PERIODIC_SCHEDULE);

        Operation servicePost = Operation
                .createPost(UriUtils.buildUri(this.host, servicePath))
                .setReferer(this.host.getUri())
                .setBodyNoCloning(body)
                .setCompletion(
                        (o, ex) -> {
                            long now = Utils.getSystemNowMicrosUtc();
                            long actual = now - start[0];
                            long limit = Math.max(this.host.getMaintenanceIntervalMicros(),
                                    s.getMaintenanceIntervalMicros());
                            if (s.hasOption(ServiceOption.INSTRUMENTATION)) {
                                updateStats(s, actual, limit, servicePath);
                            }
                            // schedule again, for next maintenance interval
                            schedule(s, now);
                            if (ex != null) {
                                this.host.log(Level.WARNING, "Service %s failed maintenance: %s",
                                        servicePath, Utils.toString(ex));
                            }
                        });

        Runnable t = () -> {
            try {
                OperationContext.setAuthorizationContext(this.host
                        .getSystemAuthorizationContext());
                if (s.hasOption(Service.ServiceOption.INSTRUMENTATION)) {
                    s.adjustStat(Service.STAT_NAME_MAINTENANCE_COUNT, 1);
                }
                start[0] = Utils.getSystemNowMicrosUtc();
                s.handleMaintenance(servicePost);
            } catch (Exception ex) {
                // Mostly at this point, CompletionHandler for servicePost has already consumed in
                // "s.handleMaintenance()" and have set null (based on the handleMaintenance impl).
                // Calling fail() will not trigger any CompletionHandler, therefore explicitly
                // write log here as well.
                this.host.log(Level.WARNING, "Service %s failed to perform maintenance: %s",
                        servicePath, Utils.toString(ex));
                servicePost.fail(ex);
            }
        };

        if (s.hasOption(ServiceOption.CORE)) {
            this.host.scheduleCore(t, SCHEDULING_EPSILON_MICROS, TimeUnit.MICROSECONDS);
        } else {
            this.host.schedule(t, SCHEDULING_EPSILON_MICROS, TimeUnit.MICROSECONDS);
        }
    }

    public synchronized void close() {
        this.trackedServices.clear();
        this.nextExpiration.clear();
    }

    private void updateStats(Service s, long actual, long limit, String servicePath) {
        ServiceStats.ServiceStat durationStat = ServiceStatUtils.getOrCreateHistogramStat(s,
                Service.STAT_NAME_MAINTENANCE_DURATION);
        s.setStat(durationStat, actual);
        if (limit * 2 < actual) {
            this.host.log(Level.WARNING,
                    "Service %s exceeded maintenance interval %d. Actual: %d",
                    servicePath, limit, actual);
            s.adjustStat(
                    Service.STAT_NAME_MAINTENANCE_COMPLETION_DELAYED_COUNT, 1);
        }
    }
}
