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

package com.vmware.dcp.common;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;

import com.vmware.dcp.common.Service.ProcessingStage;
import com.vmware.dcp.common.Service.ServiceOption;
import com.vmware.dcp.common.ServiceMaintenanceRequest.MaintenanceReason;

/**
 * Sequences service periodic maintenance
 */
class ServiceHostMaintenanceTracker {
    public static ServiceHostMaintenanceTracker create(ServiceHost host) {
        ServiceHostMaintenanceTracker smt = new ServiceHostMaintenanceTracker();
        smt.host = host;
        return smt;
    }

    private ServiceHost host;
    private ConcurrentSkipListMap<Long, Set<String>> nextExpiration = new ConcurrentSkipListMap<>();

    public void schedule(Service s) {
        long interval = s.getMaintenanceIntervalMicros();
        if (interval == 0) {
            interval = this.host.getMaintenanceIntervalMicros();
        }

        long nextExpirationMicros = Utils.getNowMicrosUtc() + interval;
        synchronized (this) {
            Set<String> services = this.nextExpiration.get(nextExpirationMicros);
            if (services == null) {
                services = new HashSet<>();
                this.nextExpiration.put(nextExpirationMicros, services);
            }
            services.add(s.getSelfLink());
        }
    }

    public void performMaintenance(Operation op, long deadline) {
        while (Utils.getNowMicrosUtc() < deadline) {
            if (this.host.isStopping()) {
                op.fail(new CancellationException("Host is stopping"));
                return;
            }

            Set<String> services = null;

            // the nextExpiration map is a concurrent data structure, but since each value is a Set, we want
            // to make sure modifications to that Set are not lost if a concurrent add() is happening
            synchronized (this) {
                // get any services set to expire within the current maintenance interval
                Entry<Long, Set<String>> e = this.nextExpiration.firstEntry();
                if (e == null || e.getKey() > deadline) {
                    // no service requires maintenance, yet
                    return;
                }
                services = e.getValue();
                this.nextExpiration.remove(e.getKey());
            }

            for (String servicePath : services) {
                Service s = this.host.findService(servicePath);

                if (s == null) {
                    continue;
                }
                if (s.getProcessingStage() != ProcessingStage.AVAILABLE) {
                    continue;
                }

                if (!s.hasOption(ServiceOption.PERIODIC_MAINTENANCE)) {
                    // maintenance was disabled on this service
                    continue;
                }

                if (s.hasOption(ServiceOption.OWNER_SELECTION)
                        && !s.hasOption(ServiceOption.DOCUMENT_OWNER)) {
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
                .setBody(body)
                .setCompletion(
                        (o, ex) -> {

                            long actual = Utils.getNowMicrosUtc() - start[0];
                            long limit = Math.max(this.host.getMaintenanceIntervalMicros(),
                                    s.getMaintenanceIntervalMicros());

                            if (limit * 2 < actual) {
                                this.host.log(Level.WARNING,
                                        "Service %s exceeded maint. interval %d. Actual: %d",
                                        servicePath, limit, actual);
                                s.adjustStat(
                                        Service.STAT_NAME_MAINTENANCE_COMPLETION_DELAYED_COUNT, 1);
                            }

                            // schedule again, for next maintenance interval
                            schedule(s);
                            if (ex != null) {
                                this.host.log(Level.WARNING, "Service %s failed maintenance: %s",
                                        servicePath, Utils.toString(ex));
                            }
                        });
        this.host.run(() -> {
            try {
                OperationContext.setAuthorizationContext(this.host
                        .getSystemAuthorizationContext());
                if (s.hasOption(Service.ServiceOption.INSTRUMENTATION)) {
                    s.adjustStat(Service.STAT_NAME_MAINTENANCE_COUNT, 1);
                }
                start[0] = Utils.getNowMicrosUtc();
                s.handleMaintenance(servicePost);
            } catch (Throwable ex) {
                servicePost.fail(ex);
            }
        });
    }

    public synchronized void close() {
        this.nextExpiration.clear();
    }
}