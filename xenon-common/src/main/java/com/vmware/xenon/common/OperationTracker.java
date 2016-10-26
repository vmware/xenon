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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.vmware.xenon.common.Service.ProcessingStage;

/**
 * Performs periodic maintenance and expiration tracking on operations. Utilized by
 * service host for all operation related maintenance.
 */
class OperationTracker {
    public static ConcurrentSkipListSet<Operation> createOperationSet() {
        return new ConcurrentSkipListSet<>(new Comparator<Operation>() {
            @Override
            public int compare(Operation o1, Operation o2) {
                return Long.compare(o1.getId(),
                        o2.getId());
            }
        });
    }

    private ServiceHost host;
    private final SortedSet<Operation> pendingStartOperations = createOperationSet();
    private final Map<String, SortedSet<Operation>> pendingServiceAvailableCompletions = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, Operation> pendingOperationsForRetry = new ConcurrentSkipListMap<>();

    public static OperationTracker create(ServiceHost host) {
        OperationTracker omt = new OperationTracker();
        omt.host = host;
        return omt;
    }

    public void trackOperationForRetry(long expirationMicros, Throwable e, Operation op) {
        this.host.log(Level.WARNING,
                "Retrying id %d to %s (retries: %d). Failure: %s",
                op.getId(), op.getUri().getHost() + ":" + op.getUri().getPort(),
                op.getRetryCount(),
                e.toString());
        op.incrementRetryCount();
        this.pendingOperationsForRetry.put(expirationMicros, op);
    }

    public void trackStartOperation(Operation op) {
        this.pendingStartOperations.add(op);
    }

    public void removeStartOperation(Operation post) {
        this.pendingStartOperations.remove(post);
    }

    public synchronized SortedSet<Operation> trackServiceAvailableCompletion(String link,
            Operation opTemplate, boolean doOpClone) {
        SortedSet<Operation> pendingOps = this.pendingServiceAvailableCompletions
                .get(link);
        if (pendingOps == null) {
            pendingOps = createOperationSet();
            this.pendingServiceAvailableCompletions.put(link, pendingOps);
        }
        pendingOps.add(doOpClone ? opTemplate.clone() : opTemplate);
        return pendingOps;
    }

    public boolean hasPendingServiceAvailableCompletions(String link) {
        return this.pendingServiceAvailableCompletions.containsKey(link);
    }

    public SortedSet<Operation> removeServiceAvailableCompletions(String link) {
        return this.pendingServiceAvailableCompletions.remove(link);
    }

    public void performMaintenance(long nowMicros) {
        Iterator<Operation> startOpsIt = this.pendingStartOperations.iterator();
        checkOperationExpiration(nowMicros, startOpsIt);

        for (Entry<String, SortedSet<Operation>> entry : this.pendingServiceAvailableCompletions
                .entrySet()) {
            String link = entry.getKey();
            SortedSet<Operation> pendingOps = entry.getValue();
            Service s = this.host.findService(link, true);
            if (s != null && s.getProcessingStage() == ProcessingStage.AVAILABLE) {
                this.host.log(Level.WARNING,
                        "Service %s available, but has pending start operations", link);
                this.host.processPendingServiceAvailableOperations(s, null, false);
                break;
            }
            Iterator<Operation> it = pendingOps.iterator();
            checkOperationExpiration(nowMicros, it);
        }

        final long intervalMicros = TimeUnit.SECONDS.toMicros(1);
        Iterator<Entry<Long, Operation>> it = this.pendingOperationsForRetry.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, Operation> entry = it.next();
            Operation o = entry.getValue();
            if (this.host.isStopping()) {
                o.fail(new CancellationException());
                return;
            }

            // Apply linear back-off: we delay retry based on the number of retry attempts. We
            // keep retrying until expiration of the operation (applied in retryOrFailRequest)
            long queuingTimeMicros = entry.getKey();
            long estimatedRetryTimeMicros = o.getRetryCount() * intervalMicros +
                    queuingTimeMicros;
            if (estimatedRetryTimeMicros > nowMicros) {
                continue;
            }
            it.remove();
            this.host.handleRequest(null, o);
        }
    }

    private void checkOperationExpiration(long now, Iterator<Operation> iterator) {
        while (iterator.hasNext()) {
            Operation op = iterator.next();
            if (op == null || op.getExpirationMicrosUtc() > now) {
                // not expired, and since we walk in ascending order, no other operations
                // are expired
                break;
            }
            iterator.remove();
            this.host.run(() -> op.fail(new TimeoutException(op.toString())));
        }
    }

    public void close() {
        for (Operation op : this.pendingOperationsForRetry.values()) {
            op.fail(new CancellationException());
        }
        this.pendingOperationsForRetry.clear();

        for (Operation op : this.pendingStartOperations) {
            op.fail(new CancellationException());
        }
        this.pendingStartOperations.clear();

        for (SortedSet<Operation> opSet : this.pendingServiceAvailableCompletions.values()) {
            for (Operation op : opSet) {
                op.fail(new CancellationException());
            }
        }
        this.pendingServiceAvailableCompletions.clear();
    }
}