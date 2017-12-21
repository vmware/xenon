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
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.vmware.xenon.common.Service.ProcessingStage;

/**
 * Performs periodic maintenance and expiration tracking on operations. Utilized by
 * service host for all operation related maintenance.
 */
public class OperationTracker {

    private static ConcurrentSkipListSet<Operation> createOperationSet() {
        return new ConcurrentSkipListSet<>(Comparator.comparingLong(Operation::getId));
    }

    private ServiceHost host;
    private final SortedSet<Operation> pendingStartOperations = createOperationSet();
    private final ConcurrentHashMap<String, SortedSet<Operation>> pendingServiceStartCompletions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SortedSet<Operation>> pendingServiceAvailableCompletions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Operation> pendingOperationsForRetry = new ConcurrentHashMap<>();

    public static OperationTracker create(ServiceHost host) {
        OperationTracker omt = new OperationTracker();
        omt.host = host;
        return omt;
    }

    public void trackOperationForRetry(long expirationMicros, Throwable e, Operation op) {
        this.host.log(Level.WARNING,
                "Retrying id %d to %s (retries: %d). Failure: %s",
                op.getId(), op.getUri(),
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

    public void trackServiceStartCompletion(String link, Operation op) {
        // Using map.compute() to make sure the function is performed atomically
        this.pendingServiceStartCompletions
                .compute(link, (k, pendingOps) -> {
                    if (pendingOps == null) {
                        pendingOps = createOperationSet();
                    }
                    pendingOps.add(op);
                    return pendingOps;
                });
    }

    public SortedSet<Operation> removeServiceStartCompletions(String link) {
        return this.pendingServiceStartCompletions.remove(link);
    }

    public void trackServiceAvailableCompletion(String link,
            Operation opTemplate, boolean doOpClone) {
        // Using map.compute() to make sure the function is performed atomically
        Operation op = doOpClone ? opTemplate.clone() : opTemplate;
        this.pendingServiceAvailableCompletions
                .compute(link, (k, pendingOps) -> {
                    if (pendingOps == null) {
                        pendingOps = createOperationSet();
                    }
                    pendingOps.add(op);
                    return pendingOps;
                });
    }

    public boolean hasPendingServiceAvailableCompletions(String link) {
        return this.pendingServiceAvailableCompletions.containsKey(link);
    }

    public SortedSet<Operation> removeServiceAvailableCompletions(String link) {
        return this.pendingServiceAvailableCompletions.remove(link);
    }

    public void performMaintenance(long nowMicros) {
        // check pendingStartOperations
        Iterator<Operation> startOpsIt = this.pendingStartOperations.iterator();
        checkOperationExpiration(nowMicros, startOpsIt);

        // check pendingServiceStartCompletions
        for (Entry<String, SortedSet<Operation>> entry : this.pendingServiceStartCompletions.entrySet()) {
            String link = entry.getKey();
            SortedSet<Operation> pendingOps = entry.getValue();
            Service s = this.host.findService(link, true);
            if (s != null && s.getProcessingStage() == ProcessingStage.AVAILABLE) {
                this.host.log(Level.WARNING,
                        "Service %s available, but has pending start operations", link);
                processPendingServiceStartOperations(link, ProcessingStage.AVAILABLE, s);
                continue;
            }

            if (s == null || s.getProcessingStage() == ProcessingStage.STOPPED) {
                this.host.log(Level.WARNING,
                        "Service %s has stopped, but has pending start operations", link);
                processPendingServiceStartOperations(link, ProcessingStage.STOPPED, null);
                continue;
            }

            Iterator<Operation> it = pendingOps.iterator();
            checkOperationExpiration(nowMicros, it);
        }

        // check pendingServiceAvailableCompletions
        for (Entry<String, SortedSet<Operation>> entry : this.pendingServiceAvailableCompletions
                .entrySet()) {
            String link = entry.getKey();
            SortedSet<Operation> pendingOps = entry.getValue();
            Service s = this.host.findService(link, true);
            if (s != null && s.getProcessingStage() == ProcessingStage.AVAILABLE) {
                this.host.log(Level.WARNING,
                        "Service %s available, but has pending start operations", link);
                this.host.processPendingServiceAvailableOperations(s, null, false);
                continue;
            }

            Iterator<Operation> it = pendingOps.iterator();
            checkOperationExpiration(nowMicros, it);
        }

        // check pendingOperationsForRetry
        final long intervalMicros = TimeUnit.SECONDS.toMicros(1);
        Iterator<Entry<Long, Operation>> it = this.pendingOperationsForRetry.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, Operation> entry = it.next();
            Operation o = entry.getValue();
            if (this.host.isStopping()) {
                o.fail(new CancellationException("Host is stopping"));
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

    void processPendingServiceStartOperations(String link, ProcessingStage processingStage, Service s) {
        SortedSet<Operation> ops = removeServiceStartCompletions(link);
        if (ops == null || ops.isEmpty()) {
            return;
        }

        for (Operation op : ops) {
            if (processingStage == ProcessingStage.AVAILABLE) {
                this.host.run(() -> {
                    if (op.getUri() == null) {
                        op.setUri(s.getUri());
                    }
                    if (op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
                        this.host.restoreActionOnChildServiceToPostOnFactory(link, op);
                    }
                    op.complete();
                });
            } else {
                this.host.run(() -> op.fail(new IllegalStateException(op.toString())));
            }
        }
    }

    public void close() {
        for (Operation op : this.pendingOperationsForRetry.values()) {
            op.fail(new CancellationException("Operation tracker is closing"));
        }
        this.pendingOperationsForRetry.clear();

        for (Operation op : this.pendingStartOperations) {
            op.fail(new CancellationException("Operation tracker is closing"));
        }
        this.pendingStartOperations.clear();

        for (SortedSet<Operation> opSet : this.pendingServiceStartCompletions.values()) {
            for (Operation op : opSet) {
                op.fail(new CancellationException("Operation tracker is closing"));
            }
        }
        this.pendingServiceStartCompletions.clear();

        for (SortedSet<Operation> opSet : this.pendingServiceAvailableCompletions.values()) {
            for (Operation op : opSet) {
                op.fail(new CancellationException("Operation tracker is closing"));
            }
        }
        this.pendingServiceAvailableCompletions.clear();
    }
}