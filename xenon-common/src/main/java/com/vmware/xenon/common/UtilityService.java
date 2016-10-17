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

import java.io.NotActiveException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UiContentService;


/**
 * Utility service managing the various URI control REST APIs for each service instance. A single
 * utility service instance manages operations on multiple URI suffixes (/stats, /subscriptions,
 * etc) in order to reduce runtime overhead per service instance
 */
public class UtilityService implements Service {
    private transient Service parent;
    private ServiceStats stats;
    private ServiceSubscriptionState subscriptions;
    private UiContentService uiService;

    public UtilityService() {
    }

    public UtilityService setParent(Service parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation op) {
        String uriPrefix = this.parent.getSelfLink() + ServiceHost.SERVICE_URI_SUFFIX_UI;

        if (op.getUri().getPath().startsWith(uriPrefix)) {
            // startsWith catches all /factory/instance/ui/some-script.js
            handleUiRequest(op);
        } else if (op.getUri().getPath().endsWith(ServiceHost.SERVICE_URI_SUFFIX_STATS)) {
            handleStatsRequest(op);
        } else if (op.getUri().getPath().endsWith(ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS)) {
            handleSubscriptionsRequest(op);
        } else if (op.getUri().getPath().endsWith(ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE)) {
            handleDocumentTemplateRequest(op);
        } else if (op.getUri().getPath().endsWith(ServiceHost.SERVICE_URI_SUFFIX_CONFIG)) {
            this.parent.handleConfigurationRequest(op);
        } else if (op.getUri().getPath().endsWith(ServiceHost.SERVICE_URI_SUFFIX_AVAILABLE)) {
            handleAvailableRequest(op);
        } else {
            op.fail(new UnknownHostException());
        }
    }

    @Override
    public void handleCreate(Operation post) {
        post.complete();
    }

    @Override
    public void handleStart(Operation startPost) {
        startPost.complete();
    }

    @Override
    public void handleStop(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation op, OperationProcessingStage opProcessingStage) {
        handleRequest(op);
    }

    private void handleAvailableRequest(Operation op) {
        if (op.getAction() == Action.GET) {
            if (this.parent.getProcessingStage() != ProcessingStage.PAUSED
                    && this.parent.getProcessingStage() != ProcessingStage.AVAILABLE) {
                // processing stage takes precedence over isAvailable statistic
                op.fail(Operation.STATUS_CODE_UNAVAILABLE);
                return;
            }
            if (this.stats == null) {
                op.complete();
                return;
            }
            ServiceStat st = this.getStat(STAT_NAME_AVAILABLE, false);
            if (st == null || st.latestValue == 1.0) {
                op.complete();
                return;
            }
            op.fail(Operation.STATUS_CODE_UNAVAILABLE);
        } else if (op.getAction() == Action.PATCH || op.getAction() == Action.PUT) {
            if (!op.hasBody()) {
                op.fail(new IllegalArgumentException("body is required"));
                return;
            }
            ServiceStat st = op.getBody(ServiceStat.class);
            if (!STAT_NAME_AVAILABLE.equals(st.name)) {
                op.fail(new IllegalArgumentException(
                        "body must be of type ServiceStat and name must be "
                                + STAT_NAME_AVAILABLE));
                return;
            }
            handleStatsRequest(op);
        } else {
            getHost().failRequestActionNotSupported(op);
        }
    }

    private void handleSubscriptionsRequest(Operation op) {
        synchronized (this) {
            if (this.subscriptions == null) {
                this.subscriptions = new ServiceSubscriptionState();
                this.subscriptions.subscribers = new ConcurrentSkipListMap<>();
            }
        }

        ServiceSubscriber body = null;
        if (op.hasBody()) {
            body = op.getBody(ServiceSubscriber.class);
            if (body.reference == null) {
                op.fail(new IllegalArgumentException("reference is required"));
                return;
            }
        }

        switch (op.getAction()) {
        case POST:
            // synchronize to avoid concurrent modification during serialization for GET
            synchronized (this.subscriptions) {
                this.subscriptions.subscribers.put(body.reference, body);
            }
            if (!body.replayState) {
                break;
            }
            // if replayState is set, replay the current state to the subscriber
            URI notificationURI = body.reference;
            this.parent.sendRequest(Operation.createGet(this, this.parent.getSelfLink())
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    op.fail(new IllegalStateException(
                                            "Unable to get current state"));
                                    return;
                                }
                                Operation putOp = Operation
                                        .createPut(notificationURI)
                                        .setBodyNoCloning(o.getBody(this.parent.getStateType()))
                                        .addPragmaDirective(
                                                Operation.PRAGMA_DIRECTIVE_NOTIFICATION)
                                        .setReferer(getUri());
                                this.parent.sendRequest(putOp);
                            }));

            break;
        case DELETE:
            // synchronize to avoid concurrent modification during serialization for GET
            synchronized (this.subscriptions) {
                this.subscriptions.subscribers.remove(body.reference);
            }
            break;
        case GET:
            ServiceDocument rsp;
            synchronized (this.subscriptions) {
                rsp = Utils.clone(this.subscriptions);
            }
            op.setBody(rsp);
            break;
        default:
            op.fail(new NotActiveException());
            break;

        }

        op.complete();
    }

    public boolean hasSubscribers() {
        ServiceSubscriptionState subscriptions = this.subscriptions;
        return subscriptions != null
                && subscriptions.subscribers != null
                && !subscriptions.subscribers.isEmpty();
    }

    public boolean hasStats() {
        ServiceStats stats = this.stats;
        return stats != null && stats.entries != null && !stats.entries.isEmpty();
    }

    public void notifySubscribers(Operation op) {
        try {
            if (op.getAction() == Action.GET) {
                return;
            }

            if (!this.hasSubscribers()) {
                return;
            }

            long now = Utils.getNowMicrosUtc();

            Operation clone = op.clone();
            clone.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NOTIFICATION);
            for (Entry<URI, ServiceSubscriber> e : this.subscriptions.subscribers.entrySet()) {
                ServiceSubscriber s = e.getValue();
                notifySubscriber(now, clone, s);
            }

            if (!performSubscriptionsMaintenance(now)) {
                return;
            }
        } catch (Throwable e) {
            this.parent.getHost().log(Level.WARNING,
                    "Uncaught exception notifying subscribers for %s: %s",
                    this.parent.getSelfLink(), Utils.toString(e));
        }
    }

    private void notifySubscriber(long now, Operation clone, ServiceSubscriber s) {
        synchronized (s) {
            if (s.failedNotificationCount != null) {
                // indicate to the subscriber that they missed notifications and should retrieve latest state
                clone.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SKIPPED_NOTIFICATIONS);
            }
        }

        CompletionHandler c = (o, ex) -> {
            s.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
            synchronized (s) {
                if (ex != null) {
                    if (s.failedNotificationCount == null) {
                        s.failedNotificationCount = 0L;
                        s.initialFailedNotificationTimeMicros = now;
                    }
                    s.failedNotificationCount++;
                    return;
                }

                if (s.failedNotificationCount != null) {
                    // the subscriber is available again.
                    s.failedNotificationCount = null;
                    s.initialFailedNotificationTimeMicros = null;
                }
            }
        };

        this.parent.sendRequest(clone.setUri(s.reference).setCompletion(c));
    }

    private boolean performSubscriptionsMaintenance(long now) {
        List<URI> subscribersToDelete = null;
        synchronized (this) {
            if (this.subscriptions == null) {
                return false;
            }

            Iterator<Entry<URI, ServiceSubscriber>> it = this.subscriptions.subscribers.entrySet()
                    .iterator();
            while (it.hasNext()) {
                Entry<URI, ServiceSubscriber> e = it.next();
                ServiceSubscriber s = e.getValue();
                boolean remove = false;
                synchronized (s) {
                    if (s.documentExpirationTimeMicros != 0 && s.documentExpirationTimeMicros < now) {
                        remove = true;
                    } else if (s.notificationLimit != null) {
                        if (s.notificationCount == null) {
                            s.notificationCount = 0L;
                        }
                        if (++s.notificationCount >= s.notificationLimit) {
                            remove = true;
                        }
                    } else if (s.failedNotificationCount != null
                            && s.failedNotificationCount > ServiceSubscriber.NOTIFICATION_FAILURE_LIMIT) {
                        if (now - s.initialFailedNotificationTimeMicros > getHost()
                                .getMaintenanceIntervalMicros()) {
                            remove = true;
                        }
                    }
                }

                if (!remove) {
                    continue;
                }

                it.remove();
                if (subscribersToDelete == null) {
                    subscribersToDelete = new ArrayList<>();
                }
                subscribersToDelete.add(s.reference);
                continue;
            }
        }

        if (subscribersToDelete != null) {
            for (URI subscriber : subscribersToDelete) {
                this.parent.sendRequest(Operation.createDelete(subscriber));
            }
        }

        return true;
    }

    private void handleUiRequest(Operation op) {
        if (op.getAction() != Action.GET) {
            op.fail(new IllegalArgumentException("Action not supported"));
            return;
        }

        if (!this.parent.hasOption(ServiceOption.HTML_USER_INTERFACE)) {
            String servicePath = UriUtils.buildUriPath(ServiceUriPaths.UI_SERVICE_BASE_URL, op
                    .getUri().getPath());
            String defaultHtmlPath = UriUtils.buildUriPath(servicePath.substring(0,
                    servicePath.length() - ServiceUriPaths.UI_PATH_SUFFIX.length()),
                    ServiceUriPaths.UI_SERVICE_HOME);

            redirectGetToHtmlUiResource(op, defaultHtmlPath);
            return;
        }

        if (this.uiService == null) {
            this.uiService = new UiContentService() {
            };
            this.uiService.setHost(this.parent.getHost());
        }

        // simulate a full service deployed at the utility endpoint /service/ui
        String selfLink = this.parent.getSelfLink() + ServiceHost.SERVICE_URI_SUFFIX_UI;
        this.uiService.handleUiGet(selfLink, this.parent, op);
    }

    public void redirectGetToHtmlUiResource(Operation op, String htmlResourcePath) {
        // redirect using relative url without host:port
        // not so much optimization as handling the case of port forwarding/containers
        try {
            op.addResponseHeader(Operation.LOCATION_HEADER,
                    URLDecoder.decode(htmlResourcePath, Utils.CHARSET));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }

        op.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
        op.complete();
    }

    private void handleStatsRequest(Operation op) {
        switch (op.getAction()) {
        case PUT:
            ServiceStats.ServiceStat stat = op
                    .getBody(ServiceStats.ServiceStat.class);
            if (stat.kind == null) {
                op.fail(new IllegalArgumentException("kind is required"));
                return;
            }
            if (stat.kind.equals(ServiceStats.ServiceStat.KIND)) {
                if (stat.name == null) {
                    op.fail(new IllegalArgumentException("stat name is required"));
                    return;
                }
                replaceSingleStat(stat);
            } else if (stat.kind.equals(ServiceStats.KIND)) {
                ServiceStats stats = op.getBody(ServiceStats.class);
                if (stats.entries == null || stats.entries.isEmpty()) {
                    op.fail(new IllegalArgumentException("stats entries need to be defined"));
                    return;
                }
                replaceAllStats(stats);
            } else {
                op.fail(new IllegalArgumentException("operation not supported for kind"));
                return;
            }
            op.complete();
            break;
        case POST:
            ServiceStats.ServiceStat newStat = op.getBody(ServiceStats.ServiceStat.class);
            if (newStat.name == null) {
                op.fail(new IllegalArgumentException("stat name is required"));
                return;
            }
            // create a stat object if one does not exist
            ServiceStats.ServiceStat existingStat = this.getStat(newStat.name);
            if (existingStat == null) {
                op.fail(new IllegalArgumentException("stat does not exist"));
                return;
            }
            initializeOrSetStat(existingStat, newStat);
            op.complete();
            break;
        case DELETE:
            // TODO support removing stats externally - do we need this?
            op.fail(new NotActiveException());
            break;
        case PATCH:
            newStat = op.getBody(ServiceStats.ServiceStat.class);
            if (newStat.name == null) {
                op.fail(new IllegalArgumentException("stat name is required"));
                return;
            }
            // if an existing stat by this name exists, adjust the stat value, else this is a no-op
            existingStat = this.getStat(newStat.name, false);
            if (existingStat == null) {
                op.fail(new IllegalArgumentException("stat to patch does not exist"));
                return;
            }
            adjustStat(existingStat, newStat.latestValue);
            op.complete();
            break;
        case GET:
            if (this.stats == null) {
                ServiceStats s = new ServiceStats();
                populateDocumentProperties(s);
                op.setBody(s).complete();
            } else {
                ServiceDocument rsp;
                synchronized (this.stats) {
                    rsp = populateDocumentProperties(this.stats);
                    rsp = Utils.clone(rsp);
                }
                op.setBodyNoCloning(rsp);
                op.complete();
            }
            break;
        default:
            op.fail(new NotActiveException());
            break;

        }
    }

    private ServiceStats populateDocumentProperties(ServiceStats stats) {
        ServiceStats clone = new ServiceStats();
        clone.entries = stats.entries;
        clone.documentUpdateTimeMicros = stats.documentUpdateTimeMicros;
        clone.documentSelfLink = UriUtils.buildUriPath(this.parent.getSelfLink(),
                ServiceHost.SERVICE_URI_SUFFIX_STATS);
        clone.documentOwner = getHost().getId();
        clone.documentKind = Utils.buildKind(ServiceStats.class);
        return clone;
    }

    private void handleDocumentTemplateRequest(Operation op) {
        if (op.getAction() != Action.GET) {
            op.fail(new NotActiveException());
            return;
        }
        ServiceDocument template = this.parent.getDocumentTemplate();
        String serializedTemplate = Utils.toJsonHtml(template);
        op.setBody(serializedTemplate).complete();
    }

    @Override
    public void handleConfigurationRequest(Operation op) {
        this.parent.handleConfigurationRequest(op);
    }

    public void handlePatchConfiguration(Operation op, ServiceConfigUpdateRequest updateBody) {
        if (updateBody == null) {
            updateBody = op.getBody(ServiceConfigUpdateRequest.class);
        }

        if (!ServiceConfigUpdateRequest.KIND.equals(updateBody.kind)) {
            op.fail(new IllegalArgumentException("Unrecognized kind: " + updateBody.kind));
            return;
        }

        if (updateBody.maintenanceIntervalMicros == null
                && updateBody.operationQueueLimit == null
                && updateBody.epoch == null
                && (updateBody.addOptions == null || updateBody.addOptions.isEmpty())
                && (updateBody.removeOptions == null || updateBody.removeOptions
                        .isEmpty())) {
            op.fail(new IllegalArgumentException(
                    "At least one configuraton field must be specified"));
            return;
        }

        // service might fail a capability toggle if the capability can not be changed after start
        if (updateBody.addOptions != null) {
            for (ServiceOption c : updateBody.addOptions) {
                this.parent.toggleOption(c, true);
            }
        }

        if (updateBody.removeOptions != null) {
            for (ServiceOption c : updateBody.removeOptions) {
                this.parent.toggleOption(c, false);
            }
        }

        if (updateBody.maintenanceIntervalMicros != null) {
            this.parent.setMaintenanceIntervalMicros(updateBody.maintenanceIntervalMicros);
        }

        op.complete();
    }

    private void initializeOrSetStat(ServiceStat stat, ServiceStat newValue) {
        synchronized (stat) {
            if (stat.timeSeriesStats == null && newValue.timeSeriesStats != null) {
                stat.timeSeriesStats = new TimeSeriesStats(newValue.timeSeriesStats.numBins,
                        newValue.timeSeriesStats.binDurationMillis, newValue.timeSeriesStats.aggregationType);
            }
            stat.unit = newValue.unit;
            stat.sourceTimeMicrosUtc = newValue.sourceTimeMicrosUtc;
            setStat(stat, newValue.latestValue);
        }
    }

    @Override
    public void setStat(ServiceStat stat, double newValue) {
        allocateStats();
        findStat(stat.name, true, stat);

        synchronized (stat) {
            stat.version++;
            stat.accumulatedValue += newValue;
            stat.latestValue = newValue;
            if (stat.logHistogram != null) {
                int binIndex = 0;
                if (newValue > 0.0) {
                    binIndex = (int) Math.log10(newValue);
                }
                if (binIndex >= 0 && binIndex < stat.logHistogram.bins.length) {
                    stat.logHistogram.bins[binIndex]++;
                }
            }
            stat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (stat.timeSeriesStats != null) {
                if (stat.sourceTimeMicrosUtc != null) {
                    stat.timeSeriesStats.add(stat.sourceTimeMicrosUtc, newValue);
                } else {
                    stat.timeSeriesStats.add(stat.lastUpdateMicrosUtc, newValue);
                }
            }
        }
    }

    @Override
    public void adjustStat(ServiceStat stat, double delta) {
        allocateStats();
        synchronized (stat) {
            stat.latestValue += delta;
            stat.version++;
            if (stat.logHistogram != null) {

                int binIndex = 0;
                if (delta > 0.0) {
                    binIndex = (int) Math.log10(delta);
                }
                if (binIndex >= 0 && binIndex < stat.logHistogram.bins.length) {
                    stat.logHistogram.bins[binIndex]++;
                }
            }
            stat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (stat.timeSeriesStats != null) {
                if (stat.sourceTimeMicrosUtc != null) {
                    stat.timeSeriesStats.add(stat.sourceTimeMicrosUtc, stat.latestValue);
                } else {
                    stat.timeSeriesStats.add(stat.lastUpdateMicrosUtc, stat.latestValue);
                }
            }
        }
    }

    @Override
    public ServiceStat getStat(String name) {
        return getStat(name, true);
    }

    private ServiceStat getStat(String name, boolean create) {
        if (!allocateStats(true)) {
            return null;
        }
        return findStat(name, create, null);
    }

    private void replaceSingleStat(ServiceStat stat) {
        if (!allocateStats(true)) {
            return;
        }
        synchronized (this.stats) {
            // create a new stat with the default values
            ServiceStat newStat = new ServiceStat();
            newStat.name = stat.name;
            initializeOrSetStat(newStat, stat);
            if (this.stats.entries == null) {
                this.stats.entries = new HashMap<>();
            }
            // add it to the list of stats for this service
            this.stats.entries.put(stat.name, newStat);
        }
    }

    private void replaceAllStats(ServiceStats newStats) {
        if (!allocateStats(true)) {
            return;
        }
        synchronized (this.stats) {
            // reset the current set of stats
            this.stats.entries.clear();
            for (ServiceStats.ServiceStat currentStat : newStats.entries.values()) {
                replaceSingleStat(currentStat);
            }

        }
    }

    private ServiceStat findStat(String name, boolean create, ServiceStat initialStat) {
        synchronized (this.stats) {
            if (this.stats.entries == null) {
                this.stats.entries = new HashMap<>();
            }
            ServiceStat st = this.stats.entries.get(name);
            if (st == null && create) {
                st = initialStat != null ? initialStat : new ServiceStat();
                st.name = name;
                this.stats.entries.put(name, st);
            }
            return st;
        }
    }

    private void allocateStats() {
        allocateStats(true);
    }

    private synchronized boolean allocateStats(boolean mustAllocate) {
        if (!mustAllocate && this.stats == null) {
            return false;
        }
        if (this.stats != null) {
            return true;
        }
        this.stats = new ServiceStats();
        return true;
    }

    @Override
    public ServiceHost getHost() {
        return this.parent.getHost();
    }

    @Override
    public String getSelfLink() {
        return null;
    }

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public OperationProcessingChain getOperationProcessingChain() {
        return null;
    }

    @Override
    public ProcessingStage getProcessingStage() {
        return ProcessingStage.AVAILABLE;
    }

    @Override
    public EnumSet<ServiceOption> getOptions() {
        return EnumSet.of(ServiceOption.UTILITY);
    }

    @Override
    public boolean hasOption(ServiceOption cap) {
        return false;
    }

    @Override
    public void toggleOption(ServiceOption cap, boolean enable) {
        throw new RuntimeException();
    }

    @Override
    public void adjustStat(String name, double delta) {
        return;
    }

    @Override
    public void setStat(String name, double newValue) {
        return;
    }

    @Override
    public void handleMaintenance(Operation post) {
        post.complete();
    }

    @Override
    public void setHost(ServiceHost serviceHost) {

    }

    @Override
    public void setSelfLink(String path) {

    }

    @Override
    public void setOperationProcessingChain(OperationProcessingChain opProcessingChain) {

    }

    @Override
    public ServiceRuntimeContext setProcessingStage(ProcessingStage initialized) {
        return null;
    }

    @Override
    public ServiceDocument setInitialState(Object state, Long initialVersion) {
        return null;
    }

    @Override
    public Service getUtilityService(String uriPath) {
        return null;
    }

    @Override
    public boolean queueRequest(Operation op) {
        return false;
    }

    @Override
    public void sendRequest(Operation op) {
        throw new RuntimeException();
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        return null;
    }

    @Override
    public void setPeerNodeSelectorPath(String uriPath) {

    }

    @Override
    public String getPeerNodeSelectorPath() {
        return null;
    }

    @Override
    public void setState(Operation op, ServiceDocument newState) {
        op.linkState(newState);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ServiceDocument> T getState(Operation op) {
        return (T) op.getLinkedState();
    }

    @Override
    public void setMaintenanceIntervalMicros(long micros) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long getMaintenanceIntervalMicros() {
        return 0;
    }

    @Override
    public Operation dequeueRequest() {
        return null;
    }

    @Override
    public Class<? extends ServiceDocument> getStateType() {
        return null;
    }

    @Override
    public final void setAuthorizationContext(Operation op, AuthorizationContext ctx) {
        throw new RuntimeException("Service not allowed to set authorization context");
    }

    @Override
    public final AuthorizationContext getSystemAuthorizationContext() {
        throw new RuntimeException("Service not allowed to get system authorization context");
    }
}
