/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

import java.net.URI;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.dcp.common.Operation.AuthorizationContext;
import com.vmware.dcp.common.ServiceStats.ServiceStat;
import com.vmware.dcp.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.dcp.common.jwt.Signer;

/**
 * Infrastructure use only. Minimal Service implementation. Core service implementations that do not
 * require synchronization, queuing or advanced options use this as the base class.
 */
public class StatelessService implements Service {

    private long maintenanceIntervalMicros;
    private OperationProcessingChain opProcessingChain;
    private ProcessingStage stage = ProcessingStage.CREATED;
    private ServiceHost host;
    private String selfLink;
    protected EnumSet<ServiceOption> options = EnumSet.noneOf(ServiceOption.class);
    private UtilityService utilityService;
    protected Class<? extends ServiceDocument> stateType;

    public StatelessService(Class<? extends ServiceDocument> stateType) {
        if (stateType == null) {
            throw new IllegalArgumentException("stateType is required");
        }
        this.stateType = stateType;
        this.options.add(ServiceOption.CONCURRENT_UPDATE_HANDLING);
    }

    public StatelessService() {
        this.stateType = ServiceDocument.class;
    }

    @Override
    public void handleStart(Operation startPost) {
        startPost.complete();
    }

    @Override
    public boolean queueRequest(Operation op) {
        return false;
    }

    @Override
    public void sendRequest(Operation op) {
        if (op.isJoined()) {
            for (Operation currentOp : op.getJoinedOperations()) {
                prepareRequest(currentOp);
            }
        } else {
            prepareRequest(op);
        }

        this.host.sendRequest(op);
    }

    protected void prepareRequest(Operation op) {
        op.setReferer(UriUtils.buildUri(getHost().getPublicUri(), getSelfLink()));
    }

    @Override
    public void handleRequest(Operation op) {
        handleRequest(op, OperationProcessingStage.PROCESSING_FILTERS);
    }

    @Override
    public void handleRequest(Operation op, OperationProcessingStage opProcessingStage) {
        if (opProcessingStage == OperationProcessingStage.PROCESSING_FILTERS) {
            OperationProcessingChain opProcessingChain = getOperationProcessingChain();
            if (opProcessingChain != null && !opProcessingChain.processRequest(op)) {
                return;
            }
            opProcessingStage = OperationProcessingStage.EXECUTING_SERVICE_HANDLER;
        }

        if (opProcessingStage == OperationProcessingStage.EXECUTING_SERVICE_HANDLER) {
            if (op.getAction() == Action.GET) {
                op.nestCompletion(o -> {
                    handleGetCompletion(op);
                });
                try {
                    handleGet(op);
                } catch (Throwable e) {
                    op.fail(e);
                }
                return;
            } else if (op.getAction() == Action.DELETE) {
                op.nestCompletion(o -> {
                    handleDeleteCompletion(op);
                });
                try {
                    handleDelete(op);
                } catch (Throwable e) {
                    op.fail(e);
                }
                return;
            }
        }

        getHost().failRequestActionNotSupported(op);
    }

    public void handleGet(Operation get) {
        get.complete();
    }

    public void handleDelete(Operation delete) {
        delete.complete();
    }

    protected void handleDeleteCompletion(Operation delete) {
        getHost().stopService(this);
        delete.complete();
    }

    private void handleGetCompletion(Operation op) {
        if (!this.options.contains(ServiceOption.PERSISTENCE)) {
            op.complete();
            return;
        }

        URI documentQuery = UriUtils.buildDocumentQueryUri(getHost(),
                this.selfLink,
                true,
                false,
                this.options);
        sendRequest(Operation.createGet(documentQuery).setCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            op.setBodyNoCloning(o.getBodyRaw()).complete();
        }));
    }

    @Override
    public void handleMaintenance(Operation post) {
        post.complete();
    }

    @Override
    public ServiceHost getHost() {
        return this.host;
    }

    @Override
    public String getSelfLink() {
        return this.selfLink;
    }

    @Override
    public URI getUri() {
        return UriUtils.buildUri(this.host, this.selfLink);
    }

    @Override
    public OperationProcessingChain getOperationProcessingChain() {
        return this.opProcessingChain;
    }

    @Override
    public ProcessingStage getProcessingStage() {
        return this.stage;
    }

    @Override
    public boolean hasOption(ServiceOption cap) {
        return this.options.contains(cap);
    }

    @Override
    public void toggleOption(ServiceOption option, boolean enable) {
        if (enable) {
            if (option == ServiceOption.REPLICATION) {
                throw new IllegalArgumentException("Option is not supported");
            }
            if (option == ServiceOption.EAGER_CONSISTENCY) {
                throw new IllegalArgumentException("Option is not supported");
            }
            if (option == ServiceOption.IDEMPOTENT_POST) {
                throw new IllegalArgumentException("Option is not supported");
            }
        }
        if (enable) {
            this.options.add(option);
        } else {
            this.options.remove(option);
        }
    }

    @Override
    public void setStat(String name, double newValue) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService();
        ServiceStat s = getStat(name);
        this.utilityService.setStat(s, newValue);
    }

    @Override
    public void setStat(ServiceStat s, double newValue) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService();
        this.utilityService.setStat(s, newValue);
    }

    @Override
    public void adjustStat(ServiceStat s, double delta) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService();
        this.utilityService.adjustStat(s, delta);
    }

    @Override
    public void adjustStat(String name, double delta) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        allocateUtilityService();
        ServiceStat s = getStat(name);
        this.utilityService.adjustStat(s, delta);
    }

    @Override
    public ServiceStat getStat(String name) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        allocateUtilityService();
        return this.utilityService.getStat(name);
    }

    public ServiceStat getHistogramStat(String name) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat s = getStat(name);
        synchronized (s) {
            if (s.logHistogram == null) {
                s.logHistogram = new ServiceStatLogHistogram();
            }
        }
        return s;
    }

    private boolean allocateUtilityService() {
        synchronized (this.options) {
            if (this.utilityService == null) {
                this.utilityService = new UtilityService().setParent(this);
                ;
            }
        }
        return true;
    }

    @Override
    public void setHost(ServiceHost serviceHost) {
        this.host = serviceHost;
    }

    @Override
    public void setSelfLink(String path) {
        if (path != null) {
            this.selfLink = path.intern();
        } else {
            this.selfLink = null;
        }
    }

    @Override
    public void setOperationProcessingChain(OperationProcessingChain opProcessingChain) {
        this.opProcessingChain = opProcessingChain;
    }

    @Override
    public void setProcessingStage(ProcessingStage stage) {
        if (this.stage == stage) {
            return;
        }

        this.stage = stage;
        if (stage != ProcessingStage.AVAILABLE) {
            return;
        }

        getHost().notifyServiceAvailabilitySubscribers(this);
    }

    @Override
    public ServiceDocument setInitialState(String jsonState, Long version) {
        ServiceDocument d = Utils.fromJson(jsonState, this.stateType);
        if (version != null) {
            d.documentVersion = version;
        }
        return d;
    }

    @Override
    public Service getUtilityService(String uriPath) {
        allocateUtilityService();
        return this.utilityService;
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument d;

        try {
            d = this.stateType.newInstance();
        } catch (Throwable e) {
            logSevere(e);
            return null;
        }

        d.documentDescription = getHost().buildDocumentDescription(this);
        return d;
    }

    public void logSevere(Throwable e) {
        log(Level.SEVERE, "%s", Utils.toString(e));
    }

    public void logSevere(String fmt, Object... args) {
        log(Level.SEVERE, fmt, args);
    }

    public void logInfo(String fmt, Object... args) {
        log(Level.INFO, fmt, args);
    }

    public void logFine(String fmt, Object... args) {
        log(Level.FINE, fmt, args);
    }

    public void logWarning(String fmt, Object... args) {
        log(Level.WARNING, fmt, args);
    }

    protected void log(Level level, String fmt, Object... args) {
        String uri = this.host != null && this.selfLink != null ? getUri().toString()
                : this.getClass().getSimpleName();
        Logger lg = Logger.getLogger(this.getClass().getName());
        Utils.log(lg, 3, uri, level, fmt, args);
    }

    @Override
    public void setPeerNodeSelectorPath(String uriPath) {
        throw new RuntimeException("Replication is not supported");
    }

    @Override
    public String getPeerNodeSelectorPath() {
        throw new RuntimeException("Replication is not supported");
    }

    @Override
    public EnumSet<ServiceOption> getOptions() {
        return this.options.clone();
    }

    public void publish(Operation op) {
        UtilityService u = this.utilityService;
        if (u == null) {
            return;
        }
        u.notifySubscribers(op);
    }

    @Override
    public void setState(Operation op, ServiceDocument state) {
        op.linkState(state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ServiceDocument> T getState(Operation op) {
        return (T) op.getLinkedState();
    }

    @Override
    public void setMaintenanceIntervalMicros(long micros) {
        if (micros < 0) {
            throw new IllegalArgumentException("micros must be positive");
        }
        this.maintenanceIntervalMicros = micros;
    }

    @Override
    public long getMaintenanceIntervalMicros() {
        return this.maintenanceIntervalMicros;
    }

    @Override
    public Operation dequeueRequest() {
        return null;
    }

    @Override
    public Class<? extends ServiceDocument> getStateType() {
        return this.stateType;
    }

    @Override
    public void handleConfigurationRequest(Operation request) {
        if (request.getAction() == Action.PATCH) {
            allocateUtilityService();
            this.utilityService.handlePatchConfiguration(request, null);
            return;
        }

        if (request.getAction() != Action.GET) {
            request.fail(new IllegalArgumentException("Action not supported: "
                    + request.getAction()));
            return;
        }

        ServiceConfiguration cfg = new ServiceConfiguration();
        cfg.options = getOptions();
        cfg.maintenanceIntervalMicros = getMaintenanceIntervalMicros();
        request.setBody(cfg).complete();
    }

    /**
     * Set authorization context on operation.
     */
    public final void setAuthorizationContext(Operation op, AuthorizationContext ctx) {
        if (getHost().isPrivilegedService(this)) {
            op.setAuthorizationContext(ctx);
        } else {
            throw new RuntimeException("Service not allowed to set authorization context");
        }
    }

    /**
     * Returns the host's token signer.
     */
    public final Signer getTokenSigner() {
        if (getHost().isPrivilegedService(this)) {
            return getHost().getTokenSigner();
        } else {
            throw new RuntimeException("Service not allowed to get token signer");
        }
    }

    /**
     * Returns the system user's authorization context.
     */
    public final AuthorizationContext getSystemAuthorizationContext() {
        if (getHost().isPrivilegedService(this)) {
            return getHost().getSystemAuthorizationContext();
        } else {
            throw new RuntimeException("Service not allowed to get system authorization context");
        }
    }
}
