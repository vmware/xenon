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
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.jwt.Signer;
import com.vmware.xenon.services.common.ServiceUriPaths;

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
    protected final EnumSet<ServiceOption> options = EnumSet.noneOf(ServiceOption.class);
    private UtilityService utilityService;
    protected Class<? extends ServiceDocument> stateType;

    public StatelessService(Class<? extends ServiceDocument> stateType) {
        if (stateType == null) {
            throw new IllegalArgumentException("stateType is required");
        }
        this.stateType = stateType;
        this.options.add(ServiceOption.CONCURRENT_GET_HANDLING);
        this.options.add(ServiceOption.CONCURRENT_UPDATE_HANDLING);
    }

    public StatelessService() {
        this.stateType = ServiceDocument.class;
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
    public void authorizeRequest(Operation op) {
        // A state-less service has no service state to apply policy to, but it does have a
        // self link. Create a document with the service link so we can apply roles with resource
        // specifications targeting the self link field
        ServiceDocument doc = new ServiceDocument();
        doc.documentSelfLink = this.selfLink;
        if (getHost().isAuthorized(this, doc, op)) {
            op.complete();
            return;
        }

        op.fail(Operation.STATUS_CODE_FORBIDDEN);
    }

    @Override
    public boolean queueRequest(Operation op) {
        return false;
    }

    @Override
    public void sendRequest(Operation op) {
        prepareRequest(op);
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
        try {
            if (opProcessingStage == OperationProcessingStage.PROCESSING_FILTERS) {
                OperationProcessingChain opProcessingChain = getOperationProcessingChain();
                if (opProcessingChain != null && !opProcessingChain.processRequest(op)) {
                    return;
                }
                opProcessingStage = OperationProcessingStage.EXECUTING_SERVICE_HANDLER;
            }

            if (opProcessingStage == OperationProcessingStage.EXECUTING_SERVICE_HANDLER) {
                if (op.getAction() == Action.GET) {
                    if (ServiceHost.isForServiceNamespace(this, op)) {
                        handleGet(op);
                        return;
                    }
                    op.nestCompletion(o -> {
                        handleGetCompletion(op);
                    });
                    handleGet(op);
                } else if (op.getAction() == Action.POST) {
                    handlePost(op);
                } else if (op.getAction() == Action.DELETE) {
                    if (ServiceHost.isForServiceNamespace(this, op)) {
                        // this is a request for the namespace, not the service itself.
                        // Call handleDelete but do not nest completion that stops the service.
                        handleDelete(op);
                        return;
                    }
                    if (ServiceHost.isServiceStop(op)) {
                        op.nestCompletion(o -> {
                            handleStopCompletion(op);
                        });
                        handleStop(op);
                    } else {
                        op.nestCompletion(o -> {
                            handleDeleteCompletion(op);
                        });
                        handleDelete(op);
                    }
                } else if (op.getAction() == Action.OPTIONS) {
                    if (ServiceHost.isForServiceNamespace(this, op)) {
                        handleOptions(op);
                        return;
                    }
                    op.nestCompletion(o -> {
                        handleOptionsCompletion(op);
                    });

                    handleOptions(op);
                } else if (op.getAction() == Action.PATCH) {
                    handlePatch(op);
                } else if (op.getAction() == Action.PUT) {
                    handlePut(op);
                }
            }
        } catch (Throwable e) {
            op.fail(e);
        }
    }

    public void handlePut(Operation put) {
        getHost().failRequestActionNotSupported(put);
    }

    public void handlePatch(Operation patch) {
        getHost().failRequestActionNotSupported(patch);
    }

    public void handleOptions(Operation options) {
        options.setBody(null).complete();
    }

    /**
     * Runs after a DELETE operation, that is not a service stop, completes.
     * It guarantees that the handleStop handler will execute next, and the shared
     * completion that stops the service will run after the stop operation is completed
     */
    protected void handleDeleteCompletion(Operation op) {
        op.nestCompletion((o) -> {
            handleStopCompletion(op);
        });
        handleStop(op);
    }

    /**
     * Stops the service
     */
    protected void handleStopCompletion(Operation op) {
        getHost().stopService(this);
        op.complete();
    }

    protected void handleOptionsCompletion(Operation options) {
        if (!options.hasBody()) {
            options.setBodyNoCloning(getDocumentTemplate());
        }
        options.complete();
    }

    public void handlePost(Operation post) {
        getHost().failRequestActionNotSupported(post);
    }

    public void handleGet(Operation get) {
        get.complete();
    }

    public void handleDelete(Operation delete) {
        delete.complete();
    }

    public void handleStop(Operation delete) {
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

    /**
     * Infrastructure use. Invoked by host to execute a service handler for a maintenance request.
     * ServiceMaintenanceRequest object is set in the operation body, with the reasons field
     * indicating the maintenance reason. Its invoked when
     *
     * 1) Periodically, if ServiceOption.PERIODIC_MAINTENANCE is set.
     *
     * 2) Node group change.
     *
     * Services should override handlePeriodicMaintenance and handleNodeGroupMaintenance.
     *
     * An implementation of this method that needs to interact with the state of this service must
     * do so as if it were a client of this service. That is: the state of the service should be
     * retrieved by requesting a GET; and the state of the service should be mutated by submitting a
     * PATCH, PUT or DELETE.
     */
    @Override
    public void handleMaintenance(Operation post) {
        ServiceMaintenanceRequest request = post.getBody(ServiceMaintenanceRequest.class);
        if (request.reasons.contains(ServiceMaintenanceRequest.MaintenanceReason.PERIODIC_SCHEDULE)) {
            this.handlePeriodicMaintenance(post);
        } else if (request.reasons.contains(ServiceMaintenanceRequest.MaintenanceReason.NODE_GROUP_CHANGE)) {
            this.handleNodeGroupMaintenance(post);
        } else {
            post.complete();
        }
    }

    /**
     * Invoked by the host periodically, if ServiceOption.PERIODIC_MAINTENANCE is set.
     * ServiceMaintenanceRequest object is set in the operation body, with the reasons field
     * indicating the maintenance reason.
     *
     * An implementation of this method that needs to interact with the state of this service must
     * do so as if it were a client of this service. That is: the state of the service should be
     * retrieved by requesting a GET; and the state of the service should be mutated by submitting a
     * PATCH, PUT or DELETE.
     */
    public void handlePeriodicMaintenance(Operation post) {
        post.complete();
    }

    /**
     * Invoked by the host on node group change.
     * ServiceMaintenanceRequest object is set in the operation body, with the reasons field
     * indicating the maintenance reason.
     *
     * An implementation of this method that needs to interact with the state of this service must
     * do so as if it were a client of this service. That is: the state of the service should be
     * retrieved by requesting a GET; and the state of the service should be mutated by submitting a
     * PATCH, PUT or DELETE.
     */
    public void handleNodeGroupMaintenance(Operation post) {
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
            if (option == ServiceOption.OWNER_SELECTION) {
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

    /**
     * Value indicating whether GET on /available returns 200 or 503
     * The method is a convenience method since it relies on STAT_NAME_AVAILABLE to report
     * availability.
     */
    public void setAvailable(boolean isAvailable) {
        this.toggleOption(ServiceOption.INSTRUMENTATION, true);
        this.setStat(STAT_NAME_AVAILABLE, isAvailable ? STAT_VALUE_TRUE : STAT_VALUE_FALSE);
    }

    /**
     * Value indicating whether GET on /available returns 200 or 503
     */
    public boolean isAvailable() {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return true;
        }
        // processing stage must also indicate service is started
        if (this.stage != ProcessingStage.AVAILABLE && this.stage != ProcessingStage.PAUSED) {
            return false;
        }
        ServiceStat st = this.getStat(STAT_NAME_AVAILABLE);
        if (st != null && st.latestValue == STAT_VALUE_TRUE) {
            return true;
        }
        return false;
    }

    private void allocateUtilityService() {
        synchronized (this.options) {
            if (this.utilityService == null) {
                this.utilityService = new UtilityService().setParent(this);
            }
        }
    }

    @Override
    public void setHost(ServiceHost serviceHost) {
        this.host = serviceHost;
    }

    @Override
    public void setSelfLink(String path) {
        this.selfLink = path;
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

        getHost().processPendingServiceAvailableOperations(this, null, false);
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
        doLogging(Level.SEVERE, () -> Utils.toString(e));
    }

    public void logSevere(String fmt, Object... args) {
        doLogging(Level.SEVERE, () -> String.format(fmt, args));
    }

    public void logSevere(Supplier<String> messageSupplier) {
        doLogging(Level.SEVERE, messageSupplier);
    }

    public void logInfo(String fmt, Object... args) {
        doLogging(Level.INFO, () -> String.format(fmt, args));
    }

    public void logInfo(Supplier<String> messageSupplier) {
        doLogging(Level.INFO, messageSupplier);
    }

    public void logFine(String fmt, Object... args) {
        doLogging(Level.FINE, () -> String.format(fmt, args));
    }

    public void logFine(Supplier<String> messageSupplier) {
        doLogging(Level.FINE, messageSupplier);
    }

    public void logWarning(String fmt, Object... args) {
        doLogging(Level.WARNING, () -> String.format(fmt, args));
    }

    public void logWarning(Supplier<String> messageSupplier) {
        doLogging(Level.WARNING, messageSupplier);
    }

    public void log(Level level, String fmt, Object... args) {
        doLogging(level, () -> String.format(fmt, args));
    }

    public void log(Level level, Supplier<String> messageSupplier) {
        doLogging(level, messageSupplier);
    }

    protected void doLogging(Level level, Supplier<String> messageSupplier) {
        String uri = this.host != null && this.selfLink != null ? getUri().toString()
                : this.getClass().getSimpleName();
        Logger lg = Logger.getLogger(this.getClass().getName());
        Utils.log(lg, 3, uri, level, messageSupplier);
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

        if (micros > 0 && micros < Service.MIN_MAINTENANCE_INTERVAL_MICROS) {
            logWarning("Maintenance interval %d is less than the minimum interval %d"
                    + ", reducing to min interval", micros, Service.MIN_MAINTENANCE_INTERVAL_MICROS);
            micros = Service.MIN_MAINTENANCE_INTERVAL_MICROS;
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

    /**
     * @see #handleUiGet(String, Service, Operation)
     * @param get
     */
    protected void handleUiGet(Operation get) {
        handleUiGet(getSelfLink(), this, get);
    }

    /**
     * This method does basic URL rewriting and forwards to the Ui service.
     *
     * Every request to /some/service/FILE gets forwarded to
     * /user-interface/resources/${serviceClass}/FILE
     * @param get
     */
    protected void handleUiGet(String selfLink, Service ownerService, Operation get) {
        String requestUri = get.getUri().getPath();

        String uiResourcePath;

        ServiceDocumentDescription desc = ownerService.getDocumentTemplate().documentDescription;
        if (desc != null && desc.userInterfaceResourcePath != null) {
            uiResourcePath = UriUtils.buildUriPath(ServiceUriPaths.UI_RESOURCES,
                    desc.userInterfaceResourcePath);
        } else {
            uiResourcePath = Utils.buildUiResourceUriPrefixPath(ownerService);
        }

        if (selfLink.equals(requestUri)) {
            // no trailing /, redirect to a location with trailing /
            get.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
            get.addResponseHeader(Operation.LOCATION_HEADER, selfLink + UriUtils.URI_PATH_CHAR);
            get.complete();
            return;
        } else {
            String relativeToSelfUri = requestUri.substring(selfLink.length());
            if (relativeToSelfUri.equals(UriUtils.URI_PATH_CHAR)) {
                // serve the index.html
                uiResourcePath += UriUtils.URI_PATH_CHAR + ServiceUriPaths.UI_RESOURCE_DEFAULT_FILE;
            } else {
                // serve whatever resource
                uiResourcePath += relativeToSelfUri;
            }
        }

        // Forward request to the /user-interface service
        Operation operation = get.clone();
        operation.setUri(UriUtils.buildUri(getHost(), uiResourcePath))
                .setCompletion((o, e) -> {
                    get.setBody(o.getBodyRaw())
                            .setStatusCode(o.getStatusCode())
                            .setContentType(o.getContentType())
                            .complete();
                });

        getHost().sendRequest(operation);
    }

    /**
     * Records the handler invocation time for an operation if the instrumentation option is
     * set in the service.This method has to be called by the child class that extends the
     * StatelessService once operation processing starts in the handler.
     */
    public void setOperationHandlerInvokeTimeStat(Operation request) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        request.setHandlerInvokeTime(Utils.getNowMicrosUtc());
    }

    /**
     * Updates the operation duration stat using the handler invocation time and the current time
     * if the instrumentation option is set in the service.This method has to be called by the child
     * class that extends the StatelessService once processing is completed to report the statistics
     * for operation duration.
     */
    public void setOperationDurationStat(Operation request) {
        if (!hasOption(Service.ServiceOption.INSTRUMENTATION)) {
            return;
        }
        if (request.getInstrumentationContext() == null) {
            return;
        }
        setStat(request.getAction() + STAT_NAME_OPERATION_DURATION,
                Utils.getNowMicrosUtc()
                        - request.getInstrumentationContext().handleInvokeTimeMicrosUtc);

    }
}
