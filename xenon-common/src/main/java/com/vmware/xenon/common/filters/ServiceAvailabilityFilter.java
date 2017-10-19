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

package com.vmware.xenon.common.filters;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceHost.ServiceAlreadyStartedException;
import com.vmware.xenon.common.ServiceHost.ServiceNotFoundException;
import com.vmware.xenon.common.UriUtils;

/**
 * This filter determines if the operation's target service is available to
 * serve requests or should be started on-demand.
 *
 * If the service is attached, it sticks it into the provided context for
 * subsequent filters to use.
 */
public class ServiceAvailabilityFilter implements Filter {

    @Override
    public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
        String servicePath = op.getUri().getPath();
        if (servicePath == null) {
            Operation.failServiceNotFound(op);
            return FilterReturnCode.FAILED_STOP_PROCESSING;
        }

        // re-use already looked-up service, if exists; otherwise, look it up
        Service service = context.getService();
        if (service == null) {
            service = context.getHost().findService(servicePath, false);
        }

        if (service != null && service.getProcessingStage() == ProcessingStage.AVAILABLE) {
            // service is already attached and available
            context.setService(service);
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        // service was not found in attached services or is not available -
        // we will regard that as a cache miss
        context.getHost().getServiceResourceTracker().updateCacheMissStats();

        if (ServiceHost.isHelperServicePath(servicePath)) {
            servicePath = UriUtils.getParentPath(servicePath);
        }

        if (service != null && ServiceHost.isServiceStarting(service.getProcessingStage())) {
            // service is in the process of starting - we will resume processing when
            // it's available
            Service finalService = service;
            op.nestCompletion(o -> {
                context.setService(finalService);
                context.getOpProcessingChain().resumeProcessingRequest(op, context);
            });

            context.getHost().registerForServiceAvailability(op, servicePath);
            return FilterReturnCode.SUSPEND_PROCESSING;
        }

        // service is not attached. maybe we should start it on demand.

        if (op.getAction() == Action.DELETE &&
                op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)) {
            // local stop - do not start on demand - complete and return
            op.complete();
            return FilterReturnCode.SUCCESS_STOP_PROCESSING;
        }

        String parentPath = UriUtils.getParentPath(servicePath);
        if (parentPath != null) {
            Service parentService = context.getHost().findService(parentPath, true);
            if (parentService != null && parentService.hasOption(ServiceOption.PERSISTENCE) &&
                    parentService instanceof FactoryService) {
                // Try to start the service on-demand.
                // Note that if this is a replicated request this will succeed and create an instance
                // regardless if the service already exists, which is what we want because replicated
                // requests need to be served using the request body.
                checkAndOnDemandStartService(op, servicePath, (FactoryService) parentService, context);
                return FilterReturnCode.SUSPEND_PROCESSING;
            }
        }

        Operation.failServiceNotFound(op);
        return FilterReturnCode.FAILED_STOP_PROCESSING;
    }

    private void checkAndOnDemandStartService(Operation op, String servicePath, FactoryService factoryService,
            OperationProcessingContext context) {
        ServiceHost host = context.getHost();

        host.log(Level.FINE, "(%d) ODL check for %s", op.getId(), servicePath);
        boolean doProbe = false;

        if (!factoryService.hasOption(ServiceOption.REPLICATION)
                && op.getAction() == Action.DELETE) {
            // do a probe (GET) to avoid starting a service on a DELETE request. We only do this
            // for non replicated services since its safe to do a local only probe. By doing a GET
            // first, we avoid the following race on local services:
            // DELETE -> starts service to determine if it exists
            // client issues POST for same self link while service is starting during ODL start
            // client sees conflict, even if the service never existed
            doProbe = true;
        }

        if (!doProbe) {
            host.log(Level.FINE, "Skipping probe - starting service %s on-demand due to %s %d (isFromReplication: %b, isSynchronizeOwner: %b, isSynchronizePeer: %b)",
                    servicePath, op.getAction(), op.getId(),
                    op.isFromReplication(), op.isSynchronizeOwner(), op.isSynchronizePeer());
            startServiceOnDemand(op, servicePath, factoryService, context);
            return;
        }

        // we should not use startService for checking if a service ever existed. This can cause a race with
        // a client POST creating the service for the first time, when they use
        // PRAGMA_QUEUE_FOR_AVAILABILITY. Instead do an attempt to load state for the service path
        Operation getOp = Operation
                .createGet(op.getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)
                .transferRefererFrom(op)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        context.getOpProcessingChain().resumedRequestFailed(op, context, e);
                        op.fail(e);
                        return;
                    }

                    if (!o.hasBody()) {
                        // the index will return success, but no body if service is not found
                        context.getOpProcessingChain().resumedRequestFailed(op, context,
                                new ServiceNotFoundException(op.getUri().getPath()));
                        Operation.failServiceNotFound(op);
                        return;
                    }

                    // service state exists, proceed with starting service
                    host.log(Level.FINE, "Starting service %s on-demand due to %s %d (isFromReplication: %b, isSynchronizeOwner: %b, isSynchronizePeer: %b)",
                            servicePath, op.getAction(), op.getId(),
                            op.isFromReplication(), op.isSynchronizeOwner(), op.isSynchronizePeer());
                    startServiceOnDemand(op, servicePath, factoryService, context);
                });

        Service indexService = host.getDocumentIndexService();
        if (indexService == null) {
            CancellationException e = new CancellationException("Index service is null");
            context.getOpProcessingChain().resumedRequestFailed(op, context, e);
            op.fail(e);
            return;
        }
        indexService.handleRequest(getOp);
    }

    private void startServiceOnDemand(Operation op, String servicePath, FactoryService factoryService,
            OperationProcessingContext context) {
        ServiceHost host = context.getHost();
        Operation onDemandPost = Operation.createPost(host, servicePath);

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (e instanceof CancellationException) {
                    // local stop of idle service raced with client request to load it. Retry.
                    host.log(Level.WARNING, "Stop of idle service %s detected, retrying",
                            op.getUri().getPath());
                    host.scheduleCore(() -> {
                        checkAndOnDemandStartService(op, servicePath, factoryService, context);
                    }, 1, TimeUnit.SECONDS);
                    return;
                }

                Action a = op.getAction();
                ServiceErrorResponse response = o.getErrorResponseBody();

                if (response != null) {
                    // Since we do a POST to start the service,
                    // we can get back a 409 status code i.e. the service has already been started or was
                    // deleted previously. Differentiate based on action, if we need to fail or succeed
                    if (response.statusCode == Operation.STATUS_CODE_CONFLICT) {
                        if (!ServiceHost.isServiceCreate(op)
                                && response.getErrorCode() == ServiceErrorResponse.ERROR_CODE_SERVICE_ALREADY_EXISTS) {
                            // service exists, action is not attempt to recreate, so complete as success
                            host.log(Level.WARNING,
                                    "Failed to start service %s because it already exists. Resubmitting request %s %d",
                                    servicePath, a, op.getId());
                            context.getOpProcessingChain().resumedRequest(op, context);
                            host.handleRequest(null, op);
                            return;
                        }

                        if (response.getErrorCode() == ServiceErrorResponse.ERROR_CODE_STATE_MARKED_DELETED) {
                            if (a == Action.DELETE) {
                                // state marked deleted, and action is to delete again, return success
                                context.getOpProcessingChain().resumedRequestCompleted(op, context);
                                op.complete();
                            } else if (a == Action.POST) {
                                // POSTs will fail with conflict since we must indicate the client is attempting a restart of a
                                // existing service.
                                context.getOpProcessingChain().resumedRequestFailed(op, context,
                                        new ServiceAlreadyStartedException(servicePath));
                                host.failRequestServiceAlreadyStarted(servicePath, null,
                                        op);
                            } else {
                                // All other actions fail with NOT_FOUND making it look like the service
                                // does not exist (or ever existed)
                                context.getOpProcessingChain().resumedRequestFailed(op, context,
                                        new ServiceNotFoundException(servicePath));
                                Operation.failServiceNotFound(op,
                                        ServiceErrorResponse.ERROR_CODE_STATE_MARKED_DELETED);
                            }
                            return;
                        }
                    }

                    // if the service we are trying to DELETE never existed, we swallow the 404 error.
                    // This is for consistency in behavior with services already resident in memory.
                    if (op.getAction() == Action.DELETE &&
                            response.statusCode == Operation.STATUS_CODE_NOT_FOUND) {
                        context.getOpProcessingChain().resumedRequestCompleted(op, context);
                        op.complete();
                        return;
                    }

                    if (response.statusCode == Operation.STATUS_CODE_NOT_FOUND) {
                        host.log(Level.WARNING,
                                "Failed to start service %s with 404 status code.", servicePath);
                        context.getOpProcessingChain().resumedRequestFailed(op, context,
                                new ServiceNotFoundException(servicePath));
                        Operation.failServiceNotFound(op);
                        return;
                    }
                }

                host.log(Level.SEVERE,
                        "Failed to start service %s with statusCode %d",
                        servicePath, o.getStatusCode());
                context.getOpProcessingChain().resumedRequestFailed(op, context,
                        new Exception("Failed with status code: " + o.getStatusCode()));
                op.setBodyNoCloning(o.getBodyRaw()).setStatusCode(o.getStatusCode());
                op.fail(e);
                return;
            }
            // proceed with handling original client request, service now started
            host.log(Level.FINE,
                    "Successfully started service %s. Resubmitting request %s %d",
                    servicePath, op.getAction(), op.getId());
            context.getOpProcessingChain().resumedRequest(op, context);
            host.handleRequest(null, op);
        };

        onDemandPost.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK)
                .transferRefererFrom(op)
                .setExpiration(op.getExpirationMicrosUtc())
                .setReplicationDisabled(true)
                .setCompletion(c);
        if (op.isSynchronizeOwner()) {
            onDemandPost.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER);
        }

        Service childService;
        try {
            childService = factoryService.createServiceInstance();
            childService.toggleOption(ServiceOption.FACTORY_ITEM, true);
        } catch (Throwable e1) {
            context.getOpProcessingChain().resumedRequestFailed(op, context, e1);
            op.fail(e1);
            return;
        }

        if (op.getAction() == Action.DELETE) {
            onDemandPost.disableFailureLogging(true);
            op.disableFailureLogging(true);
        }

        // bypass the factory, directly start service on host. This avoids adding a new
        // version to the index and various factory processes that are invoked on new
        // service creation
        host.startService(onDemandPost, childService);
    }

}
