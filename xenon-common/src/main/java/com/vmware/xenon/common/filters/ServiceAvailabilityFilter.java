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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ProcessingStage;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;

/**
 * This filter determines if the operation's target service is available to serve requests
 * or should be started.
 *
 * If the service is attached, it sticks it into the provided
 * context for subsequent filters to use.
 */
public class ServiceAvailabilityFilter implements Filter {

    @Override
    public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
        String servicePath = op.getUri().getPath();
        if (servicePath == null) {
            Operation.failServiceNotFound(op);
            return FilterReturnCode.FAILED_STOP_PROCESSING;
        }

        Service service = context.getHost().findService(servicePath, false);

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

        if ((service != null && ServiceHost.isServiceStarting(service.getProcessingStage())) ||
                op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)) {
            // service is in the process of starting - we will resume processing when
            // it's available
            op.nestCompletion(o -> {
                // TODO: why do we need this?
                op.setTargetReplicated(false);
                context.setService(service);
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

        /*
        if (op.isFromReplication()) {
            // TODO: why do we need this replication retry scheme, instead of falling through to
            // checkAndOnDemandStartService?

            // If this is a replicated update we fail the request with 404 - NOT FOUND error.
            if (op.isUpdate()) {
                context.getHost().log(Level.WARNING, "Service %s is not available. Failing replication request",
                        op.getUri().getPath());

                IllegalStateException ex = new IllegalStateException("Service not found on replica");
                Operation.fail(op, Operation.STATUS_CODE_NOT_FOUND,
                        ServiceErrorResponse.ERROR_CODE_SERVICE_NOT_FOUND_ON_REPLICA, ex);
                return FilterReturnCode.FAILED_STOP_PROCESSING;
            }
        }
        */

        String parentPath = UriUtils.getParentPath(servicePath);
        if (parentPath != null) {
            Service parentService = context.getHost().findService(parentPath, true);
            if (parentService != null && parentService.hasOption(ServiceOption.PERSISTENCE)) {
                // the service might be stopped
                // TODO: why does checkAndOnDemandStartService() return a value, and why not
                // centralizing this logic in this filter (including resume processing)?
                if (context.getHost().getServiceResourceTracker().checkAndOnDemandStartService(op)) {
                    /*
                    op.nestCompletion(o -> {
                        // TODO: why do we need this?
                        op.setTargetReplicated(false);
                        context.setService(service);
                        context.getOpProcessingChain().resumeProcessingRequest(op, context);
                    });
                    */

                    //context.getHost().registerForServiceAvailability(op, servicePath);
                    return FilterReturnCode.SUSPEND_PROCESSING;
                }
            }
        }

        Operation.failServiceNotFound(op);
        return FilterReturnCode.FAILED_STOP_PROCESSING;
    }

}
