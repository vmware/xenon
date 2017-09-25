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

import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.AuthUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;

public class AuthenticationFilter implements Filter {

    @Override
    public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
        if (!context.getHost().isAuthorizationEnabled()) {
            // authorization is disabled
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        if (op.getAuthorizationContext() != null) {
            // authz context already established
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        if (BasicAuthenticationUtils.getAuthToken(op) != null) {
            // authn token already exists
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        // If the op targets a valid authentication service, allow it to proceed
        URI authServiceUri = context.getHost().getAuthenticationServiceUri();
        if (authServiceUri != null
                && authServiceUri.getPath().equals(op.getUri().getPath())) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        URI basicAuthServiceUri = context.getHost().getBasicAuthenticationServiceUri();
        if (basicAuthServiceUri != null
                && basicAuthServiceUri.getPath().equals(op.getUri().getPath())) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        // Dispatch the operation to the authentication service for handling.
        Service authnService = context.getHost().getAuthenticationService();
        long dispatchTime = System.nanoTime();
        op.nestCompletion((o, e) -> {
            if (authnService.hasOption(ServiceOption.INSTRUMENTATION)) {
                long dispatchDuration = System.nanoTime() - dispatchTime;
                AuthUtils.setAuthDurationStat(authnService,
                        AuthenticationConstants.STAT_NAME_DURATION_MICROS_PREFIX,
                        TimeUnit.NANOSECONDS.toMicros(dispatchDuration));
            }

            if (e != null) {
                context.getOpProcessingChain().resumedRequestFailed(op, context, e);
                op.setBodyNoCloning(o.getBodyRaw())
                        .setStatusCode(o.getStatusCode()).fail(e);
                return;
            }

            // If the status code was anything but 200, and the operation
            // was marked as failed, terminate the processing chain
            if (o.getStatusCode() != Operation.STATUS_CODE_OK) {
                context.getOpProcessingChain().resumedRequestFailed(op, context,
                        new SecurityException());
                op.setBodyNoCloning(o.getBodyRaw())
                        .setStatusCode(o.getStatusCode()).complete();
                return;
            }

            // authentication success - proceed to the next filter
            context.getOpProcessingChain().resumeProcessingRequest(op, context);
        });

        // TODO: fix BasicAuthenticationService and just send it a POST
        context.getHost().queueOrScheduleRequest(authnService, op);
        return FilterReturnCode.SUSPEND_PROCESSING;
    }

}
