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

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.ServiceHost.RequestRateInfo;
import com.vmware.xenon.common.ServiceHost.RequestRateInfo.Option;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.TimeBin;
import com.vmware.xenon.services.common.ServiceHostManagementService;

public class RequestRateLimitsFilter implements Filter {

    @Override
    public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
        if (op.isFromReplication() || op.isForwarded()) {
            // rate limiting is applied on the entry point host
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        if (!op.isRemote()) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        AuthorizationContext authCtx = op.getAuthorizationContext();
        if (authCtx == null) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        Claims claims = authCtx.getClaims();
        if (claims == null) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        String subject = claims.getSubject();
        if (subject == null) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        RequestRateInfo rateInfo = context.getHost().getRequestRateLimit(subject);
        if (rateInfo == null) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }


        synchronized (rateInfo) {
            rateInfo.timeSeries.add(Utils.getSystemNowMicrosUtc(), 0, 1);
            TimeBin mostRecentBin = rateInfo.timeSeries.bins
                    .get(rateInfo.timeSeries.bins.lastKey());
            if (mostRecentBin.sum < rateInfo.limit) {
                return FilterReturnCode.CONTINUE_PROCESSING;
            }
        }

        context.getHost().getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_RATE_LIMITED_OP_COUNT, 1);

        if (rateInfo.options.contains(Option.PAUSE_PROCESSING)) {
            // Add option as a hint to the request listener to throttle the channel associated with
            // the operation
            op.toggleOption(OperationOption.RATE_LIMITED, true);
        }

        if (!rateInfo.options.contains(Option.FAIL)) {
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        Operation.failLimitExceeded(op, ServiceErrorResponse.ERROR_CODE_HOST_RATE_LIMIT_EXCEEDED,
                "rate limit for " + op.getUri().getPath());
        /*
        Operation nextOp = s.dequeueRequest();
        if (nextOp != null) {
            run(() -> handleRequest(null, nextOp));
        }
        */
        return FilterReturnCode.FAILED_STOP_PROCESSING;
    }
}
