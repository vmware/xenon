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

package com.vmware.xenon.services.samples;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.config.XenonConfiguration;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Service to generate unique ID ranges that consumer service uses to generate IDs
 * from supplied range. Each range is returned once, and never gets repeated.
 */
public class IdRangeService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.SAMPLES
            + "/unique-id-range";
    static final int RANGE_INTERVAL = XenonConfiguration.integer(
            IdRangeService.class,
            "RANGE_INTERVAL",
            100000
    );

    static final String RANGE_INTERVAL_QUERY_PARAM = "rangeInterval";

    private static final AtomicLong maxId = new AtomicLong(0);

    public static class State extends ServiceDocument{
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public long minId;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public long maxId;
    }

    public static FactoryService createFactory() {
        return FactoryService.create(IdRangeService.class);
    }

    public IdRangeService() {
        super(State.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    @Override
    public void handlePatch(Operation op) {
        op.complete();
    }

    @Override
    public void handleGet(Operation get) {
        int rangeInterval = RANGE_INTERVAL;
        URI uri = get.getUri();

        // Caller can pass rangeInterval as query parameter to override default value.
        if (uri.getQuery() != null) {
            Map<String, String> queryParams = UriUtils.parseUriQueryParams(uri);
            String value = queryParams.get(RANGE_INTERVAL_QUERY_PARAM);
            if (value != null) {
                rangeInterval = Integer.parseInt(value);
            }
        }

        long maximumId = maxId.addAndGet(rangeInterval);
        State body = new State();
        body.minId = maximumId - rangeInterval + 1;
        body.maxId = maximumId;
        Operation patch = Operation.createPatch(this.getUri()).setBody(body);

        // Commit this new range before returning to the caller.
        DeferredResult<Operation> deferredResult = this.sendWithDeferredResult(patch);
        deferredResult.whenComplete((op, e)-> {
            if (e != null) {
                get.fail(e);
                return;
            }
            State state = op.getBody(State.class);
            get.setBody(state);
            get.complete();
        });
    }
}
