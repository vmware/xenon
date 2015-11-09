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

package com.vmware.dcp.services.common;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatelessService;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;

public class BroadcastQueryPageService extends StatelessService {
    public static final String SELF_LINK_PREFIX = "broadcast-query-page";
    public static final String KIND = Utils.buildKind(QueryTask.class);

    private QueryTask.QuerySpecification spec;
    private String documentSelfLink;
    private List<String> indexLinks;

    public BroadcastQueryPageService(QueryTask.QuerySpecification spec, List<String> indexLinks) {
        super(QueryTask.class);
        this.spec = spec;
        this.indexLinks = indexLinks;
    }

    @Override
    public void handleStart(Operation post) {
        ServiceDocument initState = post.getBody(ServiceDocument.class);

        this.documentSelfLink = initState.documentSelfLink;

        long interval = initState.documentExpirationTimeMicros - Utils.getNowMicrosUtc();
        if (interval < 0) {
            logWarning("Task expiration is in the past, extending it");
            interval = TimeUnit.SECONDS.toMicros(getHost().getMaintenanceIntervalMicros() * 2);
        }

        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.setMaintenanceIntervalMicros(interval);

        post.complete();
    }

    @Override
    public void handleGet(Operation get) {
        String indexLink = indexLinks.get(0);

        Operation op = Operation
                .createGet(UriUtils.buildUri(indexLink))
                .setCompletion((o, e) -> {
                    if (e == null) {
                        QueryTask rsp = o.getBody(QueryTask.class);
                        System.out.println(rsp.toString());
                    }
                });
        this.getHost().sendRequest(get);
    }

    @Override
    public void handleMaintenance(Operation op) {
        op.complete();

        getHost().stopService(this);
    }
}
