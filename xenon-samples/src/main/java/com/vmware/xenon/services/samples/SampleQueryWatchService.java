/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

import java.util.logging.Level;

/**
 * Provides an implementation of watch service.
 */

public class SampleQueryWatchService extends QueryWatchService {

    public static final String ID = "echo-watch-singleton";

    public SampleQueryWatchService() {
        super();
    }

    public String getId() {
        return ID;
    }

    public QueryTask createContinuousQuery() {
        QueryTask.Query query = QueryTask.Query.Builder.create()
                .addKindFieldClause(SamplePreviousEchoService.EchoServiceState.class)
                .build();

        QueryTask queryTask = QueryTask.Builder.create()
                .addOption(QueryOption.EXPAND_CONTENT)
                .addOption(QueryOption.CONTINUOUS)
                .setQuery(query).build();
        return queryTask;
    }

    public void processResults(Operation op) {
        QueryTask body = op.getBody(QueryTask.class);

        if (body.results == null || body.results.documentLinks.isEmpty()) {
            return;
        }

        for (Object doc : body.results.documents.values()) {
            SamplePreviousEchoService.EchoServiceState state =
                    Utils.fromJson(doc, SamplePreviousEchoService.EchoServiceState.class);
            getHost().log(Level.INFO, "Message: %s, Action: %s", state.message, state.documentUpdateAction);
        }
    }
}