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

package com.vmware.dcp.services.samples;

import java.net.URI;
import java.util.EnumSet;
import java.util.List;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.QueryTask;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.dcp.services.common.ServiceUriPaths;

/**
 * Simplest version of echo service: Records a message (PUT); returns the version of the previous
 * recording (N-1 PUT) when called (GET). Consecutive PUTs or PATCHes just update the state.
 */
public class SamplePreviousEchoService extends StatefulService {
    public static class EchoServiceState extends ServiceDocument {
        public String message;
    }

    public SamplePreviousEchoService() {
        super(EchoServiceState.class);
        toggleOption(ServiceOption.PERSISTENCE, true);
    }

    /**
     * Replaces entire state with the request body.
     */
    @Override
    public void handlePut(Operation op) {
        super.setState(op, op.getBody(EchoServiceState.class));
        op.complete();
    }

    /**
     * Get the previous version (N-1).
     */
    @Override
    public void handleGet(Operation get) {
        QueryTask.QuerySpecification q = new QueryTask.QuerySpecification();
        q.options = EnumSet.of(QueryOption.EXPAND_CONTENT, QueryOption.INCLUDE_ALL_VERSIONS);
        q.query.setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                .setTermMatchValue(getSelfLink());
        QueryTask task = QueryTask.create(q).setDirect(true);
        URI uri = UriUtils.buildUri(getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
        Operation startPost = Operation
                .createPost(uri)
                .setBody(task)
                .setCompletion((o, f) -> handleQueryCompletion(o, f, get));
        sendRequest(startPost);
    }

    private void handleQueryCompletion(Operation o, Throwable f, Operation original) {
        if (f != null) {
            logInfo(f.toString());
            original.fail(f);
            return;
        }
        QueryTask response = o.getBody(QueryTask.class);
        // First check how many versions there are
        if (response.results.documentLinks.size() <= 1) {
            //  If no more than a single version, handle as you would normally do.
            super.handleGet(original);
            return;
        }
        // Whereas, if more than a single version, fetch the previous one
        List<String> dl = response.results.documentLinks;
        String penultimate = dl.get(1);
        Object obj = response.results.documents.get(penultimate);
        EchoServiceState s = Utils.fromJson((String) obj, EchoServiceState.class);
        original.setBody(s).complete();
    }
}
