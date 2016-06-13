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

package com.vmware.xenon.services.common;

import java.net.URI;
import java.util.EnumSet;

import com.vmware.xenon.common.ODataUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

/**
 * Parses the OData parameters of the URI and issues a direct Query.
 */
public class ODataQueryService extends StatelessService {

    public static String SELF_LINK = ServiceUriPaths.ODATA_QUERIES;

    @Override
    public void handleGet(Operation op) {

        try {
            //paginated odata queries are forwarded to the page directly
            String skipTo = UriUtils.getODataSkipToParamValue(op.getUri());
            if (skipTo != null) {
                handleOdataPaginationCompletion(op, skipTo);
                return;
            }

            QueryTask task = ODataUtils.toQuery(op, true);
            if (task == null) {
                return;
            }

            sendRequest(Operation.createPost(this, ServiceUriPaths.CORE_QUERY_TASKS).setBody(task)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            op.fail(e);
                            return;
                        }

                        QueryTask result = o.getBody(QueryTask.class);
                        op.setBodyNoCloning(result);
                        op.complete();
                    }));
        } catch (Exception e) {
            op.fail(e);
        }
    }

    private void handleOdataPaginationCompletion(final Operation op, final String skipTo) {
        String node = UriUtils.getODataNodeParamValue(op.getUri());
        if (node == null) {
            op.fail(new IllegalArgumentException(
                    String.format(" When using %s , %s must be supplied",
                            UriUtils.URI_PARAM_ODATA_SKIP_TO,
                            UriUtils.URI_PARAM_ODATA_NODE)));
            return;
        }
        URI u = UriUtils.buildUri(getHost(),
                UriUtils.buildUriPath(ServiceUriPaths.CORE, "query-page", skipTo + ""));
        URI forwarderUri = UriUtils.buildForwardToPeerUri(u, node,
                ServiceUriPaths.DEFAULT_NODE_SELECTOR, EnumSet.noneOf(ServiceOption.class));
        String nextLink = forwarderUri.getPath() + UriUtils.URI_QUERY_CHAR
                + forwarderUri.getQuery();
        sendRequest(Operation.createGet(this, nextLink).setCompletion((o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            QueryTask qrt = o.getBody(QueryTask.class);
            op.setBodyNoCloning(qrt).complete();
        }));
    }

}
