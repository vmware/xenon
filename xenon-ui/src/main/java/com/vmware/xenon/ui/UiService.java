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

package com.vmware.xenon.ui;

import java.util.EnumSet;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.UiContentService;

public class UiService extends UiContentService {
    public static final String SELF_LINK = ServiceUriPaths.UI_SERVICE_CORE_PATH;

    public static class QueryRequest extends ServiceDocument {
        public static final String KIND = Utils.buildKind(QueryRequest.class);

        public String kind;
    }

    @Override
    public void authorizeRequest(Operation op) {
        // default UI is a single page app and should be publicly accessible.
        op.complete();
    }

    @Override
    public void handlePost(Operation post) {
        if (!post.hasBody()) {
            ServiceHost.failRequestActionNotSupported(post);
            return;
        }

        QueryRequest body = post.getBody(QueryRequest.class);

        if (!QueryRequest.KIND.equals(body.kind)) {
            post.fail(new IllegalArgumentException("kind is not recognized"));
            return;
        }

        EnumSet<ServiceOption> options = EnumSet.of(ServiceOption.FACTORY);
        getHost().queryServiceUris(options, false, post);
    }
}