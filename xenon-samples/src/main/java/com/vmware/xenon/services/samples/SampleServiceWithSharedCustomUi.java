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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * This service provides the custom shared UI resources used when other services are loaded,
 * and served
 */
public class SampleServiceWithSharedCustomUi extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CUSTOM_UI_BASE_URL;

    public SampleServiceWithSharedCustomUi() {
        super();
        toggleOption(ServiceOption.HTML_USER_INTERFACE, true);

        // own the namespace so we can use relative URI to resources
        toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
    }

    /**
     * This method does basic URL rewriting and forwards to the Ui service.
     *
     * Every request to /core/custom/ui/FILE gets forwarded to
     * /user-interface/resources/com/vmware/xenon/services/samples/SampleServiceWithSharedCustomUi/FILE
     * @param get
     */
    @Override
    public void handleGet(Operation get) {
        String requestUri = get.getUri().getPath();

        String uiResourcePath = Utils.buildUiResourceUriPrefixPath(this);
        if (SELF_LINK.equals(requestUri)) {
            // no trailing /, redirect to a location with trailing /
            get.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
            get.addResponseHeader(Operation.LOCATION_HEADER, SELF_LINK + UriUtils.URI_PATH_CHAR);
            get.complete();
            return;
        } else {
            String relativeResourcePath = requestUri.substring(SELF_LINK.length());
            if (relativeResourcePath.equals(UriUtils.URI_PATH_CHAR)) {
                // serve the index.html
                uiResourcePath += UriUtils.URI_PATH_CHAR + ServiceUriPaths.UI_RESOURCE_DEFAULT_FILE;
            } else {
                // serve whatever resource
                uiResourcePath += relativeResourcePath;
            }
        }

        // Forward request to the /user-interface service
        Operation operation = get.clone();
        operation.setUri(UriUtils.buildUri(getHost(), uiResourcePath))
                .setReferer(get.getReferer())
                .setCompletion((o, e) -> {
                    get.setBody(o.getBodyRaw())
                            .setContentType(o.getContentType())
                            .complete();
                });

        getHost().sendRequest(operation);
    }
}
