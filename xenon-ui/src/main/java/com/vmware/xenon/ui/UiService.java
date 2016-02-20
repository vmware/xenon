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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class UiService extends StatelessService {
    public static final String SELF_LINK = ServiceUriPaths.UI_SERVICE_CORE_PATH;

    public UiService() {
        super();
        toggleOption(ServiceOption.HTML_USER_INTERFACE, true);
    }

    @Override
    public void handleGet(Operation get) {
        String uriPath = get.getUri().getPath();
        String uriPrefixPath = Utils.buildUiResourceUriPrefixPath(this);

        if (uriPath.equals(ServiceUriPaths.UI_SERVICE_CORE_PATH)) {
            try {
                // redirect to /core/ui/default/ (trailing slash!)
                redirectGetToTrailingSlash(get, ServiceUriPaths.UI_SERVICE_CORE_PATH);
                return;
            } catch (UnsupportedEncodingException e) {
                get.fail(e);
                return;
            }
        } else if (uriPath.equals(ServiceUriPaths.UI_SERVICE_CORE_PATH + UriUtils.URI_PATH_CHAR)) {
            // serve index.html on /core/default/ui/
            proxyGetToCustomHtmlUiResource(get, uriPrefixPath + UriUtils.URI_PATH_CHAR
                    + ServiceUriPaths.UI_RESOURCE_DEFAULT_FILE);
            return;
        }

        // forward to what's after /core/ui/default/
        uriPath = uriPath.substring(ServiceUriPaths.UI_SERVICE_CORE_PATH.length());
        proxyGetToCustomHtmlUiResource(get, uriPrefixPath + uriPath);
    }

    public void proxyGetToCustomHtmlUiResource(Operation op, String htmlResourcePath) {
        Operation get = op.clone();
        get.setUri(UriUtils.buildUri(getHost(), htmlResourcePath))
                .setReferer(op.getReferer())
                .setCompletion((o, e) -> {
                    op.setBody(o.getBodyRaw())
                            .setContentType(o.getContentType())
                            .complete();
                });

        getHost().sendRequest(get);
        return;
    }

    /**
     * 302 redirect to the provided folderPath with a '/' appended
     * @param op
     * @param folderPath
     * @throws UnsupportedEncodingException
     */
    private void redirectGetToTrailingSlash(Operation op, String folderPath)
            throws UnsupportedEncodingException {
        op.addResponseHeader(Operation.LOCATION_HEADER,
                URLDecoder.decode(folderPath + UriUtils.URI_PATH_CHAR,
                        Utils.CHARSET));
        op.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
        op.setContentType(Operation.MEDIA_TYPE_TEXT_HTML);
        op.complete();
    }
}
