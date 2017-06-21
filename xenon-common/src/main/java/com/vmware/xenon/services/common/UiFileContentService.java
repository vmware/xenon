/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Use this class to to host your custom ui at any uri. Subclasses should define
 * a SELF_LINK static field for the prefered uri. This service behaves like a web server
 * serving static files from a directory.
 */
public abstract class UiFileContentService extends StatelessService {
    public UiFileContentService() {
        super();
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleStart(Operation startPost) {
        try {
            Path baseResourcePath = Utils.getServiceUiResourcePath(this);
            Path baseUriPath = Paths.get(getSelfLink());
            String prefix = baseResourcePath.toString().replace('\\', '/');
            Map<Path, String> pathToURIPath = new HashMap<>();

            if (getHost().getState().resourceSandboxFileReference != null) {
                getHost().discoverFileResources(this, pathToURIPath, baseUriPath, prefix);
            }

            if (pathToURIPath.isEmpty()) {
                getHost().discoverJarResources(baseResourcePath, this, pathToURIPath, baseUriPath, prefix);
            }

            if (pathToURIPath.isEmpty()) {
                log(Level.WARNING, "No custom UI resources found for %s", this.getClass().getName());
            }

            for (Entry<Path, String> e : pathToURIPath.entrySet()) {
                String value = e.getValue();
                Operation post = Operation.createPost(UriUtils.buildUri(getHost(), value));
                FileContentService fcs = new FileContentService(e.getKey().toFile());
                getHost().startService(post, fcs);
            }
        } catch (Exception e) {
            log(Level.WARNING, "Error enumerating UI resources for %s: %s", this.getSelfLink(),
                    Utils.toString(e));
        }

        super.handleStart(startPost);
    }

    @Override
    public void handleGet(Operation get) {
        String selfLink = getSelfLink();
        URI uri = get.getUri();
        String requestUri = uri.getPath();

        if (selfLink.equals(requestUri) && !UriUtils.URI_PATH_CHAR.equals(requestUri)) {
            // no trailing /, redirect to a location with trailing /
            get.setStatusCode(Operation.STATUS_CODE_MOVED_TEMP);
            get.addResponseHeader(Operation.LOCATION_HEADER, selfLink + UriUtils.URI_PATH_CHAR);
            get.complete();
            return;
        }

        String uiResourcePath = selfLink + UriUtils.URI_PATH_CHAR + ServiceUriPaths.UI_RESOURCE_DEFAULT_FILE;
        Operation operation = get.clone();
        operation.setUri(UriUtils.buildUri(getHost(), uiResourcePath, uri.getQuery()))
                .setCompletion((o, e) -> {
                    get.setBody(o.getBodyRaw())
                            .setStatusCode(o.getStatusCode())
                            .setContentType(o.getContentType());
                    if (e != null) {
                        get.fail(e);
                    } else {
                        get.complete();
                    }
                });

        getHost().sendRequest(operation);
    }
}
