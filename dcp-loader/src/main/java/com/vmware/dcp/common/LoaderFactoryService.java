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

package com.vmware.dcp.common;

import java.util.HashMap;

import com.vmware.dcp.common.LoaderService.LoaderServiceState;
import com.vmware.dcp.services.common.ServiceUriPaths;

public class LoaderFactoryService extends FactoryService {
    public static final String SELF_LINK = UriUtils.buildUriPath(ServiceUriPaths.CORE, "loader");

    public LoaderFactoryService() {
        super(LoaderServiceState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new LoaderService();
    }

    public static Operation createDefaultPostOp(
            ServiceHost host) {
        LoaderServiceState d = new LoaderServiceState();
        d.documentSelfLink = LoaderService.FILESYSTEM_DEFAULT_GROUP;
        d.loaderType = LoaderService.LoaderType.FILESYSTEM;
        d.path = LoaderService.FILESYSTEM_DEFAULT_PATH;
        d.servicePackages = new HashMap<String, LoaderService.LoaderServiceInfo>();
        return Operation
                .createPost(UriUtils.buildUri(host, LoaderFactoryService.class))
                .setBody(d);
    }

}
