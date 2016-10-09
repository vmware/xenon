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

import java.io.File;
import java.net.URI;

import com.vmware.xenon.common.FileUtils;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRuntimeContext;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.serialization.KryoSerializers;

public class ServiceContextIndexService extends StatelessService {
    public static Operation createGet(ServiceHost host, String key) {
        URI uri = UriUtils.buildUri(host, ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX,
                ServiceDocument.FIELD_NAME_SELF_LINK + "=" + key);
        return Operation.createGet(uri);
    }

    public static final String SELF_LINK = ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX;
    public static final String FILE_PATH = "service-context-index";

    private File indexDirectory;

    public ServiceContextIndexService() {

    }

    @Override
    public void handleStart(Operation post) {
        this.indexDirectory = new File(new File(getHost().getStorageSandbox()), FILE_PATH);
        post.complete();
    }

    @Override
    public void handlePost(Operation post) {
        ServiceRuntimeContext s = (ServiceRuntimeContext) post.getBodyRaw();

        int retryLimit = 3;
        byte[] buffer = null;
        int size = Service.MAX_SERIALIZED_SIZE_BYTES;
        while (--retryLimit > 0) {
            buffer = KryoSerializers.getBuffer(size);
            try {
                size = KryoSerializers.serializeObject(s, buffer, 0);
                break;
            } catch (Throwable e) {
                size *= 2;
                continue;
            }
        }

        if (retryLimit <= 0) {
            post.fail(new IllegalStateException(
                    String.format("Failure serialized service %s, size larger than %d",
                            s.selfLink, size)));
            return;
        }

        File serviceContextFile = new File(this.indexDirectory,
                UriUtils.convertPathCharsFromLink(s.selfLink));
        FileUtils.writeFileAndComplete(post, serviceContextFile);
    }

}