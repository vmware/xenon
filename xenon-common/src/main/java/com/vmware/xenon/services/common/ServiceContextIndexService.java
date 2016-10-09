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
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Map;

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
    private static final String SERIALIZED_CONTEXT_FILE_EXTENSION = ".kryo";

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
        if (!createIndexDirectory(post)) {
            return;
        }
        post.complete();
    }

    private boolean createIndexDirectory(Operation post) {
        if (this.indexDirectory.exists()) {
            return true;
        }
        if (this.indexDirectory.mkdir()) {
            return true;
        }
        logWarning("Failure creating index directory %s, failing start", this.indexDirectory);
        post.fail(new IOException("could not create " + this.indexDirectory));
        return false;
    }

    @Override
    public void handlePost(Operation post) {
        ServiceRuntimeContext s = (ServiceRuntimeContext) post.getBodyRaw();
        ByteBuffer bb = KryoSerializers.serializeObject(s, Service.MAX_SERIALIZED_SIZE_BYTES);
        File serviceContextFile = getFileFromLink(s.selfLink);
        FileUtils.writeFileAndComplete(post, bb, serviceContextFile);
    }

    @Override
    public void handleGet(Operation get) {
        Map<String, String> queryParams = UriUtils.parseUriQueryParams(get.getUri());
        String link = queryParams.get(ServiceDocument.FIELD_NAME_SELF_LINK);
        if (link == null) {
            get.fail(new IllegalArgumentException(ServiceDocument.FIELD_NAME_SELF_LINK
                    + " is required URI query parameter"));
            return;
        }
        File serviceContextFile = getFileFromLink(link);
        if (!serviceContextFile.exists()) {
            get.setBody(null).complete();
            return;
        }

        get.nestCompletion((o, e) -> {
            if (e != null && e instanceof NoSuchFileException) {
                get.setBody(null).complete();
                return;
            }
            try {
                byte[] data = (byte[]) o.getBodyRaw();
                ServiceRuntimeContext src = (ServiceRuntimeContext) KryoSerializers
                        .deserializeObject(data, 0, (int) o.getContentLength());
                get.setBodyNoCloning(src).complete();
            } catch (Throwable ex) {
                o.fail(ex);
            }
        });

        FileUtils.readFileAndComplete(get, serviceContextFile);
        serviceContextFile.deleteOnExit();
    }

    private File getFileFromLink(String link) {
        String name = UriUtils.convertPathCharsFromLink(link) + SERIALIZED_CONTEXT_FILE_EXTENSION;
        return new File(this.indexDirectory, name);
    }

}