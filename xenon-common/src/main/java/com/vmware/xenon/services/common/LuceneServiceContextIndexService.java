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
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.serialization.KryoSerializers;

public class LuceneServiceContextIndexService extends LuceneDocumentIndexService {
    public static class SerializedServiceContext extends ServiceDocument {
        public int sizeBytes;
        public Object instance;
    }

    public static Operation createGet(ServiceHost host, String key) {
        URI uri = UriUtils.buildUri(host, ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX,
                ServiceDocument.FIELD_NAME_SELF_LINK + "=" + key);
        return Operation.createGet(uri);
    }

    public static Operation createPost(ServiceHost host, Service s) {
        return Operation.createPost(host, SELF_LINK)
                .setBodyNoCloning(s);
    }

    public static final String SELF_LINK = ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX;
    public static final String FILE_PATH = "lucene-service-context-index";
    public static final long DEFAULT_EXPIRATION_MICROS = TimeUnit.DAYS.toMicros(1);

    private static final ServiceDocumentDescription DESCRIPTION =
            ServiceDocumentDescription.Builder.create().buildDescription(ServiceDocument.class);

    public LuceneServiceContextIndexService() {
        super(FILE_PATH);
    }

    @Override
    public void handleRequest(Operation op) {
        if (op.getAction() == Action.POST) {
            handlePost(op);
        } else {
            super.handleRequest(op);
        }
    }

    @Override
    public void handlePost(Operation post) {
        Service s = (Service) post.getBodyRaw();

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
                            s.getSelfLink(), size)));
            return;
        }

        // create a document as the main body since parent index service uses it to
        // determine what to index
        UpdateIndexRequest body = new UpdateIndexRequest();
        // FIXME we need to encode the buffer inside the service document, so deserialization
        // works: the super class, the doc index, assumes all state derives from ServiceDocument
        // including pre serialized state
        body.serializedDocument = buffer;
        body.document = new ServiceDocument();
        body.document.documentSelfLink = s.getSelfLink();
        body.document.documentUpdateAction = Action.POST.toString();
        body.document.documentExpirationTimeMicros = DEFAULT_EXPIRATION_MICROS;
        body.description = DESCRIPTION;
        post.setBodyNoCloning(body);
        post.setContentLength(size);
        super.handleRequest(post);
    }

}