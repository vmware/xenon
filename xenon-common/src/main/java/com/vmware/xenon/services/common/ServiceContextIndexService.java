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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRuntimeContext;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.serialization.KryoSerializers;

public class ServiceContextIndexService extends StatefulService {
    private static final String SERIALIZED_CONTEXT_FILE_EXTENSION = ".kryo";
    private static int indexFileCountThresholdForDelete = 1000;

    public static Operation createGet(ServiceHost host, String key) {
        URI uri = UriUtils.buildUri(host, ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX,
                ServiceDocument.FIELD_NAME_SELF_LINK + "=" + key);
        return Operation.createGet(uri);
    }

    public static final String STAT_NAME_FILE_DELETE_COUNT = "fileDeleteCount";
    public static final String SELF_LINK = ServiceUriPaths.CORE_SERVICE_CONTEXT_INDEX;
    public static final String FILE_PATH = "service-context-index";

    public static void setIndexFileCountThresholdForDelete(int count) {
        indexFileCountThresholdForDelete = count;
    }

    private File indexDirectory;
    private Set<String> linksPendingDelete = new ConcurrentSkipListSet<String>();
    private AtomicInteger pendingDeleteCount = new AtomicInteger();

    public ServiceContextIndexService() {
        super(ServiceDocument.class);
        super.setOperationQueueLimit(OPERATION_QUEUE_DEFAULT_LIMIT * 10);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
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

    /**
     * Creates a file on disk with the serialized service state. The method uses
     * synchronous file I/O in the context of the shared host dispatcher which is normally
     * a very bad idea. However, since service pause/resume can overwhelm the host, this is
     * a natural way to throttle client requests
     */
    @Override
    public void handlePost(Operation post) {
        ServiceRuntimeContext s = (ServiceRuntimeContext) post.getBodyRaw();
        ByteBuffer bb = KryoSerializers.serializeObject(s, Service.MAX_SERIALIZED_SIZE_BYTES);
        File serviceContextFile = getFileFromLink(s.selfLink);

        OutputStream output = null;
        try {
            output = new BufferedOutputStream(new FileOutputStream(serviceContextFile));
            output.write(bb.array(), bb.position(), bb.limit());
        } catch (Throwable e) {
            post.fail(e);
            return;
        } finally {
            try {
                output.close();
            } catch (IOException e) {
            }
        }

        if (this.linksPendingDelete.remove(s.selfLink)) {
            this.pendingDeleteCount.decrementAndGet();
        }

        // we must complete the operation after the file is closed, for it to be visible to
        // a GET operation
        post.setBody(null).complete();
    }

    /**
     * Reads a file on disk associated with the service link, containing serialized service state.
     * See {@link #handlePost(Operation)} regarding use of synchronous file I/O
     */
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

        try {
            byte[] data = Files.readAllBytes(serviceContextFile.toPath());
            if (data == null || data.length == 0) {
                get.setBody(null).complete();
                return;
            }
            ServiceRuntimeContext src = (ServiceRuntimeContext) KryoSerializers
                    .deserializeObject(data, 0, data.length);
            get.setBodyNoCloning(src).complete();
            this.linksPendingDelete.add(src.selfLink);
            this.pendingDeleteCount.incrementAndGet();
        } catch (Throwable ex) {
            get.fail(ex);
        }
    }

    /**
     * Deletes files for services that have resumed and are no longer needed. The
     * PATCH is normally sent by the maintenance handler, and runs in the atomic
     * update context of the service, avoiding races with service pause
     */
    @Override
    public void handlePatch(Operation patch) {
        try {
            if (this.pendingDeleteCount.get() < 0) {
                this.pendingDeleteCount.set(this.linksPendingDelete.size());
            }
            if (this.pendingDeleteCount.get() < indexFileCountThresholdForDelete) {
                return;
            }

            int[] deletedCount = new int[1];
            logInfo("Files to delete: %d", this.pendingDeleteCount.get());
            this.linksPendingDelete.forEach((link) -> {
                File f = getFileFromLink(link);
                try {
                    Files.deleteIfExists(f.toPath());
                } catch (Throwable e) {

                }
                this.linksPendingDelete.remove(link);
                this.pendingDeleteCount.decrementAndGet();
                deletedCount[0]++;
                adjustStat(STAT_NAME_FILE_DELETE_COUNT, 1);
            });

            if (deletedCount[0] == 0) {
                return;
            }
            logInfo("Files deleted: %d", deletedCount[0]);
            this.pendingDeleteCount.set(this.linksPendingDelete.size());
        } finally {
            patch.complete();
        }
    }

    @Override
    public void handleMaintenance(Operation post) {
        Operation.createPatch(getUri())
                .setBody(new ServiceDocument())
                .setCompletion((o, e) -> {
                    post.complete();
                }).sendWith(this);
    }

    private File getFileFromLink(String link) {
        String name = UriUtils.convertPathCharsFromLink(link) + SERIALIZED_CONTEXT_FILE_EXTENSION;
        return new File(this.indexDirectory, name);
    }

}