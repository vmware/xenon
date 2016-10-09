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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRuntimeContext;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.serialization.KryoSerializers;

public class ServiceContextIndexService extends StatefulService {
    private static final String SERIALIZED_CONTEXT_FILE_EXTENSION = ".kryo";
    private static int indexFileCountThresholdForDelete = 100000;

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
    private Map<String, Long> linksPendingDelete = new ConcurrentSkipListMap<String, Long>();
    private Set<String> linksPendingCreate = new ConcurrentSkipListSet<String>();

    public ServiceContextIndexService() {
        super(ServiceDocument.class);
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

    @Override
    public void handlePost(Operation post) {
        ServiceRuntimeContext s = (ServiceRuntimeContext) post.getBodyRaw();
        this.linksPendingCreate.add(s.selfLink);
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

        // we must complete the operation after the file is closed, for it to be visible to
        // a GET operation
        post.setBody(null).complete();
        this.linksPendingCreate.remove(s.selfLink);
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

        if (this.linksPendingCreate.contains(link)) {
            logInfo("pending: %s", link);
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
            this.linksPendingDelete.put(src.selfLink, Utils.getNowMicrosUtc());
        } catch (Throwable ex) {
            get.fail(ex);
        }
    }

    @Override
    public void handleMaintenance(Operation post) {
        long now = Utils.getNowMicrosUtc();
        this.linksPendingCreate.forEach((link) -> {
            this.linksPendingDelete.remove(link);
        });

        int[] deletedCount = new int[1];
        // using size() on a concurrent skip list map is O(N) but its simpler than tracking
        // a separate counter, and we expect this list to remain small under normal load. Under
        // peak load it will be an expensive call, but it will be reduced after delete occurs
        if (this.linksPendingDelete.size() < indexFileCountThresholdForDelete) {
            return;
        }
        // there is an inherent race between trying to delete a file we think is not in use
        // and a new POST coming in to re-create it. In the future, since
        // ServiceOption.ON_DEMAND_LOAD will be required for pause/resume, worst case, we loose
        // soft state, but we can still load and start the service
        this.linksPendingDelete.forEach((link, time) -> {
            // ignore links recently touched, high risk we will collide with a POST
            if (Math.abs(time - now) < getHost().getMaintenanceIntervalMicros() * 10) {
                return;
            }
            if (this.linksPendingCreate.contains(link)) {
                return;
            }
            File f = getFileFromLink(link);
            try {
                Files.deleteIfExists(f.toPath());
            } catch (Throwable e) {

            }
            this.linksPendingDelete.remove(link);
            deletedCount[0]++;
            adjustStat(STAT_NAME_FILE_DELETE_COUNT, 1);
        });
        post.complete();
        if (deletedCount[0] == 0) {
            return;
        }
        logInfo("Files deleted: %d", deletedCount[0]);
    }

    private File getFileFromLink(String link) {
        String name = UriUtils.convertPathCharsFromLink(link) + SERIALIZED_CONTEXT_FILE_EXTENSION;
        return new File(this.indexDirectory, name);
    }

}