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

package com.vmware.xenon.host;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

import java.util.logging.Level;

/**
 * Created by dars on 8/9/17.
 */
public class MyIngestionService extends StatelessService {

    public static final String SELF_LINK = "/my-ingest";

    public MyIngestionService() {
        super();
        super.toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
    }


    @Override
    public void handleGet(Operation get) {
        String path = UriUtils.getLastPathSegment(get.getUri().getPath());
        Operation exampleGet = Operation
                .createGet(getHost(), "/core/examples/" + path)
                .setReferer(getSelfLink())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        log(Level.SEVERE, "GET failed");
                        return;
                    }
                    log(Level.INFO, "GET succeeded.");
                    try {
                        Thread.sleep(200000);
                    } catch (Exception ex) {
                        log(Level.INFO, "blah blah");
                    }
                });
        sendRequest(exampleGet);
        get.setBody("Done...");
        get.complete();
    }
}
