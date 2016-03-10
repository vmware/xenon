/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.swagger;

import java.util.EnumSet;

import io.swagger.models.Info;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

/**
 */
public class SwaggerDescriptorService extends StatelessService {
    public static final String SELF_LINK = "/swagger";

    private Info info;

    public SwaggerDescriptorService() {
        super(ServiceDocument.class);
        toggleOption(ServiceOption.HTML_USER_INTERFACE, true);
        toggleOption(ServiceOption.ON_DEMAND_LOAD, true);
        toggleOption(ServiceOption.CONCURRENT_GET_HANDLING, true);
    }

    public static void startOn(ServiceHost host, SwaggerDescriptorService service) {
        Operation post = Operation.createPost(UriUtils.buildUri(host, SwaggerDescriptorService.class));
        host.startService(post, service);
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    @Override
    public void handleGet(Operation get) {
        Operation op = Operation.createGet(this, "/");
        op.setCompletion((o, e) -> {
            new SwaggerBuilder(this)
                    .info(this.info)
                    .services(o.getBody(ServiceDocumentQueryResult.class))
                    .serve(get);
        });

        getHost().queryServiceUris(EnumSet.allOf(ServiceOption.class), false, op);
    }
}
