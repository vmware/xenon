/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.StringWriter;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;

public class PrometheusExporterService extends StatelessService {

    public static final String SELF_LINK = "/metrics";

    @Override
    public void handleGet(Operation get) {
        CollectorRegistry collectorRegistry = getHost().getCollectorRegistry();
        StringWriter writer = new StringWriter();
        try {
            TextFormat.write004(writer, collectorRegistry.metricFamilySamples());
        } catch (IOException e) {
            get.fail(e);
            return;
        }

        get.setContentType(TextFormat.CONTENT_TYPE_004);
        get.setBodyNoCloning(writer.toString());
        get.complete();
    }
}
