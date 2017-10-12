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

package com.vmware.xenon.common.opentracing;

import java.util.logging.Logger;

import com.uber.jaeger.Configuration;
import io.opentracing.Tracer;

import com.vmware.xenon.common.ServiceHost;

public class Jaeger implements TracerFactoryInterface {
    @Override
    public Tracer create(ServiceHost host) {
        Logger logger = Logger.getLogger(getClass().getName());
        logger.info("Opentracing support using Jaeger");
        Configuration config = Configuration.fromEnv();
        return config.getTracer();
    }
}
