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

import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalActiveSpanSource;
import org.junit.rules.ExternalResource;

import com.vmware.xenon.common.ServiceHost;

public class InjectMockTracer extends ExternalResource {
    TracerFactory factory;
    MockTracer tracer;

    @Override
    protected void after() {
        TracerFactory.factory = this.factory;
        super.after();
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        this.factory = TracerFactory.factory;
        MockTracer tracer = new MockTracer(new ThreadLocalActiveSpanSource(), MockTracer.Propagator.TEXT_MAP);
        this.tracer = tracer;
        TracerFactory.factory = new TracerFactory() {
            @Override
            public Tracer create(ServiceHost host) {
                return tracer;
            }

            @Override
            public boolean enabled() {
                return true;
            }
        };
    }
}
