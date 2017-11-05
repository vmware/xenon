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

import static org.junit.Assert.assertTrue;

import brave.opentracing.BraveTracer;
import io.opentracing.NoopTracer;
import io.opentracing.Tracer;
import org.junit.Test;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.config.TestXenonConfiguration;
import com.vmware.xenon.common.test.VerificationHost;

public class TracerFactoryTest {

    @Test
    public void defaultIsNoop() throws Exception {
        TestXenonConfiguration.override(TracerFactory.class, "provider", TracerFactory.IMPL_NONE);
        ServiceHost h = VerificationHost.create(0);
        Tracer tracer = TracerFactory.factory.create(h);
        assertTrue(tracer instanceof NoopTracer);
    }

    @Test
    public void jaegerGetsJaeger() throws Exception {
        TestXenonConfiguration.override(TracerFactory.class, "provider", TracerFactory.IMPL_JAEGER);
        System.setProperty("JAEGER_SERVICE_NAME", "test");
        ServiceHost h = VerificationHost.create(0);
        Tracer tracer = TracerFactory.factory.create(h);
        assertTrue(tracer instanceof com.uber.jaeger.Tracer);
    }

    @Test
    public void zipkinGetsZipkin() throws Exception {
        TestXenonConfiguration.override(TracerFactory.class, "provider", TracerFactory.IMPL_ZIPKIN);
        System.setProperty("ZIPKIN_URL", "http://host/api/v1/spans/");
        System.setProperty("ZIPKIN_SERVICE_NAME", "test");

        ServiceHost h = VerificationHost.create(0);
        Tracer tracer = TracerFactory.factory.create(h);
        assertTrue(tracer instanceof BraveTracer);
    }

    @Test(expected = RuntimeException.class)
    public void invalidTracer() throws Exception {
        TestXenonConfiguration.override(TracerFactory.class, "provider", "random junk");
        ServiceHost h = VerificationHost.create(0);
        TracerFactory.factory.create(h);
    }
}
