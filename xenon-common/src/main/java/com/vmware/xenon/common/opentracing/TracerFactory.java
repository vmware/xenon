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

import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentracing.NoopTracerFactory;
import io.opentracing.Tracer;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.config.XenonConfiguration;

public class TracerFactory {
    public static final String IMPL_JAEGER = "jaeger";
    public static final String IMPL_ZIPKIN = "zipkin";
    /**
     * Singleton: may be replaced to customise implicit tracer creation - e.g. to add support for
     * a different OpenTracing implementation.
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("MS_SHOULD_BE_FINAL")
    public static TracerFactory factory = new TracerFactory();

    private boolean enabled;
    private boolean enableChecked;

    /**
     * Create a {@link io.opentracing.Tracer} for use by a {@link com.vmware.xenon.common.ServiceHost}.
     *
     * See README.md for the configuration variables for this factory. The default implementation does
     * not perform any host-specific customisations.
     *
     * @return A {@link io.opentracing.Tracer} instance for tracing the given {@link com.vmware.xenon.common.ServiceHost}
     * If tracing is not enabled, them a {@link io.opentracing.NoopTracer} is returned.
     */
    @SuppressWarnings("unchecked")
    public synchronized Tracer create(ServiceHost host) {
        Logger logger = Logger.getLogger(getClass().getName());
        String impl = XenonConfiguration.string(TracerFactory.class, "provider", null);

        if (impl == null) {
            return NoopTracerFactory.create();
        }
        impl = impl.toLowerCase();

        Class<TracerFactoryInterface> factoryClass;
        try {
            if (impl.equals(IMPL_JAEGER)) {
                factoryClass = (Class<TracerFactoryInterface>) Class
                        .forName("com.vmware.xenon.common.opentracing.Jaeger");
            } else if (impl.equals(IMPL_ZIPKIN)) {
                factoryClass = (Class<TracerFactoryInterface>) Class
                        .forName("com.vmware.xenon.common.opentracing.Zipkin");
            } else {
                throw new RuntimeException(String.format("Bad tracer type %s", impl));
            }
        } catch (ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Failed to load impl class", e);
            throw new RuntimeException(String.format("Could not load impl for %s", impl), e);
        }

        TracerFactoryInterface factory;
        try {
            factory = factoryClass.newInstance();
        } catch (ReflectiveOperationException e) {
            logger.log(Level.SEVERE, "Failed to instantiate tracer factory", e);
            throw new RuntimeException(String.format("Could not instantiate factory for %s", impl), e);
        }

        return factory.create(host);
    }

    /**
     * Is Tracing enabled? {@link TracerFactory#create(ServiceHost)} will return a {@link io.opentracing.NoopTracer}
     *
     * @return
     */
    public boolean enabled() {
        if (!this.enableChecked) {
            String impl = XenonConfiguration.string(TracerFactory.class, "provider", null);
            this.enabled = impl != null;
            this.enableChecked = true;
        }
        return this.enabled;
    }
}
