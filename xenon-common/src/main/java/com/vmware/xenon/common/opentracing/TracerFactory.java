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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentracing.NoopTracerFactory;
import io.opentracing.Tracer;

import com.vmware.xenon.common.ServiceHost;

public class TracerFactory {
    /**
     * Singleton: may be replaced to customise implicit tracer creation - e.g. to add support for
     * a different OpenTracing implementation.
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings("MS_SHOULD_BE_FINAL")
    public static TracerFactory factory = new TracerFactory();

    /**
     * Create a {@link io.opentracing.Tracer} for use by a {@link com.vmware.xenon.common.ServiceHost}.
     *
     * See README.md for the configuration variables for this factory. The default implementation does
     * not perform any host-specific customisations.
     *
     * @return A {@link io.opentracing.Tracer} instance for tracing the given {@link com.vmware.xenon.common.ServiceHost}
     */
    @SuppressWarnings("unchecked")
    public synchronized Tracer create(ServiceHost host) {
        Logger logger = Logger.getLogger(getClass().getName());
        Map<String, String> env = System.getenv();
        String implementation = env.get("XENON_TRACER");
        if (implementation == null) {
            implementation = "";
        }
        implementation = implementation.toLowerCase();
        if (implementation.isEmpty()) {
            logger.info(String.format("Opentracing not enabled."));
            return NoopTracerFactory.create();
        }
        Class<TracerFactoryInterface> factoryClass = null;
        try {
            if (implementation.equals("jaeger")) {
                factoryClass = (Class<TracerFactoryInterface>) Class.forName("com.vmware.xenon.common.opentracing.Jaeger");
            } else if (implementation.equals("zipkin")) {
                factoryClass = (Class<TracerFactoryInterface>) Class.forName("com.vmware.xenon.common.opentracing.Zipkin");
            } else {
                throw new RuntimeException(String.format("Bad tracer type %s", implementation));
            }
            if (factoryClass == null) {
                throw new RuntimeException(String.format(
                        "Failed to cast implementation '%s' to Class<TracerFactoryInterface>", implementation));
            }
        } catch (ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Failed to load implementation class", e);
            throw new RuntimeException(String.format("Could not load implementation for %s", implementation), e);
        }
        assert factoryClass != null;
        TracerFactoryInterface factory;
        try {
            factory = factoryClass.getConstructor().newInstance();
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            logger.log(Level.SEVERE, "Failed to instantiate tracer factory", e);
            throw new RuntimeException(String.format("Could not instantiate factory for %s", implementation), e);
        }
        return factory.create(host);
    }

}
