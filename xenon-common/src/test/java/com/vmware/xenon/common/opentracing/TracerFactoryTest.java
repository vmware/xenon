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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import brave.opentracing.BraveTracer;
import io.opentracing.NoopTracer;
import io.opentracing.Tracer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.test.VerificationHost;

public class TracerFactoryTest {

    public static class IsolateEnvironment extends ExternalResource {
        private Map<String, String> env;

        @Override
        protected void after() {
            try {
                setEnv((env) -> {
                    env.clear();
                    env.putAll(this.env);
                });
            } catch (Exception e) {
                Logger.getAnonymousLogger().log(Level.INFO, "Failure", e);
                throw new RuntimeException("Failed to reset environment");
            }
            super.after();
        }

        @Override
        protected void before() throws Throwable {
            super.before();
            this.env = new HashMap<>();
            this.env.putAll(System.getenv());
        }

        public void setEnv(Consumer<Map<String, String>> mutate) throws Exception {
        /* All platforms */
            try {
                Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
                Field theUnmodifiableEnvironmentField = processEnvironmentClass.getDeclaredField("theUnmodifiableEnvironment");
                theUnmodifiableEnvironmentField.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<String,String> theUnmodifiableEnvironment = (Map<String,String>)theUnmodifiableEnvironmentField.get(null);

                Class<?> theUnmodifiableEnvironmentClass = theUnmodifiableEnvironment.getClass();
                Field theModifiableEnvField = theUnmodifiableEnvironmentClass.getDeclaredField("m");
                theModifiableEnvField.setAccessible(true);
                @SuppressWarnings("unchecked")
                Map<String,String> modifiableEnv = (Map<String,String>) theModifiableEnvField.get(theUnmodifiableEnvironment);
                mutate.accept(modifiableEnv);
            /* windows */
                try {
                    Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
                    theCaseInsensitiveEnvironmentField.setAccessible(true);
                    @SuppressWarnings("unchecked")
                    Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
                    mutate.accept(cienv);
                } catch (NoSuchFieldException e) {
                    // Nothing to do when the field does not exist.
                }
            } catch (Exception e) {
                Logger.getAnonymousLogger().log(Level.SEVERE, "failed to access writable map", e);
                throw new RuntimeException("Unable to access writable environment variable map.", e);
            }
        }

    }

    @Rule
    public final IsolateEnvironment environment = new IsolateEnvironment();

    @Rule
    public InjectCleanFactory injectCleanFactory = new InjectCleanFactory();

    @Test
    public void defaultIsNoop() throws Exception {
        this.environment.setEnv((env) -> env.remove("XENON_TRACER"));
        ServiceHost h = VerificationHost.create(0);
        Tracer tracer = TracerFactory.factory.create(h);
        assertTrue(tracer instanceof NoopTracer);
        assertFalse(TracerFactory.factory.enabled());
    }

    @Test
    public void jaegerGetsJaeger() throws Exception {
        this.environment.setEnv((env) -> {
            env.put("XENON_TRACER", "jaeger");
            env.put("JAEGER_SERVICE_NAME", "test");
        });
        ServiceHost h = VerificationHost.create(0);
        Tracer tracer = TracerFactory.factory.create(h);
        assertTrue(tracer instanceof com.uber.jaeger.Tracer);
        assertTrue(TracerFactory.factory.enabled());
    }

    @Test
    public void zipkinGetsZipkin() throws Exception {
        this.environment.setEnv((env) -> {
            env.put("XENON_TRACER", "zipkin");
            env.put("ZIPKIN_URL", "http://host/api/v1/spans/");
            env.put("ZIPKIN_SERVICE_NAME", "test");
        });
        ServiceHost h = VerificationHost.create(0);
        Tracer tracer = TracerFactory.factory.create(h);
        assertTrue(tracer instanceof BraveTracer);
        assertTrue(TracerFactory.factory.enabled());
    }

    @Test(expected = RuntimeException.class)
    public void invalidTracer() throws Exception {
        this.environment.setEnv((env) -> env.put("XENON_TRACER", "invalid"));
        ServiceHost h = VerificationHost.create(0);
        TracerFactory.factory.create(h);
    }

    public class InjectCleanFactory extends ExternalResource {
        TracerFactory factory;

        @Override
        protected void after() {
            TracerFactory.factory = this.factory;
            super.after();
        }

        @Override
        protected void before() throws Throwable {
            super.before();
            this.factory = TracerFactory.factory;
            TracerFactory.factory = new TracerFactory();
        }
    }

}
