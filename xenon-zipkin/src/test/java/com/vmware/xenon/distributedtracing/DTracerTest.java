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

package com.vmware.xenon.distributedtracing;

import java.util.UUID;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DTracerTest extends TestCase {

    private TestTracingHost launchService(TemporaryFolder tmpFolder) throws Throwable {
        TestTracingHost h = new TestTracingHost();
        String bindAddress = "127.0.0.1";
        String hostId = UUID.randomUUID().toString();
        String[] args = {
                "--port=0",
                "--sandbox=" + tmpFolder.getRoot().getAbsolutePath(),
                "--bindAddress=" + bindAddress,
                "--id=" + hostId
        };
        h.initialize(args);
        h.start();
        return h;
    }

    @Test
    public void testNoSystemProperties() throws Throwable {
        System.setProperty("tracer.appName", "");
        System.setProperty("tracer.sampleRate", "");
        System.setProperty("tracer.zipkinUrl", "");
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        TestTracingHost h = null;
        try {
            h = launchService(tmpFolder);
            assertTrue(h.getTracer().getServiceTracer().getClosableSpans().size() == 0);
            assertTrue(h.getTracer().getServiceTracer().getOpenedSpans().size() == 0);
        } finally {
            assertNotNull(h);
            h.stop();
            tmpFolder.delete();
        }

    }

    @Test
    public void testSystemProperties() throws Throwable {
        System.setProperty("tracer.appName", "tracerhost1");
        System.setProperty("tracer.sampleRate", "1");
        System.setProperty("tracer.zipkinUrl", "http://zipkinUrl");
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        TestTracingHost h = null;
        try {
            h = launchService(tmpFolder);
            assertTrue(h.getTracer().getServiceTracer().getClosableSpans().size() > 0);
            assertTrue(h.getTracer().getServiceTracer().getOpenedSpans().size() > 0);
        } finally {
            assertNotNull(h);
            h.stop();
            tmpFolder.delete();
        }

    }

    @Test
    public void testSystemPropertiesZeroSampling() throws Throwable {
        System.setProperty("tracer.appName", "tracerhost2");
        System.setProperty("tracer.sampleRate", "0");
        System.setProperty("tracer.zipkinUrl", "http://zipkinUrl");
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        TestTracingHost h = null;
        try {
            h = launchService(tmpFolder);
            assertTrue(h.getTracer().getServiceTracer().getClosableSpans().size() == 0);
            assertTrue(h.getTracer().getServiceTracer().getOpenedSpans().size() == 0);
        } finally {
            assertNotNull(h);
            h.stop();
            tmpFolder.delete();
        }

    }
}
