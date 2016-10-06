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

import java.util.logging.Level;

import com.github.kristofa.brave.SpanId;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleTaskService;
import com.vmware.xenon.services.common.LuceneDocumentIndexService;
import com.vmware.xenon.services.common.RootNamespaceService;

/**
 * Our entry point, spawning a host that run/showcase examples we can play with.
 */
public class TestTracingHost extends ServiceHost {

    private DTracer tracer = DTracer.getTracer();

    public static void main(String[] args) throws Throwable {
        TestTracingHost h = new TestTracingHost();
        h.initialize(args);
        LuceneDocumentIndexService documentIndexService = new LuceneDocumentIndexService();
        documentIndexService.toggleOption(ServiceOption.INSTRUMENTATION, true);
        h.setDocumentIndexingService(documentIndexService);
        h.toggleDebuggingMode(true);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    public DTracer getTracer() {
        return this.tracer;
    }

    @Override
    public ServiceHost start() throws Throwable {
        SpanId spanId = this.tracer.startLocalSpan(this.getClass().getName(), "start");
        super.start();

        // Start core services, must be done once
        SpanId nestedSpanId = this.tracer
                .startLocalSpan(this.getClass().getName(), "startDefaultCore");
        startDefaultCoreServicesSynchronously();

        // Start the root namespace service: this will list all available factory services for
        // queries to the root (/)
        super.startService(new RootNamespaceService());
        this.tracer.endLocalSpan(nestedSpanId);

        SpanId exampleSpanId = this.tracer
                .startLocalSpan(this.getClass().getName(), "startExampleServices");
        // Start example tutorial services
        super.startFactory(new ExampleService());
        super.startFactory(new ExampleTaskService());
        this.tracer.endLocalSpan(exampleSpanId);
        this.tracer.endLocalSpan(spanId);
        return this;
    }

    @Override public void startDefaultCoreServicesSynchronously() throws Throwable {
        SpanId spanId = this.tracer
                .startLocalSpan(this.getClass().getName(), "startDefaultCoreServicesSynchronously");
        super.startDefaultCoreServicesSynchronously();
        this.tracer.endLocalSpan(spanId);
    }
}
