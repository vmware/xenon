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

package com.vmware.xenon.distributedtracing.zipkin;

import java.util.HashMap;
import java.util.Map;

import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.http.DefaultSpanNameProvider;
import com.github.kristofa.brave.http.SpanNameProvider;

/**
 * Zipkin DTracer which will be used to submit traces to Zipkin. It will abstract
 * the underlying mechanism to submit the traces to Zipkin. Currently its based on
 * Openzipkin Brave libraries.
 */
public class ZipkinTracer {

    private static Map<String, ZipkinTracer> tracers = new HashMap<>();

    private ZipkinLocalSpanTracker localSpanTracker;
    private final Brave brave;

    private final String tracerName;

    private ZipkinTracer(String tracerName) {
        this.tracerName = tracerName;
        this.brave = ZipkinConfig.getBraveInstance(this.tracerName);
        this.localSpanTracker = new ZipkinLocalSpanTracker(this.brave);
    }

    public static synchronized ZipkinTracer getTracer() {
        return getTracer(null, true);
    }

    public static synchronized ZipkinTracer getTracer(String stackName) {
        return getTracer(stackName, true);
    }

    public static synchronized ZipkinTracer getTracer(String stackName, boolean startTracing) {
        String tracerName = ZipkinConfig.getTracerName(stackName);

        if (tracers.containsKey(tracerName)) {
            return tracers.get(tracerName);
        }
        ZipkinTracer tracer = new ZipkinTracer(tracerName);
        if (startTracing) {
            tracer.startTrackingSpans();
        }
        tracers.put(tracerName, tracer);
        return tracer;
    }

    public boolean startTrackingSpans() {
        if (!this.localSpanTracker.isTraceRunning()) {
            return this.localSpanTracker.startTracing();
        }
        return false;
    }

    public boolean stopTrackingSpans() {
        return this.localSpanTracker.stopTracing();
    }

    public SpanId startLocalSpan(String component, String operation) {
        return this.localSpanTracker.startLocalSpan(component, operation);
    }

    public void endLocalSpan(SpanId spanId) {
        if (spanId == null) {
            return;
        }
        this.localSpanTracker.endLocalSpan(spanId.toSpan());
    }

    public void startServerSpan(long traceId, long eSpanId, Long parentSpanId, String name) {
        this.brave.serverTracer().setStateCurrentTrace(traceId, eSpanId, parentSpanId, name);
        this.brave.serverTracer().setServerReceived();
    }

    public void startClientSpan() {
        this.brave.clientTracer().setClientSent();
    }

    public void endClientSpan() {
        this.brave.clientTracer().setClientReceived();
    }

    public void endServerSpan() {
        this.brave.serverTracer().setServerSend();
    }

    public void submitAnnotation(String key, String value) {
        if (key != null && value != null) {
            this.brave.localTracer().submitBinaryAnnotation(key, value);
        }
    }

    public SpanNameProvider getSpanNameProvider() {
        return new DefaultSpanNameProvider();
    }

    public ClientTracer getClientTracer() {
        return this.brave.clientTracer();
    }

    public ZipkinLocalSpanCache getOpenedSpans() {
        return this.localSpanTracker.getOpenedSpans();
    }

    public ZipkinLocalSpanCache getClosableSpans() {
        return this.localSpanTracker.getClosableSpans();
    }

    @Override
    public String toString() {
        return "ZipkinTracer{" +
                "localSpanTracker=" + this.localSpanTracker +
                ", brave=" + this.brave +
                ", tracerName='" + this.tracerName + '\'' +
                '}';
    }

}
