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

package com.vmware.xenon.tracing;

import java.util.Map;

import com.github.kristofa.brave.ClientRequestInterceptor;
import com.github.kristofa.brave.ClientResponseInterceptor;
import com.github.kristofa.brave.IdConversion;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.http.BraveHttpHeaders;
import com.github.kristofa.brave.spring.BraveClientHttpRequestInterceptor;
import com.google.gson.Gson;

import org.springframework.http.client.AsyncClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpRequestInterceptor;

import com.vmware.xenon.tracing.spring.XenonClientAsyncHttpRequestInterceptor;
import com.vmware.xenon.tracing.zipkin.ZipkinTracer;

/**
 * Tracer class which will help tracing an application or a stack (within an application).
 */
public class Tracer {

    private ZipkinTracer zipkinTracer;
    private String appStackName;
    private boolean startTracing;

    public static synchronized Tracer getTracer() {
        return getTracer(true);
    }

    public static synchronized Tracer getTracer(String tracerName) {
        return getTracer(tracerName, true);
    }

    public static synchronized Tracer getTracer(boolean startTracing) {
        return getTracer(null, startTracing);
    }

    public static synchronized Tracer getTracer(String tracerName, boolean startTracing) {
        return new Tracer(tracerName, startTracing);
    }

    private Tracer(String appStackName, boolean startTracing) {
        this.appStackName = appStackName;
        this.startTracing = startTracing;
    }

    public ZipkinTracer getServiceTracer() {
        //Lazy initialize the brave tracer
        if (this.zipkinTracer == null) {
            this.zipkinTracer = ZipkinTracer.getTracer(this.appStackName, this.startTracing);
        }
        return this.zipkinTracer;
    }

    public void submitAnnotation(String key, String value) {
        getServiceTracer().submitAnnotation(key, value);
    }

    public SpanId startLocalSpan(String component, String operation) {
        return getServiceTracer().startLocalSpan(component, operation);
    }

    public void endLocalSpan(SpanId spanId) {
        getServiceTracer().endLocalSpan(spanId);
    }

    public String startServerSpanAndGenerateContextId(Map<String, String> httpHeaders) {
        String traceId = httpHeaders.get(BraveHttpHeaders.TraceId.getName().toLowerCase());
        String spanId = httpHeaders.get(BraveHttpHeaders.SpanId.getName().toLowerCase());
        String parentSpanId = httpHeaders
                .get(BraveHttpHeaders.ParentSpanId.getName().toLowerCase());
        String sampled = httpHeaders.get(BraveHttpHeaders.Sampled.getName().toLowerCase());
        boolean success = startServerSpan(traceId, spanId, parentSpanId, sampled);
        if (success) {
            return generateContextJson(traceId, spanId, parentSpanId, sampled, generateId());
        } else {
            return null;
        }
    }

    private static class Context {
        private String traceId;
        private String spanId;
        private String parentSpanId;
        private String sampled;
        private String contextId;

        public Context(String traceId, String spanId, String parentSpanId, String sampled,
                String contextId) {
            this.traceId = traceId;
            this.spanId = spanId;
            this.parentSpanId = parentSpanId;
            this.sampled = sampled;
            this.contextId = contextId;
        }

        public String getTraceId() {
            return this.traceId;
        }

        public String getSpanId() {
            return this.spanId;
        }

        public String getParentSpanId() {
            return this.parentSpanId;
        }

        public String getSampled() {
            return this.sampled;
        }

        public String getContextId() {
            return this.contextId;
        }
    }

    private String generateContextJson(String traceId, String spanId, String parentSpanId,
            String sampled, String contextId) {
        Gson gson = new Gson();
        return gson.toJson(new Context(traceId, spanId, parentSpanId, sampled,
                contextId));
    }

    private String generateId() {
        return String.valueOf(System.nanoTime());
    }

    public boolean startServerSpan(String traceIdStr, String spanIdStr, String parentSpanIdStr,
            String sampledStr) {
        if (traceIdStr != null && spanIdStr != null && sampledStr != null) {
            long traceId = IdConversion.convertToLong(traceIdStr);
            long eSpanId = IdConversion.convertToLong(spanIdStr);
            Long parentSpanId = null;
            if (parentSpanIdStr != null) {
                parentSpanId = IdConversion.convertToLong(parentSpanIdStr);
            }
            String name = sampledStr;
            getServiceTracer().startServerSpan(traceId, eSpanId, parentSpanId, name);
            return true;
        }
        return false;
    }

    public void endServerSpan() {
        getServiceTracer().endServerSpan();
    }

    public void startClientSpan() {
        getServiceTracer().startClientSpan();
    }

    public void endClientSpan() {
        getServiceTracer().endClientSpan();
    }

    public AsyncClientHttpRequestInterceptor getAsyncHttpRequestInterceptor() {
        return new XenonClientAsyncHttpRequestInterceptor(
                new ClientRequestInterceptor(getServiceTracer().getClientTracer()),
                new ClientResponseInterceptor(getServiceTracer().getClientTracer()),
                getServiceTracer().getSpanNameProvider());
    }

    public ClientHttpRequestInterceptor getHttpRequestInterceptor() {
        return new BraveClientHttpRequestInterceptor(
                new ClientRequestInterceptor(getServiceTracer().getClientTracer()),
                new ClientResponseInterceptor(getServiceTracer().getClientTracer()),
                getServiceTracer().getSpanNameProvider());
    }

    public boolean startTracing() {
        return getServiceTracer().startTrackingSpans();
    }

    public boolean stopTracing() {
        return getServiceTracer().stopTrackingSpans();
    }

}
