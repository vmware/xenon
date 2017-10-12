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

import java.util.Map;
import java.util.logging.Logger;

import brave.Tracing;
import brave.opentracing.BraveTracer;
import brave.sampler.BoundarySampler;
import io.opentracing.Tracer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.okhttp3.OkHttpSender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import com.vmware.xenon.common.ServiceHost;

public class Zipkin implements TracerFactoryInterface {

    @Override
    public Tracer create(ServiceHost host) {
        Logger logger = Logger.getLogger(getClass().getName());
        Map<String, String> env = System.getenv();
        String zipkinUrl = env.get("ZIPKIN_URL");
        String serviceName = env.get("ZIPKIN_SERVICE_NAME");
        String sampleRateString = env.get("ZIPKIN_SAMPLERATE");
        if (zipkinUrl == null || zipkinUrl.isEmpty()) {
            throw new RuntimeException("Zipkin tracing requires ZIPKIN_URL set.");
        }
        if (serviceName == null || serviceName.isEmpty()) {
            throw new RuntimeException(("Service name missing - ZIPKIN_SERVICE_NAME not set."));
        }
        Float rate;
        if (sampleRateString == null || sampleRateString.isEmpty()) {
            rate = 1.0f;
        } else {
            try {
                rate = Float.parseFloat(sampleRateString);
            } catch (NumberFormatException nfe) {
                rate = 1.0f;
            }
        }
        Sender sender = null;
        Reporter<Span> spanReporter = null;
        if (zipkinUrl.contains("/v1/")) {
            sender = URLConnectionSender.create(zipkinUrl);
            spanReporter = AsyncReporter.builder(sender)
                    .build(SpanBytesEncoder.JSON_V1);
        } else {
            sender = OkHttpSender.create(zipkinUrl);
            spanReporter = AsyncReporter.create(sender);
        }
        Tracing braveTracing = Tracing.newBuilder()
                .localServiceName(serviceName)
                .spanReporter(spanReporter)
                .sampler(BoundarySampler.create(rate))
                .build();
        logger.info("Opentracing support using Zipkin");
        return BraveTracer.create(braveTracing);
    }
}
