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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import com.github.kristofa.brave.BoundarySampler;
import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.EmptySpanCollectorMetricsHandler;
import com.github.kristofa.brave.InheritableServerClientAndLocalSpanState;
import com.github.kristofa.brave.LoggingSpanCollector;
import com.github.kristofa.brave.SpanCollector;
import com.github.kristofa.brave.http.HttpSpanCollector;
import com.google.common.base.CaseFormat;
import com.twitter.zipkin.gen.Endpoint;

/**
 * This will read and configure the Zipkin settings - zipkin url, sampling rate & app/stack name.
 * The values for these settings will be read from java system properties - tracer.appName,
 * tracer.sampleRate & tracer.zipkinUrl. If those are not available then it will be read from
 * environment variables - TRACER_APP_NAME, TRACER_SAMPLE_RATE & TRACER_ZIPKIN_URL. In case, even
 * those are also, not available, then a No-Op DTracer will be returned.
 */
public class ZipkinConfig {

    private static final Logger LOG = Logger.getLogger(ZipkinConfig.class.getName());
    private static final String PARAM_TRACER_APP_NAME = "tracer.appName";
    private static final String PARAM_TRACER_SAMPLE_RATE = "tracer.sampleRate";
    private static final String PARAM_TRACER_ZIPKIN_URL = "tracer.zipkinUrl";
    private static final String DEFAULT_APP_NAME = "service";
    private static final float DEFAULT_SAMPLE_RATE = 1.0f;

    private static float getSampleRate() {
        float sampleRate;
        String sampleRateStr = readParameter(PARAM_TRACER_SAMPLE_RATE);
        if (sampleRateStr == null) {
            sampleRate = DEFAULT_SAMPLE_RATE;
        } else {
            try {
                sampleRate = Float.parseFloat(sampleRateStr);
            } catch (NumberFormatException nfe) {
                sampleRate = DEFAULT_SAMPLE_RATE;
            }
        }
        return sampleRate;
    }

    private static String getZipkinUrl() {
        return readParameter(PARAM_TRACER_ZIPKIN_URL);
    }

    private static String readParameter(String varName) {
        String varValue = System.getProperty(varName);
        if (varValue == null || varValue.isEmpty() || varValue
                .equalsIgnoreCase("null")) {
            String envVarName = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, varName);
            envVarName = envVarName.replaceAll("\\.", "_");
            varValue = System.getenv(envVarName);
        }
        return varValue;
    }

    public static String getTracerName(String stackName) {
        String appName = readParameter(PARAM_TRACER_APP_NAME);

        if (appName == null) {
            appName = DEFAULT_APP_NAME;
        }

        String hostName = "";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        if (stackName == null || stackName.isEmpty() || stackName
                .equalsIgnoreCase("null")) {
            return appName + "-" + hostName;
        }
        return appName + "-" + stackName + "-" + hostName;
    }

    public static Brave getBraveInstance(String tracerName) {
        InheritableServerClientAndLocalSpanState state = new InheritableServerClientAndLocalSpanState(
                Endpoint.create(tracerName, 0));
        Brave.Builder builder = new Brave.Builder(state);

        String zipkinUrl = getZipkinUrl();
        SpanCollector spanCollector = null;
        float rate = getSampleRate();
        if (zipkinUrl == null) {
            spanCollector = new LoggingSpanCollector();
            rate = 0;
            LOG.info(String.format("Initialized DTracer: [%s] as a Logger which wont sample",
                    tracerName));
        } else {
            spanCollector = HttpSpanCollector.create(zipkinUrl,
                    new EmptySpanCollectorMetricsHandler());
            LOG.info(String.format(
                    "Initialized DTracer: [%s] which will submit traces to [%s] and will sample at rate: [%s]",
                    tracerName, zipkinUrl, rate));
        }
        builder.spanCollector(spanCollector)
                .traceSampler(BoundarySampler.create(rate));
        return builder.build();
    }

}
