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

import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.SpanId;
import com.twitter.zipkin.gen.Span;

import com.vmware.xenon.distributedtracing.zipkin.ZipkinLocalSpanCache.CachedSpan;

/**
 * Local Span Tracker for Asynchronous as well as Nested Spans.
 */
public class ZipkinLocalSpanTracker {
    private Brave brave;
    private volatile boolean isTraceRunning;

    //To track what all spans are still to be closed while debugging
    private ZipkinLocalSpanCache openedSpans;

    // What all spans are marked to be closed, but could be waiting
    // on nested spans to close before closing themselves.
    private ZipkinLocalSpanCache closableSpans;

    public ZipkinLocalSpanTracker(Brave brave) {
        this.brave = brave;
        this.openedSpans = new ZipkinLocalSpanCache();
        this.closableSpans = new ZipkinLocalSpanCache();
    }

    public boolean isTraceRunning() {
        return this.isTraceRunning;
    }

    public boolean startTracing() {
        return (this.isTraceRunning = true);
    }

    public boolean stopTracing() {
        this.openedSpans.clean();
        this.closableSpans.clean();
        return (this.isTraceRunning = false);
    }

    private void markSpanFinished(Span expectedSpan) {
        this.brave.localTracer().finishSpan();
    }

    private Span readSpan() {
        return this.brave.localSpanThreadBinder().getCurrentLocalSpan();
    }

    private void closeAllPossibleSpans() {
        while (true) {
            if (this.closableSpans.isEmpty()) {
                break;
            }
            Span currentSpan = readSpan();
            CachedSpan cachedSpan = new CachedSpan(currentSpan);
            if (this.closableSpans.get(cachedSpan) == null) {
                break;
            }
            this.closableSpans.remove(cachedSpan);
            this.openedSpans.remove(cachedSpan);
        }
    }

    public SpanId startLocalSpan(String component, String operation) {
        if (!this.isTraceRunning) {
            return null;
        }
        SpanId spanId = this.brave.localTracer().startNewSpan(component, operation);
        if (spanId != null) {
            CachedSpan cachedSpan = new CachedSpan(spanId.toSpan());
            this.openedSpans.put(cachedSpan, component + "." + operation);
        }
        return spanId;
    }

    public void endLocalSpan(Span spanSent) {
        if (!this.isTraceRunning) {
            return;
        }
        CachedSpan cachedSpan = new CachedSpan(spanSent);
        String openedSpanDetail = this.openedSpans.get(cachedSpan);
        if (openedSpanDetail == null) {
            openedSpanDetail = "Unknown";
        }
        this.closableSpans.put(cachedSpan, openedSpanDetail);
        closeAllPossibleSpans();
    }

    public ZipkinLocalSpanCache getOpenedSpans() {
        return this.openedSpans;
    }

    public ZipkinLocalSpanCache getClosableSpans() {
        return this.closableSpans;
    }
}
