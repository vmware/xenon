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

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.twitter.zipkin.gen.Span;

/**
 * Zipkin local span cache - this cache will be used to track the spans.
 * It will keep the spans for 1 minute, and then the span will be removed from the cache.
 * If a span is closed before 1 minute, it's removed anyway from the cache.
 */
public class ZipkinLocalSpanCache {
    private static final long MAX_SIZE = 10000;
    private static final long MAX_TIME_IN_MINUTES = 1;
    private static final Logger LOG = Logger.getLogger(ZipkinLocalSpanCache.class.getName());
    private final LoadingCache<CachedSpan, Optional<String>> cache;

    public ZipkinLocalSpanCache() {
        this.cache = CacheBuilder.newBuilder().maximumSize(MAX_SIZE)
                .expireAfterWrite(MAX_TIME_IN_MINUTES, TimeUnit.MINUTES)
                .build(new CacheLoader<CachedSpan, Optional<String>>() {
                    @Override
                    public Optional<String> load(CachedSpan span) {
                        return Optional.ofNullable(getDefault(span));
                    }

                    private String getDefault(CachedSpan span) {
                        return null;
                    }
                });
    }

    public String get(CachedSpan key) {
        if (!this.cache.getUnchecked(key).isPresent()) {
            return null;
        }
        return this.cache.getUnchecked(key).get();
    }

    public void clean() {
        this.cache.invalidateAll();
    }

    public boolean isEmpty() {
        return this.cache.size() == 0L;
    }

    public long size() {
        return this.cache.size();
    }

    public void put(CachedSpan cachedSpan, String location) {
        this.cache.put(cachedSpan, Optional.of(location));
    }

    public void remove(CachedSpan cachedSpan) {
        this.cache.invalidate(cachedSpan);
    }

    public static class CachedSpan {
        Span span;
        private long traceId;
        private long id;
        private Long parentId;

        CachedSpan(Span span) {
            this.span = span;
            if (span != null) {
                this.traceId = span.getTrace_id();
                this.id = span.getId();
                this.parentId = span.getParent_id();
            }
        }

        public Span getSpan() {
            return this.span;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CachedSpan)) {
                return false;
            }
            CachedSpan that = (CachedSpan) o;
            return (this.traceId == that.traceId && this.id == that.id
                    && ((this.parentId != null && that.parentId != null) ?
                    (this.parentId.longValue() == that.parentId.longValue()) :
                    (this.parentId == null && that.parentId == null)));
        }

        @Override
        public int hashCode() {
            if (this.span == null) {
                return 0;
            }

            final int prime = 31;
            int result = 1;
            result = result * prime + Long.hashCode(this.traceId);
            result = result * prime + Long.hashCode(this.id);
            if (this.parentId != null) {
                result = result * prime + Long.hashCode(this.parentId);
            }
            return result;
        }

        @Override public String toString() {
            return "{traceId=" + this.span.getTrace_id() + ", spanId=" + this.span.getId()
                    + ", parentSpanId=" + this.span.getParent_id() + ", name=" + this.span.getName()
                    + "}";
        }

    }

    public ConcurrentMap<CachedSpan, Optional<String>> getEntries() {
        return this.cache.asMap();
    }
}