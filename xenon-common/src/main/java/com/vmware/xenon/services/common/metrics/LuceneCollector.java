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

package com.vmware.xenon.services.common.metrics;

import static com.vmware.xenon.common.metrics.AbstractNamedCollector.NS;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.Describable;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

public final class LuceneCollector extends Collector implements Describable {
    private static final String SYSTEM_LUCENE = "lucene";

    public final Counter commitCount;

    public final Gauge fileCount;
    public final Gauge indexSize;

    private final Collector[] allMetrics;

    public LuceneCollector() {
        this.allMetrics = new Collector[] {
                this.commitCount = counter("commit_count", "Total commits since process start"),
                this.fileCount = gauge("files_count", "Current number of files in sandbox"),
                this.indexSize = gauge("index_size_bytes", "Index size on disk")
        };
    }

    private Counter counter(String name, String help) {
        return new Counter.Builder()
                .name(NS)
                .subsystem(SYSTEM_LUCENE)
                .name(name)
                .help(help)
                .create();
    }

    private Gauge gauge(String name, String help) {
        return new Gauge.Builder()
                .name(NS)
                .subsystem(SYSTEM_LUCENE)
                .name(name)
                .help(help)
                .create();
    }

    private Summary summary(String name, String help) {
        return new Summary.Builder()
                .name(NS)
                .subsystem(SYSTEM_LUCENE)
                .name(name)
                .help(help)
                .create();
    }

    private Histogram histogram(String name, String help) {
        return new Histogram.Builder()
                .name(NS)
                .subsystem(SYSTEM_LUCENE)
                .name(name)
                .help(help)
                .create();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return Stream.of(this.allMetrics)
                .map(Collector::collect)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.emptyList();
    }
}
