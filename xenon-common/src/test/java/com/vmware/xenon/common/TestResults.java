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

package com.vmware.xenon.common;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.vmware.xenon.common.ServiceStats.ServiceStat;

/**
 * Declare this as a @Rule to your unit test. As it is bound to the test lifecycle all measurements taken will be
 * in the context of method annotated with @Test. Whether the report generated is written to disk is controlled by the
 * system property {@value PROP_WRITE_REPORT}
 */
public final class TestResults implements TestRule {

    private static final String PROP_WRITE_REPORT = CommandLineArgumentParser.PROPERTY_PREFIX + "writeReport";
    private static final boolean SHOULD_WRITE_REPORT = Boolean.getBoolean(PROP_WRITE_REPORT);
    /**
     * store test results here in if XENON_PERF_REPORT_STORE is not set
     */
    private static final String DEFAULT_REPORTS_LOCATION = "target/test-results";

    /**
     * The ID of this performance test, usually it is the commit runId or build number and is set externally.
     */
    private static final String ENV_XENON_ID = "XENON_PERF_ID";

    /**
     * Logical name of the hardware config. Usually set at the jenkins worker level.
     */
    private static final String ENV_XENON_HW_CONFIG = "XENON_PERF_HW_CONFIG";

    /**
     * Path to the directory to store reports. Must be writable and a directory.
     */
    private static final String ENV_XENON_REPORT_STORE = "XENON_PERF_REPORT_STORE";

    /**
     * Use this constant if you mean throughput: number of things per unit of time.
     */
    public static final String KEY_THROUGHPUT = "throughput";

    private Description description;

    private Report report;

    private static final class AtomicDouble {
        private final AtomicLong number;

        AtomicDouble() {
            this.number = new AtomicLong(Double.doubleToLongBits(0.0));
        }

        double doubleValue() {
            return Double.longBitsToDouble(this.number.get());
        }

        double incrementAndGet(double inc) {
            while (true) {
                long oldBits = this.number.get();
                double newVal = Double.longBitsToDouble(oldBits) + inc;
                long newBits = Double.doubleToLongBits(newVal);
                if (this.number.compareAndSet(oldBits, newBits)) {
                    return newVal;
                }
            }
        }

        void max(double newValue) {
            while (true) {
                long oldBits = this.number.get();
                double oldValue = Double.longBitsToDouble(oldBits);
                long newBits = Double.doubleToLongBits(Double.max(oldValue, newValue));
                if (this.number.compareAndSet(oldBits, newBits)) {
                    return;
                }
            }
        }

        void set(double newValue) {
            while (true) {
                long oldBits = this.number.get();
                long newBits = Double.doubleToLongBits(newValue);
                if (this.number.compareAndSet(oldBits, newBits)) {
                    return;
                }
            }
        }

        void min(double newValue) {
            while (true) {
                long oldBits = this.number.get();
                double oldValue = Double.longBitsToDouble(oldBits);
                long newBits = Double.doubleToLongBits(Double.min(oldValue, newValue));
                if (this.number.compareAndSet(oldBits, newBits)) {
                    return;
                }
            }
        }
    }

    private static class Stat {
        private final AtomicDouble value = new AtomicDouble();
        private final AtomicLong count = new AtomicLong();

        Stat(double initialValue) {
            this.value.set(initialValue);
        }

        void max(double value) {
            this.value.max(value);
        }

        void value(double value) {
            this.value.set(value);
        }

        void min(double value) {
            this.value.min(value);
        }

        long inc() {
            return this.count.incrementAndGet();
        }

        void avg(double value) {
            this.value.incrementAndGet(value);
            this.count.incrementAndGet();
        }

        int getCount() {
            return this.count.intValue();
        }

        double getAvg() {
            if (this.count.get() == 0) {
                return 0;
            }

            return this.value.doubleValue() / this.count.intValue();
        }

        double getValue() {
            return this.value.doubleValue();
        }
    }

    public static class Report {
        private final transient SortedMap<String, Stat> min = new ConcurrentSkipListMap<>();
        private final transient SortedMap<String, Stat> last = new ConcurrentSkipListMap<>();
        private final transient SortedMap<String, Stat> max = new ConcurrentSkipListMap<>();
        private final transient SortedMap<String, Stat> avg = new ConcurrentSkipListMap<>();
        private final transient SortedMap<String, Stat> counters = new ConcurrentSkipListMap<>();
        private final transient Map<String, ServiceStats> rawStats = new ConcurrentSkipListMap<>();
        private final String name;
        private String javaVersion;
        private String os;
        private String timestamp;
        private String hardwareConfig;
        private List<String> jvmArgs;
        private String id;
        private Map<String, Double> metrics;
        private Map<String, Map<String, Double>> stats;
        private Map<String, Object> xenonArgs;

        Report(Description description) {
            this.name = description.getTestClass().getSimpleName() + "." + description.getMethodName();
        }

        /**
         * Replaces value at key with value
         * @param key
         * @param value
         */
        public void lastValue(String key, double value) {
            entry(key, this.last).value(value);
        }

        private Stat entry(String key, Map<String, Stat> map) {
            return map.computeIfAbsent(key, (k) -> new Stat(0));
        }

        /**
         * Replaces value at key with new value only if it's greater than
         * @param key
         * @param value
         */
        public void maxValue(String key, double value) {
            entry(key, this.max).max(value);
        }

        /**
         * Replaces value at key with new value only if it's less than
         * @param key
         * @param value
         */
        public void minValue(String key, double value) {
            this.min.computeIfAbsent(key, (k) -> new Stat(Double.MAX_VALUE)).min(value);
        }

        /**
         * Records average value between value and all previous calls to avg.
         * @param key
         * @param value
         */
        public void avg(String key, double value) {
            entry(key, this.avg).avg(value);
        }

        /**
         * Update min/max/last/avg value for the key.
         * @param key
         * @param val
         */
        public void all(String key, double val) {
            minValue(key, val);
            maxValue(key, val);
            avg(key, val);
        }

        /**
         * Increment the counter at key.
         * @param key
         * @return
         */
        public void inc(String key) {
            entry(key, this.counters).inc();
        }

        private void prepare() {
            // convert raw stats to metrics
            this.metrics = new TreeMap<>();

            for (Entry<String, Stat> e : this.min.entrySet()) {
                this.metrics.put(e.getKey() + ":min", e.getValue().getValue());
            }
            for (Entry<String, Stat> e : this.max.entrySet()) {
                this.metrics.put(e.getKey() + ":max", e.getValue().getValue());
            }
            for (Entry<String, Stat> e : this.avg.entrySet()) {
                this.metrics.put(e.getKey() + ":avg", e.getValue().getAvg());
            }
            for (Entry<String, Stat> e : this.last.entrySet()) {
                this.metrics.put(e.getKey() + ":val", e.getValue().getValue());
            }
            for (Entry<String, Stat> e : this.counters.entrySet()) {
                this.metrics.put(e.getKey() + ":cnt", (double) e.getValue().getCount());
            }

            this.stats = new TreeMap<>();
            for (Entry<String, ServiceStats> e : this.rawStats.entrySet()) {
                Map<String, Double> values = new TreeMap<>();
                this.stats.put(e.getKey(), values);
                for (Entry<String, ServiceStat> se : e.getValue().entries.entrySet()) {
                    ServiceStat st = se.getValue();
                    if (st.name.endsWith(ServiceStats.STAT_NAME_SUFFIX_PER_DAY)) {
                        continue;
                    }
                    double total = st.accumulatedValue != 0 ? st.accumulatedValue : st.latestValue;
                    double avg = total / st.version;
                    values.put(st.name + ":val", total);
                    values.put(st.name + ":avg", avg);
                }
            }
        }

        /**
         * Reports all non-null stats.
         * @param uri
         * @param serviceStats
         */
        public void stats(URI uri, ServiceStats serviceStats) {
            String path = uri.getPath();
            this.rawStats.put(path, serviceStats);
        }
    }

    private static void writeReport(Report report) {
        if (report == null) {
            return;
        }

        if (!SHOULD_WRITE_REPORT) {
            return;
        }

        report.timestamp = new Date().toString();
        report.javaVersion = System.getProperty("java.runtime.version");
        report.hardwareConfig = getHwConfigFromEnv();
        report.jvmArgs = getNonXenonJvmArgs();
        report.xenonArgs = getXenonTestArgs();
        report.id = getIdFromEnv();
        report.os = System.getProperty("os.name") + " " + System.getProperty("os.version");
        Path dest = getReportRootFolder();
        dest = dest.resolve(report.id);

        report.prepare();
        Logger logger = Logger.getAnonymousLogger();
        try {
            Files.createDirectories(dest);
            dest = dest.resolve(report.name + ".json").toAbsolutePath();
            String json = Utils.toJsonHtml(report);
            Files.write(dest, json.getBytes(Utils.CHARSET));

            logger.info(String.format("Report for test run %s written to %s", report.id, dest));
        } catch (IOException e) {
            logger.log(Level.WARNING, "Could not save test results to " + dest, e);
        }
    }

    private static Path getReportRootFolder() {
        String s = System.getenv(ENV_XENON_REPORT_STORE);
        if (s == null) {
            s = DEFAULT_REPORTS_LOCATION;
        }

        return Paths.get(s);
    }

    private static Map<String, Object> getXenonTestArgs() {
        return System.getProperties().entrySet().stream()
                .filter(e -> e.getKey().toString().startsWith(CommandLineArgumentParser.PROPERTY_PREFIX))
                .filter(e -> !e.getKey().toString().equals(PROP_WRITE_REPORT))
                .collect(Collectors.toMap(e -> e.getKey().toString(), Entry::getValue));
    }

    private static String getIdFromEnv() {
        String s = System.getenv(ENV_XENON_ID);
        if (s == null) {
            s = System.getProperty("user.name") + "_" + Utils.getNowMicrosUtc();
        }

        return s;
    }

    private static String getHwConfigFromEnv() {
        String s = System.getenv(ENV_XENON_HW_CONFIG);
        if (s == null) {
            s = "unknown";
        }

        return s;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        this.description = description;
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                    TestResults.writeReport(TestResults.this.report);
                } finally {
                    TestResults.this.report = null;
                }
            }
        };
    }

    private static List<String> getNonXenonJvmArgs() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(s -> !s.startsWith("-D" + CommandLineArgumentParser.PROPERTY_PREFIX))
                .sorted()
                .collect(Collectors.toList());
    }

    public Report getReport() {
        if (this.report == null && !SHOULD_WRITE_REPORT) {
            // a no-op inplace implementation
            this.report = new Report(this.description) {
                @Override
                public void inc(String key) {

                }

                @Override
                public void lastValue(String key, double value) {

                }

                @Override
                public void maxValue(String key, double value) {

                }

                @Override
                public void minValue(String key, double value) {

                }

                @Override
                public void avg(String key, double value) {

                }

                @Override
                public void all(String key, double val) {

                }

                @Override
                public void stats(URI uri, ServiceStats serviceStats) {

                }
            };
        } else {
            if (this.report == null) {
                this.report = new Report(this.description);
            }
        }

        return this.report;
    }
}
