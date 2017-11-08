package com.vmware.xenon;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.vmware.xenon.common.Utils;

@State(Scope.Thread)
public class LoggingBenchmark {

    private Logger logger;

    @Setup
    public void setup() {
        this.logger = Logger.getLogger("jmh");
        // skip only actual appending, not message generation
        for (Handler handler : Logger.getLogger("").getHandlers()) {
            handler.setFilter(record -> false);
        }
    }

    @Benchmark
    public void methodAwareLogger() {
        Utils.log(logger, 1, "cls", Level.INFO, "no msg");
    }
}
