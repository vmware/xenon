/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package org.slf4j.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.LogManager;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceHostLogService.LogServiceState;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Slf4J logger test.
 */
public class Slf4JLoggerTest extends BasicTestCase {
    private static final Logger logger = LoggerFactory.getLogger(Slf4JLoggerTest.class);

    public static final int RETRY_DELAY = 100;

    @Test
    public void testLogger() throws Throwable {
        host.setLoggingLevel(Level.FINEST);
        LogManager.getLogManager().getLogger("").setLevel(Level.FINEST);

        final String exceptionMsgSegment = "exception";

        logger.trace("This is a trace test with no args");
        logger.trace("This is a trace test with no args");
        logger.trace("This is a trace test with 1 arg: {}", "argument");
        logger.trace("This is a trace test with 2 args: {}, {}", "argument1", "argument2");
        logger.trace("This is a trace test with 3 args: {}, {}, {}", "argument1", "argument2", "argument3");
        logger.trace("This is a trace test with 3 args: {}, {}, {} {} {}",
                "argument1", "argument2", "argument3", exceptionMsgSegment,
                new IllegalStateException("Some cause"));

        logger.debug("This is a debug test with no args");
        logger.debug("This is a debug test with no args");
        logger.debug("This is a debug test with 1 arg: {}", "argument");
        logger.debug("This is a debug test with 2 args: {}, {}", "argument1", "argument2");
        logger.debug("This is a debug test with 3 args: {}, {}, {}", "argument1", "argument2", "argument3");
        logger.debug("This is a debug test with 3 args: {}, {}, {} {} {}",
                "argument1", "argument2", "argument3", exceptionMsgSegment,
                new IllegalStateException("Some cause"));

        logger.info("hello methodName");
        logger.info("hello methodName {}", "param");
        logger.info("This is a info test with no args");
        logger.info("This is a info test with no args");
        logger.info("This is a info test with 1 arg: {}", "argument");
        logger.info("This is a info test with 2 args: {}, {}", "argument1", "argument2");
        logger.info("This is a info test with 3 args: {}, {}, {}", "argument1", "argument2", "argument3");
        logger.info("This is a info test with 3 args: {}, {}, {} {} {}",
                "argument1", "argument2", "argument3", exceptionMsgSegment,
                new IllegalStateException("Some cause"));

        logger.warn("This is a warn test with no args");
        logger.warn("This is a warn test with no args");
        logger.warn("This is a warn test with 1 arg: {}", "argument");
        logger.warn("This is a warn test with 2 args: {}, {}", "argument1", "argument2");
        logger.warn("This is a warn test with 3 args: {}, {}, {}", "argument1", "argument2", "argument3");
        logger.warn("This is a warn test with 3 args: {}, {}, {} {} {}",
                "argument1", "argument2", "argument3", exceptionMsgSegment,
                new IllegalStateException("Some cause"));

        logger.error("This is a error test with no args");
        logger.error("This is a error test with no args");
        logger.error("This is a error test with 1 arg: {}", "argument");
        logger.error("This is a error test with 2 args: {}, {}", "argument1", "argument2");
        logger.error("This is a error test with 3 args: {}, {}, {}", "argument1", "argument2", "argument3");
        logger.error("This is a error test with 3 args: {}, {}, {} {} {}",
                "argument1", "argument2", "argument3", exceptionMsgSegment,
                new IllegalStateException("Some cause"));

        host.setLoggingLevel(Level.OFF);
        LogManager.getLogManager().getLogger("").setLevel(Level.OFF);

        logger.trace("This message should not present");
        logger.debug("This message should not present");
        logger.info("This message should not present");
        logger.warn("This message should not present");
        logger.error("This message should not present");

        host.setLoggingLevel(Level.SEVERE);
        LogManager.getLogManager().getLogger("").setLevel(Level.SEVERE);

        final String endOfLogMarker = UUID.randomUUID().toString();
        logger.error(endOfLogMarker);

        host.testStart(1);
        AtomicReference<LogServiceState> stateRef = new AtomicReference<>();
        Operation.createGet(UriUtils.buildUri(host, ServiceUriPaths.PROCESS_LOG))
                .setReferer(host.getPublicUri())
                .setCompletion(new CompletionHandler() {
                    @Override
                    public void handle(Operation op, Throwable ex) {
                        if (ex == null) {
                            LogServiceState log = op.getBody(LogServiceState.class);
                            if (log.items.stream().filter(s -> s.contains(endOfLogMarker)).count() > 0) {
                                stateRef.set(log);
                                host.completeIteration();
                            } else {
                                // The log may be incomplete. In that case schedule a retry attempt.
                                host.schedule(() ->
                                        Operation.createGet(UriUtils.buildUri(host, ServiceUriPaths.PROCESS_LOG))
                                                .setReferer(host.getPublicUri())
                                                .setCompletion(this)
                                                .sendWith(host), RETRY_DELAY, TimeUnit.MILLISECONDS);
                            }
                        } else {
                            host.failIteration(ex);
                        }
                    }
                })
                .sendWith(host);
        host.testWait();
        LogServiceState state = stateRef.get();
        host.setLoggingLevel(Level.INFO);
        this.host.log("Log state: %s", Utils.toJsonHtml(state));

        // log statements might be preempted by xenon log output, or replaced. This causes test failures. Given
        // the non critical nature of this log bridge, we just assert that at least one log statement, at each log level
        // is observed

        int count = 0;
        count = checkLogLine(state.items, "This is a trace test with no args", count);
        count = checkLogLine(state.items, "This is a trace test with 1 arg: argument", count);
        count = checkLogLine(state.items, "This is a trace test with 2 args: argument1, argument2",
                count);
        count = checkLogLine(state.items,
                "This is a trace test with 3 args: argument1, argument2, argument3", count);
        assertTrue(count > 1);

        count = 0;
        count = checkLogLine(state.items, "This is a debug test with no args", count);
        count = checkLogLine(state.items, "This is a debug test with 1 arg: argument", count);
        count = checkLogLine(state.items, "This is a debug test with 2 args: argument1, argument2",
                count);
        count = checkLogLine(state.items,
                "This is a debug test with 3 args: argument1, argument2, argument3", count);
        assertTrue(count > 1);

        count = 0;
        count = checkLogLine(state.items, "This is a info test with no args", count);
        count = checkLogLine(state.items, "This is a info test with 1 arg: argument", count);
        count = checkLogLine(state.items, "This is a info test with 2 args: argument1, argument2",
                count);
        count = checkLogLine(state.items,
                "This is a info test with 3 args: argument1, argument2, argument3", count);
        assertTrue(count > 1);

        count = 0;
        count = checkLogLine(state.items, "This is a warn test with no args", count);
        count = checkLogLine(state.items, "This is a warn test with 1 arg: argument", count);
        count = checkLogLine(state.items, "This is a warn test with 2 args: argument1, argument2",
                count);
        count = checkLogLine(state.items,
                "This is a warn test with 3 args: argument1, argument2, argument3", count);
        assertTrue(count > 1);

        count = 0;
        count = checkLogLine(state.items, "This is a error test with no args", count);
        count = checkLogLine(state.items, "This is a error test with 1 arg: argument", count);
        count = checkLogLine(state.items, "This is a error test with 2 args: argument1, argument2",
                count);
        count = checkLogLine(state.items,
                "This is a error test with 3 args: argument1, argument2, argument3", count);
        assertTrue(count > 1);

        checkLogLineNegative(state.items, "This message should not present");

        // "testLogger" is the name of this test; test if it is correctly extracted and logged
        count = 0;
        count = checkLogLine(state.items, "[testLogger][hello methodName", count);
        assertEquals(1, count);


        count = 0;
        count = checkLogLine(state.items, "[testLogger][hello methodName param", count);
        assertEquals(1, count);
        // multi line log statements can be pre empted by xenon logs so we can rely on exact match.
        // Instead just verify the beginning is present
        for (String line : state.items) {
            if (line.contains(exceptionMsgSegment)) {
                count++;
            }
        }

        assertTrue(count > 1);
    }

    private int checkLogLine(List<String> items, String line, int count) {
        for (String item : items) {
            if (item.contains(line)) {
                count++;
                return count;
            }
        }

        return count;
    }

    private void checkLogLineNegative(List<String> items, String line) {
        for (String item : items) {
            if (item.contains(line)) {
                Assert.fail("There should be no log line with the following substring: " + line);
            }
        }
    }
}
