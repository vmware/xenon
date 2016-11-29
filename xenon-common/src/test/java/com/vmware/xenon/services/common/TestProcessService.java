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

package com.vmware.xenon.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.SystemHostInfo.OsFamily;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;

public class TestProcessService extends BasicTestCase {

    public void beforeHostStart(VerificationHost host) {
        host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS
                .toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
    }

    @Test
    public void startProcess() throws Throwable {
        final ProcessState[] state = { new ProcessState() };
        if (host.getSystemInfo().osFamily == OsFamily.WINDOWS) {
            state[0].arguments = new String[] { "cmd.exe", "/C", "echo" };
        } else {
            state[0].arguments = new String[] { "echo" };
        }

        this.host.testStart(1);

        final URI[] processURI = { null };
        URI uri = UriUtils.buildUri(this.host, ProcessFactoryService.SELF_LINK);
        Operation op = Operation.createPost(uri)
                .setBody(state[0])
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    ProcessState newState = o.getBody(ProcessState.class);
                    processURI[0] = UriUtils.buildUri(this.host, newState.documentSelfLink);
                    this.host.completeIteration();
                });

        this.host.send(op);
        this.host.testWait();

        int startCount;
        int startDelayMillis;

        startCount = getStat(processURI[0], ProcessService.STAT_NAME_START_COUNT);
        assertTrue(startCount >= 1);

        Date expiration = this.host.getTestExpiration();
        while (new Date().before(expiration)) {
            startCount = getStat(processURI[0], ProcessService.STAT_NAME_START_COUNT);
            if (startCount >= 3) {
                // The process was restarted at least twice.
                // Test that the back-off kicked in after the first restart.
                startDelayMillis = getStat(processURI[0],
                        ProcessService.STAT_NAME_START_DELAY_MILLIS);
                assertTrue(startDelayMillis > 0);
                return;
            }

            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(this.host.getMaintenanceIntervalMicros()));
        }
    }

    @Test
    public void testLongRunning() throws Throwable {
        if (host.getSystemInfo().osFamily == OsFamily.WINDOWS) {
            return;
        }

        final ProcessState[] state = { new ProcessState() };
        // issue a sleep for a random number of seconds between 100 and 200
        String processCommandLine = "sleep";
        Random r = new Random();
        String sleepTime = Integer.toString((r.nextInt(100) + 100));
        state[0].arguments = new String[] { processCommandLine, sleepTime };
        state[0].isRestartRequired = false;

        // create the process
        final URI[] processURI = { null };
        this.host.sendAndWait(Operation.createPost(
                    UriUtils.buildUri(this.host, ProcessFactoryService.SELF_LINK))
                .setBody(state[0])
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    ProcessState newState = o.getBody(ProcessState.class);
                    processURI[0] = UriUtils.buildUri(this.host, newState.documentSelfLink);
                    this.host.completeIteration();
                }));

        // ensure process is up
        String processCheck = "ps -ef | grep \"" + processCommandLine + " " + sleepTime + "\" | grep -v \"grep\" | wc -l";
        waitForProcess(processCheck, true);

        // issue a delete and ensure process has been stopped
        this.host.sendAndWait(Operation.createDelete(processURI[0])
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                }));
        waitForProcess(processCheck, false);
    }

    private void waitForProcess(String commandToExecute, boolean exists) throws Throwable {
        this.host.waitFor("Process state did not change", () -> {
            ProcessBuilder pb = new ProcessBuilder("sh", "-c", commandToExecute);
            Process process = pb.start();
            BufferedReader bReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            Integer outputState = Integer.valueOf(bReader.readLine().trim());
            if (outputState.equals(exists ? 1 : 0)) {
                return true;
            }
            return false;
        });
    }

    @Test
    public void processThatCannotStart() throws Throwable {
        ProcessState state = new ProcessState();
        state.arguments = new String[1];
        state.arguments[0] = "command-that-doesnt-exist";
        state.logFile = null;

        this.host.testStart(1);

        URI uri = UriUtils.buildUri(this.host, ProcessFactoryService.SELF_LINK);
        Operation op = Operation.createPost(uri)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (!(e instanceof java.io.IOException)) {
                        e = new Throwable("Expected java.io.IOException");
                        this.host.failIteration(e);
                        return;
                    }

                    this.host.completeIteration();
                });

        this.host.send(op);
        this.host.testWait();
    }

    @Test
    public void testNoRestarts() throws Throwable {
        String FILE_CONTENTS = "processServiceTest";
        // This test cannot be run under windows (requires bash and I/O redirection)
        if (host.getSystemInfo().osFamily == OsFamily.WINDOWS) {
            return;
        }

        final ProcessState[] state = { new ProcessState() };
        String fileName = System.getProperty("user.dir") + "/" + UUID.randomUUID().toString();
        // If this works correctly, this should be appended only once!
        String childProc = "echo '" + FILE_CONTENTS + "' >> " + fileName;

        state[0].arguments = new String[] { "sh", "-c", childProc };
        state[0].isRestartRequired = false;

        this.host.testStart(1);

        final URI[] processURI = { null };
        URI uri = UriUtils.buildUri(this.host, ProcessFactoryService.SELF_LINK);
        Operation op = Operation.createPost(uri)
                .setBody(state[0])
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    ProcessState newState = o.getBody(ProcessState.class);
                    processURI[0] = UriUtils.buildUri(this.host, newState.documentSelfLink);
                    this.host.completeIteration();
                });

        this.host.send(op);
        this.host.testWait();

        File f = new File(fileName);
        while ((!f.exists()) && f.length() < 19) {
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(this.host.getMaintenanceIntervalMicros()));
        }

        String contents = new String(Files.readAllBytes(Paths.get(fileName)));
        if ((FILE_CONTENTS + "\n").equals(contents)) {
            assertEquals(FILE_CONTENTS + "\n", contents);
        }

        Files.delete(Paths.get(fileName));
    }

    public int getStat(URI uri, String name) throws Throwable {
        URI statsURI = UriUtils.buildStatsUri(uri);
        ServiceStats stats = this.host.getServiceState(null, ServiceStats.class, statsURI);
        double startCount = stats.entries.get(name).latestValue;
        return (int) startCount;
    }

}
