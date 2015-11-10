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

package com.vmware.dcp.services.common;

import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;

public class ProcessService extends StatefulService {
    static final String STAT_NAME_START_COUNT = "startCount";
    static final String STAT_NAME_START_DELAY_MILLIS = "startDelayMillis";

    // Never wait more than this amount between process starts;
    static final long MAX_START_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(120);

    // If the process is up for longer than this amount, reset back-off.
    static final long STABLE_THRESHOLD_MILLIS = TimeUnit.SECONDS.toMillis(60);

    // lines to tail if the process restarts.
    static final int LINES = 100;

    private long startDelayExp = 0;
    private long startDelayMillis = 0;

    private Process process;
    private int processExitStatus;
    private long processStartTimeMillis;
    private long processStopTimeMillis;

    public ProcessService() {
        super(ProcessState.class);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    protected boolean stopProcess() {
        if (this.process == null) {
            return false;
        }

        // Kill process if it hasn't exited already.
        if (this.process.isAlive()) {
            this.process.destroyForcibly();

            // Wait for process to really terminate, so we can get its exit status.
            // The documentation of the Process class tells us that isAlive may be true for a brief
            // period after calling destroyForcibly. Be aware that using blocks of code that
            // potentially block is highly discouraged, but that we don't have an alternative here.
            // Given that process deletion is rare and expected to only happen on process shutdown,
            // we allow it here.
            while (this.process.isAlive()) {
                try {
                    this.process.waitFor();
                } catch (InterruptedException ignored) {
                }
            }
        }

        this.processExitStatus = this.process.exitValue();
        this.processStopTimeMillis = (new Date()).getTime();
        this.process = null;

        return true;
    }

    protected void startProcess(Operation op, ProcessState state) {
        if (this.stopProcess()) {
            // Reset back-off if the process was alive longer than STABLE_THRESHOLD_MILLIS.
            // This threshold determines when we no longer see a process as flapping.
            if ((this.processStopTimeMillis - this.processStartTimeMillis) >= STABLE_THRESHOLD_MILLIS) {
                this.startDelayExp = 0;
            }

            this.startDelayMillis = TimeUnit.SECONDS
                    .toMillis((int) Math.pow(2, this.startDelayExp) - 1);
            if (this.startDelayMillis < MAX_START_DELAY_MILLIS) {
                this.startDelayExp++;
            } else {
                this.startDelayMillis = MAX_START_DELAY_MILLIS;
            }

            // print the log before restarting
            if (state.logLink != null && !state.logLink.isEmpty()) {
                Operation getLog = Operation.createGet(UriUtils.buildUri(this.getHost(),
                        state.logLink, "lineCount=" + LINES))
                        .setCompletion((o, e) -> {
                            if (e != null) {
                                logWarning("Unable to get logs: %s", e.getMessage());
                                return;
                            }
                            String json = Utils.toJsonHtml(o.getBodyRaw());
                            logInfo("%s", json);
                        });
                sendRequest(getLog);
            }

            setStat(STAT_NAME_START_DELAY_MILLIS, this.startDelayMillis);
            logWarning("Process %s exited with status %d, restarting in %ds",
                    state.arguments[0],
                    this.processExitStatus,
                    TimeUnit.MILLISECONDS.toSeconds(this.startDelayMillis));
        }

        // Check if we need to wait longer before starting the process again.
        if ((new Date()).getTime() < this.processStopTimeMillis + this.startDelayMillis) {
            op.complete();
            return;
        }

        logInfo("Starting %s", state.arguments[0]);
        adjustStat(STAT_NAME_START_COUNT, 1);

        ProcessBuilder pb = new ProcessBuilder(state.arguments);
        if (state.logFile != null) {
            File file = new File(state.logFile);
            pb.redirectErrorStream(true);
            pb.redirectOutput(Redirect.appendTo(file));
        }

        try {
            this.process = pb.start();
            this.processStartTimeMillis = (new Date()).getTime();
        } catch (Throwable e) {
            logWarning("Failure starting %s (%s)",
                    state.arguments[0],
                    e.toString());
            op.fail(e);
            return;
        }

        op.complete();
    }

    @Override
    public void handleStart(Operation op) {
        ProcessState state = op.getBody(ProcessState.class);
        if (state.arguments.length == 0) {
            op.fail(new IllegalArgumentException("No arguments specified"));
            return;
        }

        if (!state.isRestartRequired) {
            toggleOption(ServiceOption.PERIODIC_MAINTENANCE, false);
        }

        this.startProcess(op, state);
    }

    @Override
    public void handleDelete(Operation op) {
        this.stopProcess();
        op.complete();
    }

    @Override
    public void handleMaintenance(Operation op) {
        // Process is alive, nothing to do
        if (this.process != null && this.process.isAlive()) {
            op.complete();
            return;
        }

        // Process is NOT alive, try to start it.
        sendRequest(Operation.createGet(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }

                    ProcessState state = o.getBody(ProcessState.class);
                    this.startProcess(op, state);
                }));
    }
}
