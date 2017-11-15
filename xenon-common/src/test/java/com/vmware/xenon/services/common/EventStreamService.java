/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServerSentEvent;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.Utils;

/**
 * Event stream service for testing the Server Sent Events functionality.
 * On [GET] this service replays the passed in events with the specified initial delay and period.
 */
public class EventStreamService extends StatelessService {
    public static final String SELF_LINK = "/test/event-stream";

    private final List<ServerSentEvent> events;
    private final long initialDelay;
    private final long period;
    private final TimeUnit timeUnit;
    private final ExecutorService executorService;

    private int repeat;
    private Exception failException;
    private Operation sessionOp;

    /**
     * @param events The events to replay
     * @param initialDelay The initial delay
     * @param period The period with which to emit the messages
     * @param timeUnit
     * @param parallelism The number of parallel operations that the service can handle
     */
    public EventStreamService(List<ServerSentEvent> events, long initialDelay, long period, TimeUnit timeUnit,
            int parallelism, int repeat) {
        this.events = events;
        this.initialDelay = initialDelay;
        this.period = period;
        this.timeUnit = timeUnit;
        // Intentionally using ThreadPoolExecutor instead of ScheduledExecutor, in order to simulate load.
        this.executorService = Executors.newFixedThreadPool(parallelism);
        this.repeat = repeat;
    }

    @Override
    public void handlePost(Operation post) {
        if (this.sessionOp != null) {
            post.fail(Operation.STATUS_CODE_CONFLICT);
            return;
        }
        post.startEventStream();
        this.sessionOp = post;
    }

    @Override
    public void handlePatch(Operation patch) {
        if (this.sessionOp == null) {
            patch.fail(new IllegalStateException("session does not exist"));
            return;
        }
        ServerSentEvent event = new ServerSentEvent()
                .setData(Utils.toJson(patch.getBodyRaw()))
                .setCompletionCallback(t -> {
                    if (t != null) {
                        patch.fail(t);
                        return;
                    }
                    patch.complete();
                });
        this.sessionOp.sendServerSentEvent(event);
    }

    @Override
    public void handleDelete(Operation delete) {
        if (this.sessionOp != null) {
            this.sessionOp.complete();
            this.sessionOp = null;
        }
        super.handleDelete(delete);
    }

    @Override
    public void handleGet(Operation get) {
        this.replayEvents(get);
    }

    /**
     * @param failException If not null, upon the end of the replay the service will fail with this
     *                      exception, otherwise the operation will complete successfully.
     */
    public void setFailException(Exception failException) {
        this.failException = failException;
    }

    private void replayEvents(Operation op) {
        op.startEventStream();
        this.executorService.execute(() -> {
            sleep(op, this.timeUnit.toMillis(this.initialDelay));
            for (int r = 0; r < this.repeat; r++) {
                for (int i = 0; i < this.events.size(); ++i) {
                    op.sendServerSentEvent(this.events.get(i));
                    if (i < this.events.size() - 1) {
                        sleep(op, this.timeUnit.toMillis(this.period));
                    }
                }
            }

            if (this.failException == null) {
                op.complete();
            } else {
                op.fail(this.failException);
            }
        });
    }

    private void sleep(Operation op, long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            op.fail(e);
        }
    }

    @Override
    public void handleStop(Operation delete) {
        this.executorService.shutdownNow();
    }
}
