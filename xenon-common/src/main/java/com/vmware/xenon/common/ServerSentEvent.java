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

package com.vmware.xenon.common;

import java.util.Objects;

/**
 * Defines the Server Sent Event model according to <a href="https://www.w3.org/TR/eventsource/">the specification</a>
 */
public class ServerSentEvent {
    /**
     * In case of error, the event's type will be set to this value.
     * The data will be the json encoded error message.
     */
    public static final String EVENT_TYPE_ERROR = "x-xenon-error";

    /**
     * The event type. Optional.
     */
    public String event;

    /**
     * The data for this event.
     */
    public String data;

    /**
     * Event identifier. Optional.
     */
    public String id;

    /**
     * According to the specification, the server can request the client to wait specified time in seconds
     * before reconnecting in case the connection gets closed.
     * The reconnection time in milliseconds. Optional.
     */
    public Long retry;

    /**
     * Sets the event type for this event.
     * @param event
     * @return
     */
    public ServerSentEvent setEvent(String event) {
        this.event = event;
        return this;
    }

    /**
     * Sets the data for this event.
     * @param data
     * @return
     */
    public ServerSentEvent setData(String data) {
        this.data = data;
        return this;
    }

    /**
     * Sets the identifier for this event.
     * @param id
     * @return
     */
    public ServerSentEvent setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Sets the retry field for this event.
     * @param retry
     * @return
     */
    public ServerSentEvent setRetry(Long retry) {
        this.retry = retry;
        return this;
    }

    @Override
    public String toString() {
        return Utils.toJsonHtml(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ServerSentEvent)) {
            return false;
        }
        ServerSentEvent other = (ServerSentEvent) obj;
        return Objects.equals(this.data, other.data) &&
                Objects.equals(this.event, other.event) &&
                Objects.equals(this.id, other.id) &&
                Objects.equals(this.retry, other.retry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.data, this.event, this.id, this.retry);
    }
}
