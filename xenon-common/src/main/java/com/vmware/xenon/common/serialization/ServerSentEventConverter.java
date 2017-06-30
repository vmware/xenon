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

package com.vmware.xenon.common.serialization;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.vmware.xenon.common.ServerSentEvent;
import com.vmware.xenon.common.Utils;

/**
 * Responsible for the serialization and deserialization of {@link ServerSentEvent}.
 */
public class ServerSentEventConverter {
    public static final ServerSentEventConverter INSTANCE = new ServerSentEventConverter();

    /**
     * The charset used for encoding the messages.
     */
    public static final Charset ENCODING_CHARSET = StandardCharsets.UTF_8;

    private static final String NEW_LINE = "\n";
    private static final String NEW_LINE_REGEX = "\r?\n";

    private static final String FIELD_DATA = "data";
    private static final String FIELD_ID = "id";
    private static final String FIELD_EVENT = "event";
    private static final String FIELD_RETRY = "retry";

    private static final String FIELD_NAME_DELIMITER = ":";
    private static final String FIELD_DELIMITER = NEW_LINE;
    private static final String FIELD_DELIMITER_REGEX = NEW_LINE_REGEX;
    private static final String EVENT_DELIMITER = NEW_LINE;

    private ServerSentEventConverter() {
    }

    /**
     * Serializes the event as described in <a href="https://www.w3.org/TR/eventsource/">the specification</a>.
     * @return The string representation of the event, ready to be written to the connection.
     */
    public String serialize(ServerSentEvent event) {
        StringBuilder sb = Utils.getBuilder();
        if (event.event != null) {
            addField(sb, FIELD_EVENT, event.event);
        }
        if (event.data != null) {
            // We use limit -1 in order to include trailing empty strings
            String[] lines = event.data.split(NEW_LINE_REGEX, -1);
            for (String line: lines) {
                addField(sb, FIELD_DATA, line);
            }
        }
        if (event.id != null) {
            addField(sb, FIELD_ID, event.id);
        }
        if (event.retry != null) {
            addField(sb, FIELD_RETRY, String.valueOf(event.retry.longValue()));
        }
        sb.append(EVENT_DELIMITER);
        return sb.toString();
    }

    private StringBuilder addField(StringBuilder sb, String fieldName, String value) {
        return sb.append(fieldName).append(FIELD_NAME_DELIMITER).append(value).append(FIELD_DELIMITER);
    }

    /**
     * Deserializes an event according to <a href="https://www.w3.org/TR/eventsource/">the specification</a>.
     * @param serialized The string to deserialize
     * @return The deserialized event.
     */
    public ServerSentEvent deserialize(String serialized) {
        ServerSentEvent event = new ServerSentEvent();
        StringBuilder dataBuilder = new StringBuilder();
        boolean dataAdded = false;
        String[] fields = serialized.split(FIELD_DELIMITER_REGEX);
        for (String field: fields) {
            String[] tokens = field.split(FIELD_NAME_DELIMITER, 2);
            if (tokens.length < 2) {
                tokens = new String[] {field, ""};
            }
            String fieldName = tokens[0];
            if (fieldName.isEmpty()) {
                // this is considered a comment
                continue;
            }
            String value = tokens[1];
            if (value.startsWith(" ")) {
                value = value.substring(1);
            }
            switch (fieldName) {
            case FIELD_DATA:
                if (dataAdded) {
                    dataBuilder.append(NEW_LINE);
                }
                dataBuilder.append(value);
                dataAdded = true;
                break;
            case FIELD_EVENT:
                event.event = value;
                break;
            case FIELD_RETRY:
                try {
                    event.retry = Long.parseLong(value, 10);
                } catch (NumberFormatException e) {
                    // ignore -- acceptable according to the specification
                }
                break;
            case FIELD_ID:
                event.id = value;
                break;
            default:
                continue;
            }
        }
        if (dataAdded) {
            event.data = dataBuilder.toString();
        }
        return event;
    }
}
