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
import java.io.PrintWriter;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import com.vmware.xenon.common.config.XenonConfiguration;

public class LogFormatter extends Formatter {

    private static final boolean LOG_DATE_FIRST = XenonConfiguration
            .bool(LogFormatter.class, "LOG_DATE_FIRST", false);

    private static final DateTimeFormatter DEFAULT_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public static void formatTimestampMillisTo(long millis, Appendable appendable) {
        long seconds = millis / 1000;
        int nanos = (int) (millis % 1000) * 1_000_000;

        DEFAULT_FORMAT
                .formatTo(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC), appendable);
    }

    @Override
    public String format(LogRecord record) {
        int mLen = record.getMessage() == null ? 0 : record.getMessage().length();
        StringBuilder sb = new StringBuilder(128 + mLen);
        if (LOG_DATE_FIRST) {
            formatTimestampMillisTo(record.getMillis(), sb);
        }
        sb.append('[').append(record.getSequenceNumber()).append(']');
        sb.append('[').append(record.getLevel().getName().charAt(0)).append(']');

        if (!LOG_DATE_FIRST) {
            sb.append('[');
            formatTimestampMillisTo(record.getMillis(), sb);
            sb.append(']');
        }

        sb.append('[').append(record.getThreadID()).append(']');
        sb.append('[').append(getClassOrUri(record)).append(']');
        sb.append('[').append(record.getSourceMethodName()).append(']');

        // Always include the message brackets to keep consistent log structure.
        sb.append('[');
        if (mLen > 0) {
            sb.append(record.getMessage());
        }
        sb.append(']');
        if (record.getThrown() != null) {
            sb.append("[\n");

            // write directly to allocated buffer
            PrintWriter pw = new PrintWriter(new Writer() {
                @Override
                public void write(char[] cbuf, int off, int len) throws IOException {
                    sb.append(cbuf, off, len);
                }

                @Override
                public void flush() throws IOException {

                }

                @Override
                public void close() throws IOException {

                }
            });
            record.getThrown().printStackTrace(pw);
            sb.append(']');
        }

        sb.append('\n');

        return sb.toString();
    }

    private String getClassOrUri(LogRecord record) {
        String classOrUri = record.getSourceClassName();
        if (classOrUri == null) {
            classOrUri = "";
        } else if (classOrUri.startsWith("http")) {
            // Remove leading URI schema & host. Support for ipv6 and path containing
            // colon (ex, http://[::FFFF:129.144.52.38]:9000/group:department).
            //
            // Finding first slash at index 8, calculated based on minimum path starting
            // location in http/https URIs.
            // 0123456789
            // http://a/
            // https://a/
            final int findPathFromIndex = 8;
            int pathIndex = classOrUri.indexOf('/', findPathFromIndex);
            int portIndex = classOrUri
                    .lastIndexOf(':', pathIndex == -1 ? Integer.MAX_VALUE : pathIndex);
            classOrUri = classOrUri.substring(portIndex + 1);
        } else {
            int simpleNameIndex = classOrUri.lastIndexOf('.');
            if (simpleNameIndex != -1) {
                classOrUri = classOrUri.substring(simpleNameIndex + 1);
            }
        }

        return classOrUri;
    }
}
