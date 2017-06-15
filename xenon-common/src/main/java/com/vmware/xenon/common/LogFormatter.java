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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter {

    private static final DateTimeFormatter DEFAULT_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public static class LogItem {
        public String l;
        public long id;
        public long t;
        public int threadId;
        public String m;
        public String method;
        public String classOrUri;
        public Throwable thrown;

        public static LogItem create(LogRecord source) {
            LogItem sr = new LogItem();
            sr.l = source.getLevel().toString();
            sr.t = source.getMillis();
            sr.id = source.getSequenceNumber();
            sr.threadId = source.getThreadID();
            sr.m = source.getMessage();
            sr.method = source.getSourceMethodName();
            sr.classOrUri = source.getSourceClassName();
            sr.thrown = source.getThrown();

            if (sr.classOrUri == null) {
                sr.classOrUri = "";
            } else if (sr.classOrUri.startsWith("http")) {
                // Remove leading URI schema & host. Support for ipv6 and path containing
                // colon (ex, http://[::FFFF:129.144.52.38]:9000/group:department).
                //
                // Finding first slash at index 8, calculated based on minimum path starting
                // location in http/https URIs.
                // 0123456789
                // http://a/
                // https://a/
                final int findPathFromIndex = 8;
                int pathIndex = sr.classOrUri.indexOf('/', findPathFromIndex);
                int portIndex = sr.classOrUri
                        .lastIndexOf(':', pathIndex == -1 ? Integer.MAX_VALUE : pathIndex);
                sr.classOrUri = sr.classOrUri.substring(portIndex + 1);
            } else {
                int simpleNameIndex = sr.classOrUri.lastIndexOf('.');
                if (simpleNameIndex != -1) {
                    sr.classOrUri = sr.classOrUri.substring(simpleNameIndex + 1);
                }
            }
            return sr;
        }

        @Override
        public String toString() {
            int mLen = this.m == null ? 0 : this.m.length();
            StringBuilder sb = new StringBuilder(128 + mLen);
            sb.append('[').append(this.id).append(']');
            sb.append('[').append(this.l.charAt(0)).append(']');

            sb.append('[');
            formatTimestampMillisTo(this.t, sb);
            sb.append(']');

            sb.append('[').append(this.threadId).append(']');
            sb.append('[').append(this.classOrUri).append(']');
            sb.append('[').append(this.method).append(']');

            // Always include the message brackets to keep consistent log structure.
            sb.append('[');
            if (mLen > 0) {
                sb.append(this.m);
            }
            sb.append(']');

            return sb.toString();
        }
    }

    public static void formatTimestampMillisTo(long millis, Appendable appendable) {
        long seconds = millis / 1000;
        int nanos = (int) (millis % 1000) * 1_000_000;

        DEFAULT_FORMAT
                .formatTo(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC), appendable);
    }

    @Override
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder(LogItem.create(record).toString());
        sb.append('\n');
        return sb.toString();
    }
}
