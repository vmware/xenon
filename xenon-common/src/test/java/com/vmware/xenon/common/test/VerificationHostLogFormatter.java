/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.test;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import com.vmware.xenon.common.ColorLogFormatter;
import com.vmware.xenon.common.LogFormatter;

public class VerificationHostLogFormatter extends LogFormatter {

    public static Formatter NORMAL_FORMAT = new NormalFormat();
    public static Formatter COLORED_FORMAT = new ColoredFormat();

    public static class NormalFormat extends LogFormatter {
        @Override
        public String format(LogRecord record) {
            long threadId = Thread.currentThread().getId();
            LogItem logItem = LogItem.create(record);

            // added threadId. the rest is same as LogFormatter
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(logItem.id).append("]");
            sb.append("[").append(logItem.l.substring(0, 1)).append("]");
            sb.append("[").append(logItem.t).append("]");
            sb.append("[").append(threadId).append("]");
            sb.append("[").append(logItem.classOrUri).append("]");
            sb.append("[").append(logItem.method).append("]");
            if (logItem.m != null && !logItem.m.isEmpty()) {
                sb.append("[").append(logItem.m).append("]");
            }

            sb.append("\n");
            return sb.toString();
        }
    }

    public static class ColoredFormat extends ColorLogFormatter {
        @Override
        protected String getFormattedMessage(LogRecord record) {
            return NORMAL_FORMAT.format(record);
        }
    }

}
