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

package com.vmware.dcp.common;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter {

    public static class LogItem {
        public String l;
        public long id;
        public long t;
        public String m;
        public String method;
        public String classOrUri;

        public static LogItem create(LogRecord source) {
            LogItem sr = new LogItem();
            sr.l = source.getLevel().toString();
            sr.t = source.getMillis();
            sr.id = source.getSequenceNumber();
            sr.m = source.getMessage();
            sr.method = source.getSourceMethodName();
            sr.classOrUri = source.getSourceClassName();
            if (sr.classOrUri == null) {
                return sr;
            }
            if (sr.classOrUri.startsWith("http")) {
                int portIndex = sr.classOrUri.lastIndexOf(":");
                sr.classOrUri = sr.classOrUri.substring(portIndex + 1);
            } else {
                int simpleNameIndex = sr.classOrUri.lastIndexOf(".");
                if (simpleNameIndex != -1) {
                    sr.classOrUri = sr.classOrUri.substring(simpleNameIndex + 1);
                }
            }
            return sr;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(this.id).append("]");
            sb.append("[").append(this.l.substring(0, 1)).append("]");
            sb.append("[").append(this.t).append("]");
            sb.append("[").append(this.classOrUri).append("]");
            sb.append("[").append(this.method).append("]");
            if (this.m != null && !this.m.isEmpty()) {
                sb.append("[").append(this.m).append("]");
            }

            return sb.toString();
        }
    }

    @Override
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder();
        sb.append(LogItem.create(record).toString());
        sb.append("\n");
        return sb.toString();
    }
}
