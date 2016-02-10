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

package com.vmware.xenon.common.logging;

import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * LogRecord that contains invoked line of code StackTraceElement.
 *
 * StackTraceElement contains detailed information about where log method was called.
 */
public class StackAwareLogRecord extends LogRecord {

    private static final long serialVersionUID = 2233534645443849237L;
    private StackTraceElement stack;

    public StackAwareLogRecord(Level level, String msg) {
        super(level, msg);
    }

    public String getStackClassName() {
        return this.stack == null ? null : this.stack.getClassName();
    }

    public String getStackMethodName() {
        return this.stack == null ? null : this.stack.getMethodName();
    }

    public String getStackFileName() {
        return this.stack == null ? null : this.stack.getFileName();
    }

    public int getStackLineNumber() {
        return this.stack == null ? -1 : this.stack.getLineNumber();
    }

    public StackTraceElement getStack() {
        return this.stack;
    }

    public void setStack(StackTraceElement stack) {
        this.stack = stack;
    }
}
