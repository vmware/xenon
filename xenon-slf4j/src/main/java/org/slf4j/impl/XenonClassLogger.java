/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package org.slf4j.impl;

import java.io.ObjectStreamException;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

import com.vmware.xenon.common.Utils;

/**
 * Xenon-based class logger for slf4j.
 */
public class XenonClassLogger extends MarkerIgnoringBase implements Logger {
    private static final long serialVersionUID = 0L;

    private final transient java.util.logging.Logger logger;

    public XenonClassLogger(String name) {
        this.logger = java.util.logging.Logger.getLogger(name);
        this.name = name;
    }

    @Override
    public boolean isTraceEnabled() {
        return this.logger.isLoggable(Level.FINEST);
    }

    @Override
    public void trace(String msg) {
        doLog(Level.FINEST, msg);
    }

    @Override
    public void trace(String format, Object arg) {
        doLog(Level.FINEST, format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        doLog(Level.FINEST, format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        doLog(Level.FINEST, format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        doLog(Level.FINEST, msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return this.logger.isLoggable(Level.FINE);
    }

    @Override
    public void debug(String msg) {
        doLog(Level.FINE, msg);
    }

    @Override
    public void debug(String format, Object arg) {
        doLog(Level.FINE, format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        doLog(Level.FINE, format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        doLog(Level.FINE, format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        doLog(Level.FINE, msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return this.logger.isLoggable(Level.INFO);
    }

    @Override
    public void info(String msg) {
        doLog(Level.INFO, msg);
    }

    @Override
    public void info(String format, Object arg) {
        doLog(Level.INFO, format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        doLog(Level.INFO, format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        doLog(Level.INFO, format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        doLog(Level.INFO, msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return this.logger.isLoggable(Level.WARNING);
    }

    @Override
    public void warn(String msg) {
        doLog(Level.WARNING, msg);
    }

    @Override
    public void warn(String format, Object arg) {
        doLog(Level.WARNING, format, arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        doLog(Level.WARNING, format, arguments);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        doLog(Level.WARNING, format, arg1, arg2);
    }

    @Override
    public void warn(String msg, Throwable t) {
        doLog(Level.WARNING, msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return this.logger.isLoggable(Level.SEVERE);
    }

    @Override
    public void error(String msg) {
        doLog(Level.SEVERE, msg);
    }

    @Override
    public void error(String format, Object arg) {
        doLog(Level.SEVERE, format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        doLog(Level.SEVERE, format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        doLog(Level.SEVERE, format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        doLog(Level.SEVERE, msg, t);
    }

    private void doLog(Level lvl, String msg) {
        if (this.logger.isLoggable(lvl)) {
            Utils.log(this.logger, 4, name, lvl, "%s", msg);
        }
    }

    private void doLog(Level lvl, String format, Object arg) {
        if (this.logger.isLoggable(lvl)) {
            FormattingTuple tuple = MessageFormatter.format(format, arg);
            doLog(lvl, tuple);
        }
    }

    private void doLog(Level lvl, String format, Object arg1, Object arg2) {
        if (this.logger.isLoggable(lvl)) {
            FormattingTuple tuple = MessageFormatter.format(format, arg1, arg2);
            doLog(lvl, tuple);
        }
    }

    private void doLog(Level lvl, String format, Object... args) {
        if (this.logger.isLoggable(lvl)) {
            FormattingTuple tuple = MessageFormatter.arrayFormat(format, args);
            doLog(lvl, tuple);
        }
    }

    private void doLog(Level lvl, FormattingTuple tuple) {
        if (tuple.getThrowable() == null) {
            Utils.log(this.logger, 5, this.name, lvl, "%s", tuple.getMessage());
        } else {
            Utils.log(this.logger, 5, this.name, lvl, "%s: %s", tuple.getMessage(), Utils.toString(tuple.getThrowable()));
        }
    }

    protected Object readResolve() throws ObjectStreamException {
        return LoggerFactory.getLogger(this.getName());
    }
}
