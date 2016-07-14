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

import java.util.logging.LogRecord;

/**
 * Log formatter which adds color escape character in case when terminal supports colors. Should be used only for
 * console appender and works on Mac, Linux and some Windows terminals.
 * <p/>
 * In case if terminal does not support colors (i.e. TERM environment variable value does not contain "color" substring,
 * output is provided as is with no color escape codes.
 * <p/>
 * This formatter intentionally does not use too dark or too light colors so logs look fine on both black and white
 * background.
 * <p/>
 * Formatter is configured with {@link CommandLineArgumentParser} so colors may be customized via command line args or
 * Java system properties.
 */
public class ColorLogFormatter extends LogFormatter {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";

    public static final String ANSI_DEFAULT = "";

    public static final String TERM = "TERM";
    public static final String COLOR = "color";
    public static final String COLOR_256 = "256";

    public String colorSevere = ANSI_DEFAULT;
    public String colorWarn = ANSI_DEFAULT;
    public String colorConfig = ANSI_DEFAULT;
    public String colorInfo = ANSI_DEFAULT;
    public String colorFine = ANSI_DEFAULT;

    public boolean colorsEnabled;

    public ColorLogFormatter() {
        String term = System.getenv(TERM);
        this.colorsEnabled = term != null && term.contains(COLOR);
        if (this.colorsEnabled) {
            // Use softer colors if there is 256 color terminal capability
            if (term.contains(COLOR_256)) {
                this.colorSevere = "\u001B[38;5;196m";
                this.colorWarn = "\u001B[38;5;215m";
                this.colorConfig = "\u001B[38;5;116m";
                this.colorInfo = ANSI_DEFAULT;
                this.colorFine = "\u001B[38;5;107m";
            } else {
                this.colorSevere = ANSI_RED;
                this.colorWarn = ANSI_YELLOW;
                this.colorConfig = ANSI_CYAN;
                this.colorInfo = ANSI_DEFAULT;
                this.colorFine = ANSI_GREEN;
            }
        }
    }

    protected String getFormattedMessage(LogRecord record) {
        return super.format(record);
    }

    @Override
    public String format(LogRecord record) {
        String formatted = getFormattedMessage(record);
        if (!this.colorsEnabled) {
            return formatted;
        } else {
            String color;
            String reset = ANSI_RESET;
            switch (record.getLevel().getName().substring(0, 1).toUpperCase()) {
            case "S":
                color = this.colorSevere;
                break;
            case "W":
                color = this.colorWarn;
                break;
            case "I":
                color = this.colorInfo;
                break;
            case "F":
                color = this.colorFine;
                break;
            case "C":
                color = this.colorConfig;
                break;
            default:
                color = ANSI_DEFAULT;
                break;
            }
            if (color.equals(ANSI_DEFAULT)) {
                reset = "";
            }
            return color + formatted + reset;
        }
    }
}
