/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.config;

import static com.vmware.xenon.common.config.ConfigurationSource.subsystemToString;

import java.time.Duration;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads configuration from environment. First system properties are examined then ENV variables.
 * To configure the property "maxTTL" of an imaginary class SystemLimits you have to pass either of these:
 * -Dxenon.SystemLimits.maxTTL=250
 * export XENON_SYSTEM_LIMITS_MAX_TTL=250
 */
public final class XenonConfiguration {
    private final ConfigurationSource source;

    static final Logger logger = Logger.getLogger(XenonConfiguration.class.getName());

    private static final XenonConfiguration INSTANCE = new XenonConfiguration(new ConfigurationSource());

    XenonConfiguration(ConfigurationSource source) {
        this.source = source;
    }

    /**
     * Returns a double-precision number.
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static double decimal(Class<?> subsystem, String property, double defaultValue) {
        return INSTANCE.getDecimal(subsystem, property, defaultValue);
    }

    /**
     * Returns a long interger, can be negative.
     * @see XenonConfiguration#integer(Class, String, long)
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static long number(Class<?> subsystem, String property, long defaultValue) {
        return INSTANCE.getInteger(subsystem, property, defaultValue);
    }

    /**
     * Same as {@link #number(Class, String, long)} but result is cast to int.
     *
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static int integer(Class<?> subsystem, String property, long defaultValue) {
        return (int) INSTANCE.getInteger(subsystem, property, defaultValue);
    }

    /**
     * Return a number of microseconds. This methods understand these suffixes:
     * ms for millis
     * s for seconds
     * h for hours
     * d for days.
     *
     * No suffix means microseconds. Duration cannot be negative.
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static long duration(Class<?> subsystem, String property, Duration defaultValue) {
        return INSTANCE.getDuration(subsystem, property, defaultValue);
    }

    /**
     * See {@link #duration(Class, String, Duration)}
     * @param subsystem
     * @param property
     * @param unit
     * @param duration
     * @return
     */
    public static long duration(Class<?> subsystem, String property, TimeUnit unit, long duration) {
        return INSTANCE.getDuration(subsystem, property, Duration.ofNanos(unit.toNanos(duration)));
    }

    /**
     * Retrieves a string-valued config parameter.
     * Use this method only for backward compatibility.
     *
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static String string(Class<?> subsystem, String property, String defaultValue) {
        return INSTANCE.getString(subsystem, property, defaultValue);
    }

    /**
     * Parse
     * @param subsystem
     * @param property
     * @param enumType
     * @param defaultValue
     * @param <T>
     * @return
     */
    public static <T extends Enum<T>> T choice(Class<?> subsystem, String property, Class<T> enumType,
            T defaultValue) {
        return INSTANCE.getChoice(subsystem, property, enumType, defaultValue);
    }

    /**
     * Same as {@link #string(Class, String, String)} but without exposing senstivie information in the
     * logs.
     *
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static String secret(Class<?> subsystem, String property, String defaultValue) {
        return INSTANCE.getSecret(subsystem, property, defaultValue);
    }

    /**
     * Use this method only for backward compatibility.
     * @see #bool(Class, String, boolean)
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static boolean bool(String subsystem, String property, boolean defaultValue) {
        return INSTANCE.getBool(subsystem, property, defaultValue);
    }

    /**
     * Retrieves a boolean config params. 1 and 0 values are treated as true and false.
     * Otherwise {@link Boolean#parseBoolean(String)} conventions are followed.
     * @param subsystem
     * @param property
     * @param defaultValue
     * @return
     */
    public static boolean bool(Class<?> subsystem, String property, boolean defaultValue) {
        return INSTANCE.getBool(subsystem, property, defaultValue);
    }

    String getString(Class<?> subsystem, String property, String defaultValue) {
        String v = this.source.get(subsystem, property, true);
        if (v == null) {
            logUsingDefaultBecauseUnset(subsystem, property, defaultValue, true);
            return defaultValue;
        } else {
            return v;
        }
    }

    <T extends Enum<T>> T getChoice(Class<?> subsystem, String property, Class<T> enumType, T defaultValue) {
        String v = this.source.get(subsystem, property, true);
        if (v == null) {
            logUsingDefaultBecauseUnset(subsystem, property, defaultValue, true);
            return defaultValue;
        } else {
            try {
                T res = Enum.valueOf(enumType, v);
                return res;
            } catch (IllegalArgumentException e) {
                logUsingDefaultBecauseBadValue(subsystem, property, defaultValue, v);
                return defaultValue;
            }
        }
    }

    boolean getBool(String subsystem, String property, boolean defaultValue) {
        String v = this.source.get(subsystem, property, true);

        if (v == null) {
            logUsingDefaultBecauseUnset(subsystem, property, defaultValue, true);
            return defaultValue;
        }

        // popular way of setting booleans in env vars
        Long num = parseOr(v);
        if (num != null && num == 0) {
            return false;
        } else if (num != null && num == 1) {
            return true;
        }

        return Boolean.parseBoolean(v);
    }

    boolean getBool(Class<?> subsystem, String property, boolean defaultValue) {
        return getBool(subsystem.getSimpleName(), property, defaultValue);
    }

    String getSecret(Class<?> subsystem, String property, String defaultValue) {
        String v = this.source.get(subsystem, property, false);

        if (v == null) {
            logUsingDefaultBecauseUnset(subsystem, property, defaultValue, false);
            return defaultValue;
        } else {
            return v;
        }
    }

    long getDuration(Class<?> subsystem, String property, Duration defaultValue) {
        String v = this.source.get(subsystem, property, true);
        long dv = defaultValue.toNanos() / 1000;

        if (v == null) {
            logUsingDefaultBecauseUnset(subsystem, property, dv, true);
            return dv;
        }

        try {
            long micros = Long.parseLong(v);
            if (micros < 0) {
                logUsingDefaultBecauseBadValue(subsystem, property, dv, v);
                return dv;
            } else {
                return micros;
            }
        } catch (NumberFormatException ignore) {

        }

        if (v.endsWith("ms")) {
            Long ms = parseOr(stripSuffix(v, 2));
            if (ms != null && ms >= 0) {
                return TimeUnit.MILLISECONDS.toMicros(ms);
            } else {
                logUsingDefaultBecauseBadValue(subsystem, property, dv, v);
                return dv;
            }
        }

        if (v.endsWith("h")) {
            Long h = parseOr(stripSuffix(v, 1));
            if (h != null && h >= 0) {
                return TimeUnit.HOURS.toMicros(h);
            } else {
                logUsingDefaultBecauseBadValue(subsystem, property, dv, v);
                return dv;
            }
        }

        if (v.endsWith("d")) {
            Long days = parseOr(stripSuffix(v, 1));
            if (days != null && days >= 0) {
                return TimeUnit.DAYS.toMicros(days);
            } else {
                logUsingDefaultBecauseBadValue(subsystem, property, dv, v);
                return dv;
            }
        }

        if (v.endsWith("s")) {
            Long sec = parseOr(stripSuffix(v, 1));
            if (sec != null && sec >= 0) {
                return TimeUnit.SECONDS.toMicros(sec);
            } else {
                logUsingDefaultBecauseBadValue(subsystem, property, dv, v);
                return dv;
            }
        }

        if (v.endsWith("m")) {
            Long min = parseOr(stripSuffix(v, 1));
            if (min != null && min >= 0) {
                return TimeUnit.MINUTES.toMicros(min);
            } else {
                logUsingDefaultBecauseBadValue(subsystem, property, dv, v);
                return dv;
            }
        }

        return dv;
    }

    double getDecimal(Class<?> subsystem, String property, double defaultValue) {
        String v = this.source.get(subsystem, property, true);

        if (v == null) {
            logUsingDefaultBecauseUnset(subsystem, property, defaultValue, true);
            return defaultValue;
        }

        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            logUsingDefaultBecauseBadValue(subsystem, property, defaultValue, v);
            return defaultValue;
        }
    }

    long getInteger(Class<?> subsystem, String property, long defaultValue) {
        String v = this.source.get(subsystem, property, true);

        if (v == null) {
            logUsingDefaultBecauseUnset(subsystem, property, defaultValue, true);
            return defaultValue;
        }

        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
        }

        if (lastToUpper(v) == 'K') {
            Long res = parseOr(stripSuffix(v, 1));
            if (res == null) {
                logUsingDefaultBecauseBadValue(subsystem, property, defaultValue, v);
                return defaultValue;
            } else {
                return 1024 * res;
            }
        }
        if (lastToUpper(v) == 'M') {
            Long res = parseOr(stripSuffix(v, 1));
            if (res == null) {
                logUsingDefaultBecauseBadValue(subsystem, property, defaultValue, v);
                return defaultValue;
            } else {
                return 1024 * 1024 * res;
            }
        }
        if (lastToUpper(v) == 'G') {
            Long res = parseOr(stripSuffix(v, 1));
            if (res == null) {
                logUsingDefaultBecauseBadValue(subsystem, property, defaultValue, v);
                return defaultValue;
            } else {
                return 1024 * 1024 * 1024 * res;
            }
        }

        logUsingDefaultBecauseBadValue(subsystem, property, defaultValue, v);
        return defaultValue;
    }

    private char lastToUpper(String v) {
        return Character.toUpperCase(v.charAt(v.length() - 1));
    }

    private void logUsingDefaultBecauseUnset(Object subsystem, String property, Object defaultValue,
            boolean verboseLogValue) {

        if (logger.isLoggable(Level.FINE)) {
            if (!verboseLogValue) {
                defaultValue = ConfigurationSource.MASK;
            }

            logger.log(Level.FINE,
                    String.format("Using default value \"%s\" for %s.%s",
                            defaultValue,
                            subsystemToString(subsystem),
                            property));
        }
    }

    private void logUsingDefaultBecauseBadValue(Object subsystem, String property, Object defaultValue,
            String badValue) {

        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE,
                    String.format("Using default value \"%s\" for %s.%s. Cannot parse \"%s\"",
                            defaultValue,
                            subsystemToString(subsystem),
                            property, badValue));
        }
    }

    private String stripSuffix(String v, int suffixLen) {
        return v.substring(0, v.length() - suffixLen);
    }

    private Long parseOr(String v) {
        if (v == null) {
            return null;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException ignore) {
            return null;
        }
    }
}
