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

import java.util.logging.Level;

import com.vmware.xenon.common.Utils;

class ConfigurationSource {

    static final String MASK = "******";

    String get(Class<?> subsystem, String name, boolean verboseResolution) {
        return get(subsystemToString(subsystem), name, verboseResolution);
    }

    String get(String subsystem, String name, boolean verboseResolution) {
        String key = buildKey(subsystem, name);
        String value = getProperty(key);
        if (value == null) {
            key = toSnakeUpperCase(Utils.PROPERTY_NAME_PREFIX + subsystem).replace(".", "");
            key = key + '_' + toSnakeUpperCase(name);
            value = getEnv(key);
            if (value != null) {
                logFoundEnvVar(verboseResolution, key, value);
            }
        } else {
            logFoundSystemProperty(verboseResolution, key, value);
        }

        if (value == null || value.length() == 0) {
            return null;
        }

        return value;
    }

    private void logFoundSystemProperty(boolean verboseResolution, String key, String value) {
        if (XenonConfiguration.logger.isLoggable(Level.FINE)) {
            if (!verboseResolution) {
                value = MASK;
            }
            XenonConfiguration.logger.log(Level.FINE, String.format("Found system property %s=\"%s\"", key, value));
        }
    }

    private void logFoundEnvVar(boolean verboseResolution, String varName, String value) {
        if (XenonConfiguration.logger.isLoggable(Level.FINE)) {
            if (!verboseResolution) {
                value = MASK;
            }

            XenonConfiguration.logger
                    .log(Level.FINE, String.format("Found environment variable %s=\"%s\"", varName, value));
        }
    }

    static String buildKey(String subsystem, String name) {
        return Utils.PROPERTY_NAME_PREFIX + subsystem + "." + name;
    }

    static String buildKey(Class<?> subsystem, String name) {
        return buildKey(subsystem.getSimpleName(), name);
    }

    static String subsystemToString(Object subsystem) {
        return subsystem instanceof Class ? ((Class) subsystem).getSimpleName() : subsystem.toString();
    }

    static String toSnakeUpperCase(String key) {
        key = key.replace('.', '_');
        key = key.replace('-', '_');
        // sorry for that
        key = key.replaceAll("([^\\p{Upper}_])(\\p{Upper}+)", "$1_$2").toUpperCase();
        return key;
    }

    /**
     * overridable for testing purposed
     * @param key
     * @return
     */
    String getEnv(String key) {
        String res = System.getenv(key);
        if ("".equals(res)) {
            return null;
        }
        return res;
    }

    /**
     * overridable for testing purposed
     * @param key
     * @return
     */
    String getProperty(String key) {
        return System.getProperty(key);
    }
}