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

class DefaultSource implements ConfigurationSource {

    @Override
    public String get(String subsystem, String name) {
        String key = buildKey(subsystem, name);
        String value = System.getProperty(key);
        if (value == null) {
            key = toSnakeUpperCase(Utils.PROPERTY_NAME_PREFIX + subsystem).replace(".", "");
            key = key + '_' + toSnakeUpperCase(name);
            value = getEnv(key);
            if (value != null) {
                logFoundEnvVar(key, value);
            }
        } else {
            logFoundSystemProperty(key, value);
        }

        if (value == null || value.length() == 0) {
            return null;
        }

        return value;
    }

    private void logFoundSystemProperty(String key, String value) {
        if (XenonConfiguration.logger.isLoggable(Level.FINE)) {
            XenonConfiguration.logger.log(Level.FINE, String.format("Found value for key %s=\"%s\"", key, value));
        }
    }

    private void logFoundEnvVar(String varName, String value) {
        if (XenonConfiguration.logger.isLoggable(Level.FINE)) {
            XenonConfiguration.logger
                    .log(Level.FINE, String.format("Found environment variable varName %s=\"%s\"", varName, value));
        }
    }

    public static String buildKey(String subsystem, String name) {
        return Utils.PROPERTY_NAME_PREFIX + subsystem + "." + name;
    }

    public static String buildKey(Class<?> subsystem, String name) {
        return buildKey(subsystem.getSimpleName(), name);
    }

    public static String toSnakeUpperCase(String key) {
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
}