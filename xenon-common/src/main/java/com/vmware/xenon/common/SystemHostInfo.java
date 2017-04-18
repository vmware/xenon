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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Describes the runtime and operating system environment for the service host
 */
public class SystemHostInfo {
    public static final String PROPERTY_NAME_USER_NAME = "user.name";
    public static final String PROPERTY_NAME_OS_NAME = "os.name";
    public static final String PROPERTY_NAME_OS_VERSION = "os.version";
    public static final String PROPERTY_NAME_OS_ARCH = "os.arch";
    public static final String PROPERTY_JAVA_RUNTIME_VERSION = "java.runtime.version";
    public static final String PROPERTY_JAVA_VM_NAME = "java.vm.name";
    public static final String PROPERTY_JAVA_RUNTIME_NAME = "java.runtime.name";
    public Map<String, String> properties = new ConcurrentSkipListMap<>();
    public Map<String, String> environmentVariables = new ConcurrentSkipListMap<>();
    public long availableProcessorCount;
    public long freeMemoryByteCount;
    public long totalMemoryByteCount;
    public long maxMemoryByteCount;
    public List<String> ipAddresses = new ArrayList<>();
    public long freeDiskByteCount;
    public long usableDiskByteCount;
    public long totalDiskByteCount;
    public String osName;
    public OsFamily osFamily;

    public String getOsName() {
        return this.properties.get(SystemHostInfo.PROPERTY_NAME_OS_NAME);
    }

    public static OsFamily determineOsFamily(String osName) {
        osName = osName == null ? "" : osName.toLowerCase(Locale.ENGLISH);
        if (osName.contains("mac")) {
            return OsFamily.MACOS;
        } else if (osName.contains("win")) {
            return OsFamily.WINDOWS;
        } else if (osName.contains("nux")) {
            return OsFamily.LINUX;
        } else {
            return OsFamily.OTHER;
        }
    }

    public enum OsFamily {
        WINDOWS,
        LINUX,
        MACOS,
        OTHER
    }
}