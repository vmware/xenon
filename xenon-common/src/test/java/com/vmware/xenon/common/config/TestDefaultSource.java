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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestDefaultSource {
    @Test
    public void getSysprop() throws Exception {
        System.setProperty("xenon.TestDefaultSource.key1", "value1");

        ConfigurationSource src = new ConfigurationSource();
        XenonConfiguration cfg = new XenonConfiguration(src);
        assertEquals("value1", cfg.getString(TestDefaultSource.class, "key1", "hello"));
        assertEquals("value1", XenonConfiguration.string(TestDefaultSource.class, "key1", "hello"));
    }

    @Test
    public void testSnake() {
        assertEquals("ABC", ConfigurationSource.toSnakeUpperCase("abc"));
        assertEquals("A_BC", ConfigurationSource.toSnakeUpperCase("aBc"));
        assertEquals("ABC", ConfigurationSource.toSnakeUpperCase("ABC"));
        assertEquals("AR_15", ConfigurationSource.toSnakeUpperCase("Ar_15"));
        assertEquals("TEST_ME", ConfigurationSource.toSnakeUpperCase("test.me"));
        assertEquals("REALLY_TEST_ME", ConfigurationSource.toSnakeUpperCase("ReallyTest.me"));
        assertEquals("REALLY_REALLY_TEST_ME", ConfigurationSource.toSnakeUpperCase("Really-ReallyTest.me"));
        assertEquals("", ConfigurationSource.toSnakeUpperCase(""));
        assertEquals("DOUBLE__UNDERSCORE", ConfigurationSource.toSnakeUpperCase("double__underscore"));
        assertEquals("DOUBLE__DASH", ConfigurationSource.toSnakeUpperCase("double--dash"));
        assertEquals("WILL NOT HANDLE SPACE", ConfigurationSource.toSnakeUpperCase("will not handle space"));
        assertEquals("NEITHER SPECIAL CHARS: !@#", ConfigurationSource.toSnakeUpperCase("neither special chars: !@#"));
    }

    @Test
    public void testEnv() throws Exception {
        ConfigurationSource src = new ConfigurationSource() {
            @Override
            String getEnv(String key) {
                if (key.equals("XENON_TEST_DEFAULT_SOURCE_KEY1")) {
                    return "env-overridden";
                }

                return super.getEnv(key);
            }
        };
        XenonConfiguration cfg = new XenonConfiguration(src);
        assertEquals("env-overridden", cfg.getString(TestDefaultSource.class, "key1", "hello"));
    }

    @Test
    public void testSysPropOverride() throws Exception {
        System.setProperty("xenon.TestDefaultSource.sys", "system-prop");
        ConfigurationSource src = new ConfigurationSource() {
            @Override
            String getEnv(String key) {
                if (key.equals("XENON_TEST_DEFAULT_SOURCE_SYS")) {
                    return "env-overridden";
                }

                return super.getEnv(key);
            }
        };
        XenonConfiguration cfg = new XenonConfiguration(src);
        assertEquals("system-prop", cfg.getString(TestDefaultSource.class, "sys", "default-value"));
    }

    @Test
    public void testAllUppercase() throws Exception {
        ConfigurationSource src = new ConfigurationSource() {
            @Override
            String getEnv(String key) {
                if (key.equals("XENON_TEST_DEFAULT_SOURCE_USE_DNS")) {
                    return "env-overridden";
                }

                return super.getEnv(key);
            }
        };
        XenonConfiguration cfg = new XenonConfiguration(src);
        assertEquals("env-overridden", cfg.getString(TestDefaultSource.class, "useDNS", "hello"));
    }
}