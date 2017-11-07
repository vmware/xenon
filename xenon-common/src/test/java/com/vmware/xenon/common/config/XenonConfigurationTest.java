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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class XenonConfigurationTest {
    private String foundProperty = "myPropName";
    private String undefined = "undefinedPropName";
    private XenonConfiguration config;

    @Test
    public void decimal() throws Exception {
        prepareConfig("1.5");
        assertEquals(1.5, this.config.getDecimal(XenonConfigurationTest.class, this.foundProperty, 20), 0.001);
        assertEquals(7, this.config.getDecimal(XenonConfigurationTest.class, this.undefined, 7), 0.001);

        prepareConfig("hello");
        assertEquals(7, this.config.getDecimal(XenonConfigurationTest.class, this.foundProperty, 7), 0.001);
    }

    private void prepareConfig(String value) {
        this.config = new XenonConfiguration(new ConfigurationSource() {
            @Override
            public String get(String subsystem, String name, boolean verboseResolution) {
                if (name.equals(XenonConfigurationTest.this.foundProperty) && Objects
                        .equals(subsystem, XenonConfigurationTest.class.getSimpleName())) {
                    return value;
                }
                return null;
            }
        });
    }

    @Test
    public void integer() throws Exception {
        prepareConfig("20");
        assertEquals(20, this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));
        assertEquals(5, this.config.getInteger(XenonConfigurationTest.class, this.undefined, 5));

        prepareConfig("not really an int");
        assertEquals(7, this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, 7));
    }

    @Test
    public void integerSuffix() throws Exception {
        prepareConfig("20K");
        assertEquals(20 * 1024, this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));
        prepareConfig("20k");
        assertEquals(20 * 1024, this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));

        prepareConfig("1M");
        assertEquals(1 * 1024 * 1024, this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));
        prepareConfig("1m");
        assertEquals(1 * 1024 * 1024, this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));

        prepareConfig("1G");
        assertEquals(1 * 1024 * 1024 * 1024,
                this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));
        prepareConfig("1g");
        assertEquals(1 * 1024 * 1024 * 1024,
                this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));

        // suffix but not a number
        prepareConfig("hundredK");
        assertEquals(-1, this.config.getInteger(XenonConfigurationTest.class, this.foundProperty, -1));
    }

    @Test
    public void duration() throws Exception {
        prepareConfig("1000000");
        assertEquals(1000000,
                this.config.getDuration(XenonConfigurationTest.class, this.foundProperty, Duration.ofSeconds(33)));
        assertEquals(2000,
                this.config.getDuration(XenonConfigurationTest.class, this.undefined, Duration.ofMillis(2)));

        prepareConfig("alphanumeric");
        assertEquals(10000,
                this.config.getDuration(XenonConfigurationTest.class, this.foundProperty, Duration.ofMillis(10)));

        // valid suffix, bad value
        prepareConfig("sixtym");
        assertEquals(10000,
                this.config.getDuration(XenonConfigurationTest.class, this.foundProperty, Duration.ofMillis(10)));

        prepareConfig("1h");
        assertEquals(TimeUnit.HOURS.toMicros(1),
                this.config.getDuration(XenonConfigurationTest.class, this.foundProperty, Duration.ofMillis(2)));

        prepareConfig("1m");
        assertEquals(TimeUnit.MINUTES.toMicros(1),
                this.config.getDuration(XenonConfigurationTest.class, this.foundProperty, Duration.ofMillis(2)));

        prepareConfig("1ms");
        assertEquals(TimeUnit.MILLISECONDS.toMicros(1),
                this.config.getDuration(XenonConfigurationTest.class, this.foundProperty, Duration.ofMillis(2)));

        prepareConfig("1d");
        assertEquals(TimeUnit.DAYS.toMicros(1),
                this.config.getDuration(XenonConfigurationTest.class, this.foundProperty, Duration.ofMillis(2)));
    }

    @Test
    public void string() throws Exception {
        prepareConfig("abc");
        assertEquals("abc", this.config.getString(XenonConfigurationTest.class, this.foundProperty, "xyz"));
        assertEquals("hello", this.config.getString(XenonConfigurationTest.class, this.undefined, "hello"));

        // secrets are the same as string sans value logging
        prepareConfig("abc");
        assertEquals("abc", this.config.getSecret(XenonConfigurationTest.class, this.foundProperty, "xyz"));
        assertEquals("hello", this.config.getSecret(XenonConfigurationTest.class, this.undefined, "hello"));
    }

    @Test
    public void bool() throws Exception {
        prepareConfig("true");
        assertEquals(true, this.config.getBool(XenonConfigurationTest.class, this.foundProperty, false));
        assertEquals(false, this.config.getBool(XenonConfigurationTest.class, this.undefined, false));
        assertEquals(true, this.config.getBool(XenonConfigurationTest.class, this.undefined, true));

        prepareConfig("1");
        assertEquals(true, this.config.getBool(XenonConfigurationTest.class, this.foundProperty, false));

        prepareConfig("0");
        assertEquals(false, this.config.getBool(XenonConfigurationTest.class, this.foundProperty, true));
    }

}