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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestClaims {

    private static final String KEY = "key";
    private static final String VALUE = "value";

    @Test
    public void testEmptyProperties() {
        Claims.Builder builder = new Claims.Builder();

        Claims claims = builder.getResult();
        assertNotNull(claims.getProperties());
        assertEquals(0, claims.getProperties().size());
    }

    @Test
    public void testProperties() {
        Map<String, String> map = new HashMap<>();
        map.put(KEY, VALUE);

        Claims.Builder builder = new Claims.Builder();
        builder.setProperties(map);

        Claims claims = builder.getResult();
        assertNotNull(claims.getProperties());
        assertEquals(1, claims.getProperties().size());
        assertEquals(VALUE, claims.getProperties().get(KEY));
    }
}
