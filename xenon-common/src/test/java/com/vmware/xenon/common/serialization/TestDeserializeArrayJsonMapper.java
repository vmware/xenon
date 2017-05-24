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

package com.vmware.xenon.common.serialization;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.common.Utils;

public class TestDeserializeArrayJsonMapper {

    @Test
    public void testMapper() {

        Map<String, Object> data = new HashMap<>();
        data.put("d_int", Obj.of(1));
        data.put("d_str", Obj.of("two"));
        data.put("d_lst",
                Obj.of(new String[] { "two", "four" }));
        data.put("d_bool", Obj.of(false));
        data.put("cmplx_val", createComplexObj("cmplx_val"));
        data.put("cmplx_lst", new Obj[] {
                createComplexObj("cmplx_lst_1"),
                createComplexObj("cmplx_lst_2") });
        Obj cdo = Obj.of(data);

        String json = Utils.toJson(cdo);
        System.out.println("json        : " + json);

        Obj deserialized = Utils.fromJson(json, Obj.class);
        System.out.println("deserialized: " + deserialized);

        Assert.assertNotNull(deserialized);
    }

    private static Obj createComplexObj(String prefix) {
        Map<String, Obj> data = new HashMap<>();
        data.put(prefix + "_bool", Obj.of(true));
        data.put(prefix + "_int", Obj.of(42));
        data.put(prefix + "_decimal", Obj.of(Math.PI));
        data.put(prefix + "_string", Obj.of(prefix));

        return Obj.of(data);
    }

    public static class Obj {
        public Object value;

        public Map<String, Object> data;

        @Override
        public String toString() {
            return String.format("MapperTest[value=%s, data=%s]", this.value, this.data);
        }

        public static Obj of(Object value) {
            Obj obj = new Obj();
            obj.value = value;
            return obj;
        }

        public static Obj of(Map<String, Object> data) {
            Obj obj = new Obj();
            obj.data = data;
            return obj;
        }
    }
}
