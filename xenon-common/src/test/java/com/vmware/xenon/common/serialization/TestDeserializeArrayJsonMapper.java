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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.common.Utils;

public class TestDeserializeArrayJsonMapper {
    private Logger logger = Logger.getLogger(getClass().getName());
    private static final String EXPECTED_SER =
            "{\"data\":{\"data_lst\":[{\"data\":{\"data_lst_1_string\":{\"value\":\"data_lst_1\"}}},{\"data\":{\"data_lst_2_string\":{\"value\":\"data_lst_2\"}}}]}}";

    @Test
    public void testMapper() {

        Map<String, Object> data = new HashMap<>();
        String keyCmplx_lst = "data_lst";
        TestEntity data_lst_1 = createTestEntityWithData("data_lst_1");
        TestEntity data_lst_2 = createTestEntityWithData("data_lst_2");
        data.put(keyCmplx_lst, new TestEntity[] { data_lst_1, data_lst_2 });
        TestEntity cdo = TestEntity.withData(data);

        String json = Utils.toJson(cdo);
        this.logger.info("json        : " + json);
        Assert.assertEquals(EXPECTED_SER, json);
        TestEntity deserialized = Utils.fromJson(json, TestEntity.class);
        this.logger.info("deserialized: " + deserialized);

        Assert.assertNotNull(deserialized);
        Map<String, Object> dsData = deserialized.data;
        Assert.assertNotNull(dsData);
        Object objCmplx_lst = dsData.get(keyCmplx_lst);
        Assert.assertTrue("actualType:" + objCmplx_lst.getClass().getName(),
                objCmplx_lst instanceof JsonArray);
        JsonArray arr = (JsonArray) objCmplx_lst;
        for (Iterator<JsonElement> iterator = arr.iterator(); iterator.hasNext(); ) {
            JsonElement next = iterator.next();
            this.logger.info("validate: " + next);
            Assert.assertTrue(next instanceof JsonObject);
            JsonObject jsonObject = (JsonObject) next;
            JsonElement jsonElementData = jsonObject.get("data");
            Assert.assertTrue(jsonElementData instanceof JsonObject);
            JsonObject jsonObjectData = (JsonObject) jsonElementData;
            Assert.assertTrue("next: " + next,
                    equal(data_lst_1.data, jsonObjectData)
                            || equal(data_lst_2.data, jsonObjectData));
        }
    }

    private boolean equal(Map<String, Object> map, JsonObject jsonObjectData) {
        for (Entry<String, Object> entry : map.entrySet()) {
            JsonElement jsonElement = jsonObjectData.get(entry.getKey());
            if (jsonElement == null) {
                return false;
            }
            // TestEntity testEntity = Utils.fromJson(jsonElement.getAsString(), TestEntity.class);
            JsonPrimitive value = (JsonPrimitive) ((JsonObject) jsonElement).get("value");
            TestEntity entryValue = (TestEntity) entry.getValue();
            if (!entryValue.value.toString().equals(value.getAsString())) {
                return false;
            }
        }
        return true;
    }

    private static TestEntity createTestEntityWithData(String prefix) {
        Map<String, Object> data = new HashMap<>();
        data.put(prefix + "_string", TestEntity.withValue(prefix));

        return TestEntity.withData(data);
    }

    public static class TestEntity {
        public Object value;

        public Map<String, Object> data;

        @Override
        public String toString() {
            return String.format("TestEntity[value=%s, data=%s]", this.value, this.data);
        }

        public static TestEntity withValue(Object value) {
            TestEntity obj = new TestEntity();
            obj.value = value;
            return obj;
        }

        public static TestEntity withData(Map<String, Object> data) {
            TestEntity obj = new TestEntity();
            obj.data = data;
            return obj;
        }
    }
}
