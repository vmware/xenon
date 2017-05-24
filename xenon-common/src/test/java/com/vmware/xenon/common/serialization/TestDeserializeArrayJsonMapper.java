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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

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
        Assert.assertNotNull(objCmplx_lst);
        List arr = Utils.fromJson(objCmplx_lst, List.class);
        for (Iterator<Object> iterator = arr.iterator(); iterator.hasNext(); ) {
            Object nextObj = iterator.next();
            this.logger.info("validate: " + nextObj);
            Assert.assertTrue(nextObj instanceof Map);
            Map nextMap = (Map) nextObj;
            Object nextMapDataObj = nextMap.get("data");
            Assert.assertTrue(nextMapDataObj instanceof Map);
            Map nextMapData = (Map) nextMapDataObj;
            Assert.assertTrue("next: " + nextObj,
                    equal(data_lst_1.data, nextMapData)
                            || equal(data_lst_2.data, nextMapData));
        }
    }

    private boolean equal(Map<String, Object> one, Map other) {
        for (Entry<String, Object> entry : one.entrySet()) {
            Object entryValue = other.get(entry.getKey());
            if (entryValue == null) {
                return false;
            }
            Object value = ((Map) entryValue).get("value");
            TestEntity testEntity = (TestEntity) entry.getValue();
            if (!testEntity.value.toString().equals(value)) {
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
