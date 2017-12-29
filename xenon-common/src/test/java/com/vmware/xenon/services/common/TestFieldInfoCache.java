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

package com.vmware.xenon.services.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.junit.Test;

/**
 * This tests peeks into lucene internals. It is possible this test can fail after lucene upgrades in the future.
 * This will not necessarily signal a problem but this test needs to be revisited/deleted.
 */
public class TestFieldInfoCache {

    private static final Field fiValues;

    static {
        try {
            fiValues = FieldInfos.class.getDeclaredField("values");
            fiValues.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    public void testDense() throws NoSuchFieldException, IllegalAccessException {
        FieldInfo[] infosArray = makeInfosArray(100);

        FieldInfos orig = new FieldInfos(infosArray);
        Collection<Object> values = extractValues(orig);
        assertSame(Collections.unmodifiableCollection(new TreeMap<String, Object>().values()).getClass(),
                values.getClass());

        FieldInfoCache cache = new FieldInfoCache();
        FieldInfos fixedFieldInfo = cache.dedupFieldInfos(infosArray);
        values = extractValues(fixedFieldInfo);
        assertSame(Collections.unmodifiableCollection(Collections.emptyList()).getClass(), values.getClass());
    }

    @Test
    public void testSparse() throws NoSuchFieldException, IllegalAccessException {
        FieldInfo[] infosArray = new FieldInfo[3];
        infosArray[0] = makeFieldInfo(1);
        infosArray[1] = makeFieldInfo(1000);
        infosArray[2] = makeFieldInfo(5000);

        FieldInfos orig = new FieldInfos(infosArray);
        Collection<Object> values = extractValues(orig);
        assertSame(Collections.unmodifiableCollection(new TreeMap<String, Object>().values()).getClass(),
                values.getClass());

        FieldInfoCache cache = new FieldInfoCache();
        FieldInfos fixedFieldInfo = cache.dedupFieldInfos(infosArray);
        values = extractValues(fixedFieldInfo);
        assertSame(Collections.unmodifiableCollection(new TreeMap<String, Object>().values()).getClass(),
                values.getClass());
    }

    @SuppressWarnings("unchecked")
    private Collection<Object> extractValues(FieldInfos fixedFieldInfo) throws IllegalAccessException {
        return (Collection<Object>) fiValues.get(fixedFieldInfo);
    }

    private FieldInfo[] makeInfosArray(int len) {
        FieldInfo[] infosArray = new FieldInfo[len];
        // create a dense fieldinfos
        for (int i = 0; i < infosArray.length; i++) {
            infosArray[i] = makeFieldInfo(i);
        }
        return infosArray;
    }

    @Test
    public void testEqual() {
        FieldInfo f1 = new FieldInfo("f1",
                1, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, 1, Collections.emptyMap(), 7, 7);

        FieldInfo f2 = new FieldInfo("f1",
                1, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, 1, Collections.emptyMap(), 7, 7);

        assertEquals(FieldInfoCache.hashCode(f1), FieldInfoCache.hashCode(f2));
        assertTrue(FieldInfoCache.equals(f1, f2));
    }

    @Test
    public void testNoEqualDocValue() {
        FieldInfo f1 = new FieldInfo("f1",
                1, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, -1, Collections.emptyMap(), 7, 7);

        FieldInfo f2 = new FieldInfo("f1",
                1, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, 1, Collections.emptyMap(), 7, 7);

        assertNotEquals(FieldInfoCache.hashCode(f1), FieldInfoCache.hashCode(f2));
        assertFalse(FieldInfoCache.equals(f1, f2));
    }

    @Test
    public void testNoEqualNumber() {
        FieldInfo f1 = new FieldInfo("f1",
                1, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, -1, Collections.emptyMap(), 7, 7);

        FieldInfo f2 = new FieldInfo("f1",
                2, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, -1, Collections.emptyMap(), 7, 7);

        assertNotEquals(FieldInfoCache.hashCode(f1), FieldInfoCache.hashCode(f2));
        assertFalse(FieldInfoCache.equals(f1, f2));
    }

    @Test
    public void testNoEqualName() {
        FieldInfo f1 = new FieldInfo("f1",
                1, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, -1, Collections.emptyMap(), 7, 7);

        FieldInfo f2 = new FieldInfo("f2",
                1, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, -1, Collections.emptyMap(), 7, 7);

        assertNotEquals(FieldInfoCache.hashCode(f1), FieldInfoCache.hashCode(f2));
        assertFalse(FieldInfoCache.equals(f1, f2));
    }

    private FieldInfo makeFieldInfo(int number) {
        return new FieldInfo("name" + number,
                number, false, false, true, IndexOptions.NONE,
                DocValuesType.NUMERIC, 1, Collections.emptyMap(), 7, 7);
    }
}
