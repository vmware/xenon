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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;

import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.TestUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestKryoSerializers {

    @Test
    public void serializeDocumentForIndexing() {
        ExampleServiceState state = new ExampleServiceState();
        state.documentSelfLink = "selfLink";
        state.documentKind = Utils.buildKind(ExampleServiceState.class);

        Output o = KryoSerializers.serializeDocumentForIndexing(state, 2048);

        ExampleServiceState deser = (ExampleServiceState) KryoSerializers.deserializeDocument(
                o.getBuffer(), 0,
                o.position());
        assertNull(deser.documentSelfLink);
        assertNull(deser.documentKind);
    }

    @Test
    public void serializeDeserializeDocument() throws Throwable {
        ServiceDocumentDescription sdd = TestUtils.buildStateDescription(
                ExampleService.ExampleServiceState.class, null);
        ExampleServiceState st = new ExampleServiceState();
        st.id = UUID.randomUUID().toString();
        st.counter = Utils.getNowMicrosUtc();
        st.documentSelfLink = st.id;
        st.keyValues = new HashMap<>();
        st.keyValues.put(st.id, st.id);
        st.documentKind = Utils.buildKind(ExampleServiceState.class);
        Output o = KryoSerializers.serializeDocument(st, 1024);
        ExampleServiceState deserializedSt = (ExampleServiceState) KryoSerializers
                .deserializeDocument(o.getBuffer(), 0, o.position());
        assertTrue(ServiceDocument.equals(sdd, st, deserializedSt));
    }

    @Test
    public void getBuffer() {
        byte[] existing = KryoSerializers.getBuffer(1024);
        int size = existing.length * 41;
        byte[] b = KryoSerializers.getBuffer(size);
        assertEquals(size, b.length);
        byte[] bSame = KryoSerializers.getBuffer(size);
        assertTrue(b.hashCode() == bSame.hashCode());
        byte[] bLarger = KryoSerializers.getBuffer(size * 2);
        assertTrue(b.hashCode() != bLarger.hashCode());
        byte[] veryLarge = KryoSerializers
                .getBuffer((int) KryoSerializers.THREAD_LOCAL_BUFFER_LIMIT_BYTES * 2);
        // make sure buffer was not cached
        byte[] veryLargeSecond = KryoSerializers
                .getBuffer((int) KryoSerializers.THREAD_LOCAL_BUFFER_LIMIT_BYTES * 2);
        assertTrue(veryLarge.hashCode() != veryLargeSecond.hashCode());
    }

    @Test
    public void testEmptyCollectionSerialization() {
        Object target;

        target = Collections.emptyList();
        assertCollectionEqualAndUsable(target, serAndDeser(target));

        target = Collections.emptySet();
        assertCollectionEqualAndUsable(target, serAndDeser(target));

        target = Collections.emptyMap();
        assertCollectionEqualAndUsable(target, serAndDeser(target));

        target = Collections.emptyNavigableMap();
        assertCollectionEqualAndUsable(target, serAndDeser(target));

        target = Collections.emptyNavigableSet();
        assertCollectionEqualAndUsable(target, serAndDeser(target));

        target = Collections.emptySortedMap();
        assertCollectionEqualAndUsable(target, serAndDeser(target));

        target = Collections.emptySortedSet();
        assertCollectionEqualAndUsable(target, serAndDeser(target));
    }


    @Test
    public void testEmptyCollectionClone() {
        Object target;

        target = Collections.emptyList();
        assertCollectionEqualAndUsable(target, cloneWithKryo(target));

        target = Collections.emptySet();
        assertCollectionEqualAndUsable(target, cloneWithKryo(target));

        target = Collections.emptyMap();
        assertCollectionEqualAndUsable(target, cloneWithKryo(target));

        target = Collections.emptyNavigableMap();
        assertCollectionEqualAndUsable(target, cloneWithKryo(target));

        target = Collections.emptyNavigableSet();
        assertCollectionEqualAndUsable(target, cloneWithKryo(target));

        target = Collections.emptySortedMap();
        assertCollectionEqualAndUsable(target, cloneWithKryo(target));

        target = Collections.emptySortedSet();
        assertCollectionEqualAndUsable(target, cloneWithKryo(target));
    }

    private Object cloneWithKryo(Object target) {
        return KryoSerializers.clone(target);
    }

    @SuppressWarnings("unchecked")
    private void assertCollectionEqualAndUsable(Object orig, Object deserialized) {
        if (orig instanceof Map && deserialized instanceof Map) {
            Map<Object, Object> first = (Map<Object, Object>) orig;
            Map<Object, Object> second = (Map<Object, Object>) deserialized;
            // wrap in another map to exclude check on concrete type
            assertEquals(new HashMap<>(first), new HashMap<>(second));


            assertTypesCompliant(Map.class, first, second);
            assertTypesCompliant(NavigableMap.class, first, second);

            //check deseriliazed map can be written to
            second.put("test", "test");
            return;
        }

        Collection<Object> first = (Collection<Object>) orig;
        Collection<Object> second = (Collection<Object>) deserialized;

        assertTypesCompliant(Set.class, first, second);
        assertTypesCompliant(NavigableSet.class, first, second);
        assertTypesCompliant(List.class, first, second);
        assertArrayEquals(first.toArray(), second.toArray());

        // check deseriliazed collection can be written to
        second.add("test");
    }

    /**
     * if the first is assignable to type, then so should second.
     *
     * @param type
     * @param first
     * @param second
     */
    private void assertTypesCompliant(Class<?> type, Object first, Object second) {
        if (type.isInstance(first)) {
            assertTrue(type.isInstance(second));
        }
    }

    @Test
    public void testSingletonCollectionsSerialization() {
        Object target;

        target = Collections.singletonList("");
        assertEquals(target, serAndDeser(target));

        target = Collections.singleton("");
        assertEquals(target, serAndDeser(target));

        target = Collections.singletonMap("", 1);
        assertEquals(target, serAndDeser(target));
    }

    @Test
    public void testSingletonCollectionsClone() {
        Object target;

        target = Collections.singletonList("");
        assertEquals(target, cloneWithKryo(target));

        target = Collections.singleton("");
        assertEquals(target, cloneWithKryo(target));

        target = Collections.singletonMap("", 1);
        assertEquals(target, cloneWithKryo(target));
    }

    private Object serAndDeser(Object o) {
        return KryoSerializers.deserializeObject(KryoSerializers.serializeObject(o, 1000));
    }
}
