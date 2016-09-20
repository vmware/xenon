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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.UUID;

import org.junit.Test;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.TestUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestKryoSerializers {

    @Test
    public void serializeObjectForIndexing() {
        ExampleServiceState state = new ExampleServiceState();
        state.documentSelfLink = "selfLink";
        state.documentKind = Utils.buildKind(ExampleServiceState.class);

        byte[] buffer = KryoSerializers.getBuffer(2 * 1024);
        KryoSerializers.serializeObjectForIndexing(state, buffer, 0);

        ExampleServiceState deser = (ExampleServiceState) Utils.fromDocumentBytes(buffer, 0,
                buffer.length);
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
        byte [] buffer = KryoSerializers.getBuffer(4096);
        int lengthWithKind = KryoSerializers.serializeDocument(st, buffer, 0);
        ExampleServiceState deserializedSt = (ExampleServiceState) KryoSerializers.deserializeDocument(buffer, 0, lengthWithKind);
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
}
