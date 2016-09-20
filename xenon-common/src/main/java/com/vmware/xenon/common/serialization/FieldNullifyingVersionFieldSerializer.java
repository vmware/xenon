/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import java.lang.reflect.Field;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.VersionFieldSerializer;

import com.vmware.xenon.common.ServiceDocument;

public final class FieldNullifyingVersionFieldSerializer<T> extends VersionFieldSerializer<T> {
    private static final String FIELD_TYPE_VERSION = "typeVersion";
    private int superTypeVersion;

    private int indexSelfLink = -1;
    private int indexKind = -1;
    private static final ServiceDocument TEMPLATE = new ServiceDocument();

    public FieldNullifyingVersionFieldSerializer(Kryo kryo, Class<?> type) {
        super(kryo, type);
        ignoreFields();
    }

    public FieldNullifyingVersionFieldSerializer(Kryo kryo, Class<?> type, boolean compatible) {
        super(kryo, type, compatible);
        ignoreFields();
    }

    private void ignoreFields() {
        try {
            Field versionField = VersionFieldSerializer.class.getDeclaredField(FIELD_TYPE_VERSION);
            versionField.setAccessible(true);
            this.superTypeVersion = (int) versionField.get(this);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }

        CachedField<?>[] fields = getFields();
        for (int i = 0, n = fields.length; i < n; i++) {
            Field theField = fields[i].getField();
            if (theField.getDeclaringClass() == ServiceDocument.class && theField.getName()
                    .equals(ServiceDocument.FIELD_NAME_SELF_LINK)) {
                this.indexSelfLink = i;
            } else if (theField.getDeclaringClass() == ServiceDocument.class && theField.getName()
                    .equals(ServiceDocument.FIELD_NAME_KIND)) {
                this.indexKind = i;
            }
        }
    }

    /**
     * Copied from the parent class. Keep it in sync.
     *
     * @param kryo
     * @param _o
     * @param object
     */
    @Override
    public void write(Kryo kryo, Output _o, T object) {
        if (!(_o instanceof OutputWithRoot)) {
            super.write(kryo, _o, object);
            return;
        }

        OutputWithRoot output = (OutputWithRoot) _o;

        CachedField<?>[] fields = getFields();
        // Write type version.
        output.writeVarInt(this.superTypeVersion, true);

        // Write fields.
        for (int i = 0, n = fields.length; i < n; i++) {
            if (output.getRoot() == object && (i == this.indexKind || i == this.indexSelfLink)) {
                // cheat Kryo to serialize the field from a purposely build nullified template
                fields[i].write(output, TEMPLATE);
            } else {
                fields[i].write(output, object);
            }
        }
    }
}
