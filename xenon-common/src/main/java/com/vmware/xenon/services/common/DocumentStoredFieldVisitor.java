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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import com.vmware.xenon.common.ServiceDocument;

/**
 * Copy of lucene class - the only change is that the doc field is resetable.
 *
 * @see org.apache.lucene.document.DocumentStoredFieldVisitor
 */
final class DocumentStoredFieldVisitor extends StoredFieldVisitor {
    private Set<String> fieldsToAdd;

    public String documentUpdateAction;
    public String documentSelfLink;
    public long documentVersion;
    public long documentUpdateTimeMicros;
    public Long documentExpirationTimeMicros;
    public byte[] binarySerializedState;
    public String jsonSerializedState;
    private Map<String, String> links;

    private int loadedFields;

    public DocumentStoredFieldVisitor() {
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        if (LuceneDocumentIndexService.LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE
                .equals(fieldInfo.name)) {
            this.binarySerializedState = value;
        }
        this.loadedFields++;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
        String stringValue = new String(value, StandardCharsets.UTF_8);

        switch (fieldInfo.name) {
        case ServiceDocument.FIELD_NAME_SELF_LINK:
            this.documentSelfLink = stringValue;
            break;
        case ServiceDocument.FIELD_NAME_UPDATE_ACTION:
            this.documentUpdateAction = stringValue;
            break;
        case LuceneDocumentIndexService.LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE:
            this.jsonSerializedState = stringValue;
            break;
        default:
            if (this.links == null) {
                this.links = new HashMap<>();
            }
            this.links.put(fieldInfo.name, stringValue);
        }
        this.loadedFields++;
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) {

    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
        switch (fieldInfo.name) {
        case ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS:
            this.documentUpdateTimeMicros = value;
            break;
        case ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS:
            this.documentExpirationTimeMicros = value;
            break;
        case ServiceDocument.FIELD_NAME_VERSION:
            this.documentVersion = value;
            break;
        default:
        }
        this.loadedFields++;
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) {
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) {
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (this.loadedFields >= this.fieldsToAdd.size()) {
            return Status.STOP;
        }
        return this.fieldsToAdd.contains(fieldInfo.name) ? Status.YES : Status.NO;
    }

    public void reset(String field) {
        this.fieldsToAdd = Collections.singleton(field);
        this.loadedFields = 0;
    }

    public void reset(Set<String> fields) {
        this.fieldsToAdd = fields;
        this.loadedFields = 0;

        this.documentUpdateAction = null;
        this.documentUpdateTimeMicros = 0;
        this.documentExpirationTimeMicros = null;
        this.documentSelfLink = null;
        this.documentVersion = 0;
        this.binarySerializedState = null;
        this.jsonSerializedState = null;

        if (this.links != null) {
            this.links.clear();
        }
    }

    public String getLink(String linkName) {
        if (this.links == null) {
            return null;
        }

        return this.links.get(linkName);
    }
}