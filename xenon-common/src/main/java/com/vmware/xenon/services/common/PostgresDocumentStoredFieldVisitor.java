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

import java.util.HashMap;
import java.util.Map;

import com.vmware.xenon.common.ServiceDocument;

/**
 * Copy of lucene class - the only change is that the doc field is resetable.
 *
 * @see org.apache.lucene.document.DocumentStoredFieldVisitor
 */
final class PostgresDocumentStoredFieldVisitor {
    public String documentUpdateAction;
    public String documentSelfLink;
    public String documentIndexingId;
    public String documentKind;
    public long documentVersion;
    public long documentTombstoneTimeMicros;
    public long documentUpdateTimeMicros;
    public Long documentExpirationTimeMicros;
    public byte[] binarySerializedState;
    public String jsonSerializedState;
    private Map<String, String> links;

    public PostgresDocumentStoredFieldVisitor() {
    }

    public void stringField(String name, String stringValue) {
        switch (name) {
        case ServiceDocument.FIELD_NAME_SELF_LINK:
            this.documentSelfLink = stringValue;
            break;
        case ServiceDocument.FIELD_NAME_KIND:
            this.documentKind = stringValue;
            break;
        case ServiceDocument.FIELD_NAME_UPDATE_ACTION:
            this.documentUpdateAction = stringValue;
            break;
        case LuceneDocumentIndexService.LUCENE_FIELD_NAME_JSON_SERIALIZED_STATE:
            this.jsonSerializedState = stringValue;
            break;
        case LuceneIndexDocumentHelper.FIELD_NAME_INDEXING_ID:
            this.documentIndexingId = stringValue;
            break;
        default:
            if (this.links == null) {
                this.links = new HashMap<>();
            }
            this.links.put(name, stringValue);
        }
    }

    public void longField(String name, long value) {
        switch (name) {
        case ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS:
            this.documentUpdateTimeMicros = value;
            break;
        case ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS:
            this.documentExpirationTimeMicros = value;
            break;
        case ServiceDocument.FIELD_NAME_VERSION:
            this.documentVersion = value;
            break;
        case LuceneIndexDocumentHelper.FIELD_NAME_INDEXING_METADATA_VALUE_TOMBSTONE_TIME:
            this.documentTombstoneTimeMicros = value;
            break;
        default:
        }
    }

    public void reset() {
        this.documentUpdateAction = null;
        this.documentUpdateTimeMicros = 0;
        this.documentExpirationTimeMicros = null;
        this.documentSelfLink = null;
        this.documentIndexingId = null;
        this.documentTombstoneTimeMicros = LuceneIndexDocumentHelper.ACTIVE_DOCUMENT_TOMBSTONE_TIME;
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