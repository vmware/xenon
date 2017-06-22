/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.serialization.KryoSerializers;

import com.vmware.xenon.services.common.QueryTask.QuerySpecification;

/**
 * Internal only helper used by the {@code LuceneDocumentIndexService} to add new documents
 * to the index (each document added represents a specific version of a service state)
 *
 * Each instance of this class is kept in a thread local variable in the index service, to
 * avoid allocations of Document and Field instances for every service state update.
 */
class LuceneIndexDocumentHelper {

    public static final String GROUP_BY_PROPERTY_NAME_SUFFIX = "_groupBySuffix";

    public static final String SORT_PROPERTY_NAME_SUFFIX = "_sort";

    private static final String DISABLE_SORT_FIELD_NAMING_PROPERTY_NAME =
            Utils.PROPERTY_NAME_PREFIX + "LuceneIndexDocumentHelper.DISABLE_SORT_FIELD_NAMING";

    private static boolean DISABLE_SORT_FIELD_NAMING = Boolean.getBoolean(
            DISABLE_SORT_FIELD_NAMING_PROPERTY_NAME);

    private Document doc = new Document();

    public Document getDoc() {
        return this.doc;
    }

    abstract static class LongFieldContext {
        public StoredField storedField;
        public LongPoint longPoint;
        public NumericDocValuesField numericDocField;

        public abstract void initialize();
    }

    abstract static class StringFieldContext {
        public StringField stringField;
        public SortedDocValuesField sortedField;

        public abstract void initialize();
    }

    private final LongFieldContext versionField = new LongFieldContext() {
        @Override
        public void initialize() {
            this.storedField = new StoredField(ServiceDocument.FIELD_NAME_VERSION, 0L);
            this.longPoint = new LongPoint(ServiceDocument.FIELD_NAME_VERSION, 0L);
            this.numericDocField = new NumericDocValuesField(ServiceDocument.FIELD_NAME_VERSION,
                    0L);
        }
    };

    private final LongFieldContext updateTimeField = new LongFieldContext() {
        @Override
        public void initialize() {
            this.storedField = new StoredField(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, 0L);
            this.longPoint = new LongPoint(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, 0L);
            this.numericDocField = new NumericDocValuesField(
                    ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, 0L);
        }
    };

    private final LongFieldContext expirationTimeField = new LongFieldContext() {
        @Override
        public void initialize() {
            this.storedField = new StoredField(ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS,
                    0L);
            this.longPoint = new LongPoint(ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 0L);
            this.numericDocField = new NumericDocValuesField(
                    ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS, 0L);
        }
    };

    private final StringFieldContext selfLinkField = new StringFieldContext() {
        @Override
        public void initialize() {
            this.stringField = new StringField(ServiceDocument.FIELD_NAME_SELF_LINK, "", Store.YES);
            this.sortedField = new SortedDocValuesField(
                    createSortFieldPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK),
                    new BytesRef(" "));
        }
    };

    private final StringFieldContext kindField = new StringFieldContext() {
        @Override
        public void initialize() {
            this.stringField = new StringField(ServiceDocument.FIELD_NAME_KIND, "", Store.NO);
        }
    };

    private final StringFieldContext authPrincipalLinkField = new StringFieldContext() {
        @Override
        public void initialize() {
            this.stringField = new StringField(ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK, "",
                    Store.NO);
        }
    };

    private final StringFieldContext txIdField = new StringFieldContext() {
        @Override
        public void initialize() {
            this.stringField = new StringField(ServiceDocument.FIELD_NAME_TRANSACTION_ID, "",
                    Store.NO);
        }
    };

    private final StringFieldContext updateActionField = new StringFieldContext() {
        @Override
        public void initialize() {
            this.stringField = new StringField(ServiceDocument.FIELD_NAME_UPDATE_ACTION, "",
                    Store.YES);
        }
    };

    private final Map<String, StoredField> storedFields = new HashMap<>();

    private final Map<String, StringField> stringFields = new HashMap<>();

    private final Map<String, StringField> storedStringFields = new HashMap<>();

    private final Map<String, SortedDocValuesField> sortedStringFields = new HashMap<>();

    private final Map<String, LongPoint> longPointFields = new HashMap<>();

    private final Map<String, DoublePoint> doublePointFields = new HashMap<>();

    private Map<String, NumericDocValuesField> numericFields = new HashMap<>();

    public LuceneIndexDocumentHelper() {
        this.selfLinkField.initialize();
        this.kindField.initialize();
        this.authPrincipalLinkField.initialize();
        this.expirationTimeField.initialize();
        this.txIdField.initialize();
        this.updateActionField.initialize();
        this.updateTimeField.initialize();
        this.versionField.initialize();
    }

    void addSelfLinkField(String selfLink) {
        StringFieldContext ctx = this.selfLinkField;
        ctx.stringField.setStringValue(selfLink);
        ctx.sortedField.setBytesValue(new BytesRef(selfLink));
        this.doc.add(ctx.stringField);
        this.doc.add(ctx.sortedField);
    }

    void addKindField(String kind) {
        this.kindField.stringField.setStringValue(kind);
        this.doc.add(this.kindField.stringField);
    }

    void addUpdateActionField(String action) {
        this.updateActionField.stringField.setStringValue(action);
        this.doc.add(this.updateActionField.stringField);
    }

    void addTxIdField(String txId) {
        this.txIdField.stringField.setStringValue(txId);
        this.doc.add(this.txIdField.stringField);
    }

    void addAuthPrincipalLinkField(String authLink) {
        this.authPrincipalLinkField.stringField.setStringValue(authLink);
        this.doc.add(this.authPrincipalLinkField.stringField);
    }

    void addVersionField(long version) {
        updateLongFieldContext(version, this.versionField);
    }

    void addUpdateTimeField(long updateTimeMicros) {
        updateLongFieldContext(updateTimeMicros, this.updateTimeField);
    }

    void addExpirationTimeField(long exp) {
        updateLongFieldContext(exp, this.expirationTimeField);
    }

    private void updateLongFieldContext(long value, LongFieldContext ctx) {
        ctx.storedField.setLongValue(value);
        this.doc.add(ctx.storedField);
        ctx.longPoint.setLongValue(value);
        this.doc.add(ctx.longPoint);
        ctx.numericDocField.setLongValue(value);
        this.doc.add(ctx.numericDocField);
    }

    void addNumericField(String propertyName, long propertyValue,
            boolean isStored, boolean isCollectionItem, boolean sorted) {
        if (isStored) {
            Field field = isCollectionItem ? new StoredField(propertyName, propertyValue)
                    : getAndSetStoredField(propertyName, propertyValue);
            this.doc.add(field);
        }

        // LongPoint adds an index field to the document that allows for efficient search
        // and range queries
        if (isCollectionItem) {
            this.doc.add(new LongPoint(propertyName, propertyValue));
        } else {
            LongPoint lpField = this.longPointFields.computeIfAbsent(propertyName, (k) -> {
                return new LongPoint(propertyName, propertyValue);
            });
            lpField.setLongValue(propertyValue);
            this.doc.add(lpField);
        }

        // NumericDocValues allow for efficient group operations for a property.
        NumericDocValuesField ndField = getAndSetNumericField(propertyName, propertyValue,
                isCollectionItem);
        this.doc.add(ndField);

        if (sorted) {
            // special handling for groupBy queries, docValuesField can not be added twice
            // We suffix the property name with "_group", add a SortedDocValuesField
            Field sdField = getAndSetSortedStoredField(propertyName + GROUP_BY_PROPERTY_NAME_SUFFIX,
                    Long.toString(propertyValue));
            this.doc.add(sdField);
        }
    }

    private void addNumericField(String propertyName, double propertyValue,
            boolean stored, boolean isCollectionItem, boolean sorted) {
        long longPropertyValue = NumericUtils.doubleToSortableLong(propertyValue);

        if (stored) {
            Field field = isCollectionItem ? new StoredField(propertyName, propertyValue)
                    : getAndSetStoredField(propertyName, propertyValue);
            this.doc.add(field);
        }

        // DoublePoint adds an index field to the document that allows for efficient search
        // and range queries
        if (isCollectionItem) {
            this.doc.add(new DoublePoint(propertyName, propertyValue));
        } else {
            DoublePoint dpField = this.doublePointFields.computeIfAbsent(propertyName,
                    (k) -> {
                        return new DoublePoint(propertyName, propertyValue);
                    });
            dpField.setDoubleValue(propertyValue);
            this.doc.add(dpField);
        }

        NumericDocValuesField ndField = getAndSetNumericField(propertyName, longPropertyValue,
                isCollectionItem);
        this.doc.add(ndField);

        if (sorted) {
            // special handling for groupBy queries
            Field sdField = getAndSetSortedStoredField(propertyName + GROUP_BY_PROPERTY_NAME_SUFFIX,
                    Double.toString(propertyValue));
            this.doc.add(sdField);
        }

    }

    public void addBinaryStateFieldToDocument(ServiceDocument s, byte[] serializedDocument,
            ServiceDocumentDescription desc) {
        try {
            int count = 0;
            if (serializedDocument == null) {
                Output o = KryoSerializers.serializeDocumentForIndexing(s,
                        desc.serializedStateSizeLimit);
                count = o.position();
                serializedDocument = o.getBuffer();
            } else {
                count = serializedDocument.length;
            }
            Field bodyField = new StoredField(
                    LuceneDocumentIndexService.LUCENE_FIELD_NAME_BINARY_SERIALIZED_STATE,
                    serializedDocument, 0, count);
            this.doc.add(bodyField);
        } catch (KryoException ke) {
            throw new IllegalArgumentException(
                    "Failure serializing state of service " + s.documentSelfLink
                            + ", possibly due to size limit."
                            + " Service author should override getDocumentTemplate() and adjust"
                            + " ServiceDocumentDescription.serializedStateSizeLimit. Cause: "
                            + ke.toString());
        }
    }

    public void addIndexableFieldsToDocument(Object podo,
            ServiceDocumentDescription sd) {
        for (Entry<String, PropertyDescription> e : sd.propertyDescriptions.entrySet()) {
            String name = e.getKey();
            PropertyDescription pd = e.getValue();
            if (pd.usageOptions != null
                    && pd.usageOptions.contains(PropertyUsageOption.INFRASTRUCTURE)) {
                continue;
            }
            Object v = ReflectionUtils.getPropertyValue(pd, podo);
            addIndexableFieldToDocument(v, pd, name, false, true);
        }
    }

    /**
     * Add single indexable field to the Lucene {@link Document}.
     * This function recurses if the field value is a PODO, map, array, or collection.
     */
    private void addIndexableFieldToDocument(Object podo, PropertyDescription pd,
            String fieldName, boolean isCollectionItem, boolean allowSortedField) {
        Field luceneField = null;
        Field luceneDocValuesField = null;
        Field.Store fsv = Field.Store.NO;
        boolean isSortedField = false;
        boolean expandField = false;
        Object v = podo;
        if (v == null) {
            return;
        }

        EnumSet<PropertyIndexingOption> opts = pd.indexingOptions;

        if (opts != null) {
            if (opts.contains(PropertyIndexingOption.STORE_ONLY)) {
                return;
            }
            if (opts.contains(PropertyIndexingOption.SORT)) {
                isSortedField = true;
            }
            if (opts.contains(PropertyIndexingOption.EXPAND)) {
                expandField = true;
            }
        }

        if (pd.usageOptions != null) {
            if (pd.usageOptions.contains(PropertyUsageOption.LINK)) {
                fsv = Field.Store.YES;
            }
            if (pd.usageOptions.contains(PropertyUsageOption.LINKS)) {
                expandField = true;
            }
        }

        boolean isStored = fsv == Field.Store.YES;
        String stringValue = null;

        if (v instanceof String) {
            stringValue = v.toString();
            if (opts == null) {
                luceneField = getAndSetStringField(fieldName, stringValue, fsv, isCollectionItem);
            } else {
                if (opts.contains(PropertyIndexingOption.CASE_INSENSITIVE)) {
                    stringValue = stringValue.toLowerCase();
                }
                if (opts.contains(PropertyIndexingOption.TEXT)) {
                    luceneField = new TextField(fieldName, stringValue, fsv);
                } else {
                    luceneField = getAndSetStringField(fieldName, stringValue, fsv,
                            isCollectionItem);
                }
            }

        } else if (v instanceof URI) {
            stringValue = QuerySpecification.toMatchValue((URI) v);
            luceneField = getAndSetStringField(fieldName, stringValue, fsv, isCollectionItem);
        } else if (pd.typeName.equals(TypeName.ENUM)) {
            stringValue = QuerySpecification.toMatchValue((Enum<?>) v);
            luceneField = getAndSetStringField(fieldName, stringValue, fsv, isCollectionItem);
        } else if (pd.typeName.equals(TypeName.LONG)) {
            long value = ((Number) v).longValue();
            addNumericField(fieldName, value, isStored, isCollectionItem, isSortedField);
            // Set sorted to false; Appropriate SortedDocValues field is added in addNumericField
            isSortedField = false;
        } else if (pd.typeName.equals(TypeName.DATE)) {
            // Index as microseconds since UNIX epoch
            long value = ((Date) v).getTime() * 1000;
            addNumericField(fieldName, value, isStored, isCollectionItem, false);
            isSortedField = false;
        } else if (pd.typeName.equals(TypeName.DOUBLE)) {
            double value = ((Number) v).doubleValue();
            addNumericField(fieldName, value, isStored, isCollectionItem, isSortedField);
            // Set sorted to false; Appropriate SortedDocValues field is added in addNumericField
            isSortedField = false;
        } else if (pd.typeName.equals(TypeName.BOOLEAN)) {
            stringValue = QuerySpecification.toMatchValue((boolean) v);
            luceneField = getAndSetStringField(fieldName, stringValue, fsv, isCollectionItem);
        } else if (pd.typeName.equals(TypeName.BYTES)) {
            // Don't store bytes in the index
            isSortedField = false;
        } else if (pd.typeName.equals(TypeName.PODO)) {
            // Ignore all complex fields if they are not explicitly marked with EXPAND.
            // We special case all fields of TaskState to ensure task based services have
            // a guaranteed minimum level indexing and query behavior
            if (!(v instanceof TaskState) && !expandField) {
                return;
            }
            addObjectIndexableFieldToDocument(v, pd, fieldName);
            return;
        } else if (expandField && pd.typeName.equals(TypeName.MAP)) {
            addMapIndexableFieldToDocument(v, pd, fieldName);
            return;
        } else if (expandField && (pd.typeName.equals(TypeName.COLLECTION))) {
            addCollectionIndexableFieldToDocument(v, pd, fieldName);
            return;
        } else {
            stringValue = v.toString();
            luceneField = getAndSetStringField(fieldName, stringValue, fsv, isCollectionItem);
        }

        if (isSortedField && allowSortedField) {
            luceneDocValuesField = getAndSetSortedStoredField(
                    createSortFieldPropertyName(fieldName), stringValue);
        }

        if (luceneField != null) {
            this.doc.add(luceneField);
        }

        if (luceneDocValuesField != null) {
            this.doc.add(luceneDocValuesField);
        }
    }

    private void addObjectIndexableFieldToDocument(Object v, PropertyDescription pd,
            String fieldNamePrefix) {
        for (Entry<String, PropertyDescription> e : pd.fieldDescriptions.entrySet()) {
            PropertyDescription fieldDescription = e.getValue();
            Object fieldValue = ReflectionUtils.getPropertyValue(fieldDescription, v);
            if (v == null) {
                continue;
            }
            if (pd.indexingOptions.contains(PropertyIndexingOption.SORT)) {
                fieldDescription.indexingOptions.add(PropertyIndexingOption.SORT);
            }
            String fieldName = QuerySpecification.buildCompositeFieldName(fieldNamePrefix,
                    e.getKey());
            addIndexableFieldToDocument(fieldValue, fieldDescription, fieldName, false, true);
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private void addMapIndexableFieldToDocument(Object v, PropertyDescription pd,
            String fieldNamePrefix) {
        final String errorMsg = "Field not supported. Map keys must be of type String.";

        Map m = (Map) v;
        if (pd.indexingOptions.contains(PropertyIndexingOption.SORT)) {
            pd.elementDescription.indexingOptions.add(PropertyIndexingOption.SORT);
        }

        for (Object o : m.entrySet()) {
            Entry entry = (Entry) o;
            Object mapKey = entry.getKey();
            if (!(mapKey instanceof String)) {
                throw new IllegalArgumentException(errorMsg);
            }

            // There is a risk our field thread local maps grow too much from discrete field names
            // built from map keys. It should be unlikely however: "keys" need to be a set of well known
            // names for queries to be useful. Even 1M discrete field names should be OK on a loaded
            // node
            addIndexableFieldToDocument(entry.getValue(), pd.elementDescription,
                    QuerySpecification.buildCompositeFieldName(fieldNamePrefix, (String) mapKey), false, true);

            if (pd.indexingOptions.contains(PropertyIndexingOption.FIXED_ITEM_NAME)) {
                addIndexableFieldToDocument(entry.getKey(), new PropertyDescription(), fieldNamePrefix, true, true);

                addIndexableFieldToDocument(entry.getValue(), pd.elementDescription,
                        fieldNamePrefix, true, false);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private void addCollectionIndexableFieldToDocument(Object v,
            PropertyDescription pd, String fieldNamePrefix) {
        fieldNamePrefix = QuerySpecification.buildCollectionItemName(fieldNamePrefix);

        Collection c;
        if (v instanceof Collection) {
            c = (Collection) v;
        } else {
            c = Arrays.asList((Object[]) v);
        }
        if (pd.indexingOptions.contains(PropertyIndexingOption.SORT)) {
            pd.elementDescription.indexingOptions.add(PropertyIndexingOption.SORT);
        }
        for (Object cv : c) {
            if (cv == null) {
                continue;
            }

            addIndexableFieldToDocument(cv, pd.elementDescription, fieldNamePrefix, true, true);
        }
    }

    private Field getAndSetSortedStoredField(String name, String value) {
        Field f = this.sortedStringFields.computeIfAbsent(name, (k) -> {
            return new SortedDocValuesField(name, new BytesRef(value));
        });
        f.setBytesValue(new BytesRef(value));
        return f;
    }

    private Field getAndSetStringField(String name, String value, Field.Store fsv,
            boolean isCollectionItem) {
        if (isCollectionItem) {
            return new StringField(name, value, fsv);
        }
        if (fsv == Field.Store.YES) {
            return getAndSetStoredField(name, value);
        } else {
            return getAndSetStringField(name, value);
        }
    }

    private Field getAndSetStringField(String name, String value) {
        Field f = this.stringFields.computeIfAbsent(name, (k) -> {
            return new StringField(name, value, Field.Store.NO);
        });
        f.setStringValue(value);
        return f;
    }

    private Field getAndSetStoredField(String name, String value) {
        Field f = this.storedStringFields.computeIfAbsent(name, (k) -> {
            return new StringField(name, value, Field.Store.YES);
        });
        f.setStringValue(value);
        return f;
    }

    private Field getAndSetStoredField(String name, Long value) {
        Field f = this.storedFields.computeIfAbsent(name, (k) -> {
            return new StoredField(name, value);
        });
        f.setLongValue(value);
        return f;
    }

    private Field getAndSetStoredField(String name, Double value) {
        Field f = this.storedFields.computeIfAbsent(name, (k) -> {
            return new StoredField(name, value);
        });
        f.setDoubleValue(value);
        return f;
    }

    private NumericDocValuesField getAndSetNumericField(String propertyName, long propertyValue,
            boolean isCollectionItem) {
        if (isCollectionItem) {
            return new NumericDocValuesField(propertyName, propertyValue);
        }
        NumericDocValuesField ndField = this.numericFields.computeIfAbsent(propertyName,
                (k) -> {
                    return new NumericDocValuesField(propertyName, propertyValue);
                });
        ndField.setLongValue(propertyValue);
        return ndField;
    }

    static String createSortFieldPropertyName(String propertyName) {
        return DISABLE_SORT_FIELD_NAMING ? propertyName : propertyName + SORT_PROPERTY_NAME_SUFFIX;
    }
}