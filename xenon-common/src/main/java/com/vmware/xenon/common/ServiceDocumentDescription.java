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

package com.vmware.xenon.common;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.vmware.xenon.common.RequestRouter.Route;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.ServiceDocument.Documentation;
import com.vmware.xenon.common.ServiceDocument.IndexingParameters;
import com.vmware.xenon.common.ServiceDocument.PropertyOptions;
import com.vmware.xenon.common.ServiceDocument.UsageOption;
import com.vmware.xenon.common.ServiceDocument.UsageOptions;

public class ServiceDocumentDescription {

    /**
     * Default limit on the maximum number of document versions per self link retained in the
     * index. This can be configured on a per-service basis with the {@link #versionRetentionLimit}
     * field.
     */
    public static final int DEFAULT_VERSION_RETENTION_LIMIT = 1000;

    /**
     * Default limit on the minimum number of document versions per self link retained in the
     * index. This can be configured on a per-service basis with the {@link #versionRetentionFloor}
     * field.
     */
    public static final int DEFAULT_VERSION_RETENTION_FLOOR = DEFAULT_VERSION_RETENTION_LIMIT / 2;

    public static final long FIELD_VALUE_DISABLED_VERSION_RETENTION = Long.MIN_VALUE;

    /**
     * Upper bound, in bytes, of serialized state. If the a service state version exceeds this,
     * indexing and replication operations will fail. This can be configured per service using the
     * serializedStateSizeLimit field
     */
    public static final int DEFAULT_SERIALIZED_STATE_LIMIT = 4096 * 8;

    public static final String FIELD_NAME_TENANT_LINKS = "tenantLinks";


    public enum TypeName {
        LONG,
        STRING,
        BYTES,
        PODO,
        COLLECTION,
        MAP,
        BOOLEAN,
        DOUBLE,
        InternetAddressV4,
        InternetAddressV6,
        DATE,
        URI,
        ENUM
    }

    public enum PropertyUsageOption {
        /**
         * Property is set once and then becomes immutable
         */
        SINGLE_ASSIGNMENT,

        /**
         * Property is not required
         */
        OPTIONAL,

        /**
         * Property is manipulated by service and is not intended for visualization or client use
         */
        SERVICE_USE,

        /**
         * Infrastructure use only. Set on on all core document fields
         */
        INFRASTRUCTURE,

        /**
         * Property value is replaced by PATCH request in case it is not {@code null} in PATCH document
         */
        AUTO_MERGE_IF_NOT_NULL,

        /**
         * Property is an ID (currently just used for DocumentDescription generation)
         */
        ID,

        /**
         * Property is a link (relative URI path) to another indexed document. Implicitly added
         * for fields which name ends with "{@code Link}"
         */
        LINK,

        /**
         * Property is a collection of links (relative URI paths) to other indexed documents.
         * Implicitly added for fields which name ends with "{@code Links}"
         */
        LINKS,

        /**
         * Property contains sensitive information. Special framework methods should be used so the property value
         * is hidden when serializing to JSON
         */
        SENSITIVE,

        /**
         * Property is required.
         */
        REQUIRED,
    }

    public enum PropertyIndexingOption {
        /**
         * Directs the indexing service to fully index a PODO. all fields will be indexed using the
         * field name as the prefix to each child field. Implicitly added for
         * {@link PropertyUsageOption#LINKS} fields.
         *
         * If the field is a collection of PODOs each item will be fully indexed.
         */
        EXPAND,

        /**
         * Directs the indexing service to ensure the indexing property name will be a fixed
         * value, matching that of the field itself. Applicable for MAP
         */
        FIXED_ITEM_NAME,

        /**
         * Directs the indexing service to store but not index this field
         */
        STORE_ONLY,

        /**
         * Directs the indexing service to index the field contents as text
         */
        TEXT,

        /**
         * Directs the indexing service to index the field so queries converted to lower case,
         * can find values originally in any case. The index will index the field content in lower
         * case, but the original content will be preserved as part of the service document
         */
        CASE_INSENSITIVE,

        /**
         * Directs the indexing service to exclude the field from the content signature calculation.
         */
        EXCLUDE_FROM_SIGNATURE,

        /**
         * Directs the indexing service to add DocValues field to enable sorting.
         */
        SORT
    }

    public static class PropertyDescription {
        public ServiceDocumentDescription.TypeName typeName;
        /**
         * Set only for PODO-typed fields.
         */
        public String kind;
        public Object exampleValue;
        transient Field accessor;

        public EnumSet<PropertyIndexingOption> indexingOptions;
        public EnumSet<PropertyUsageOption> usageOptions;
        public String propertyDocumentation;

        /**
         * Map with descriptions of fields if this property is of the PODO type
         */
        public Map<String, PropertyDescription> fieldDescriptions;

        /**
         * Description of element type if this property is of the ARRAY or COLLECTION type.
         */
        public PropertyDescription elementDescription;

        /**
         * Set only for enums, the set of possible values.
         */
        public String[] enumValues;

        public PropertyDescription() {
            this.indexingOptions = EnumSet.noneOf(PropertyIndexingOption.class);
            this.usageOptions = EnumSet.noneOf(PropertyUsageOption.class);
        }
    }

    public Map<String, ServiceDocumentDescription.PropertyDescription> propertyDescriptions;
    public EnumSet<Service.ServiceOption> serviceCapabilities;
    public Map<Action, List<Route>> serviceRequestRoutes;
    public String userInterfaceResourcePath;

    /**
     * Upper bound on how many state versions to track in the index. Versions that exceed the limit will
     * be permanently deleted.
     */
    public long versionRetentionLimit = DEFAULT_VERSION_RETENTION_LIMIT;

    /**
     * Lower bound on how many state versions should be tracked in the index. Versions above this
     * limit may be deleted by the system at any time.
     */
    public long versionRetentionFloor = DEFAULT_VERSION_RETENTION_FLOOR;

    /**
     * Upper bound, in bytes, of binary serialized state
     */
    public int serializedStateSizeLimit = DEFAULT_SERIALIZED_STATE_LIMIT;

    /**
     * Builder is a parameterized factory for ServiceDocumentDescription instances.
     */
    public static class Builder {

        public static Builder create() {
            return new Builder();
        }

        private Builder() {
        }

        public ServiceDocumentDescription buildDescription(
                Class<? extends ServiceDocument> type) {
            PropertyDescription root = buildPodoPropertyDescription(type, new HashSet<>(), 0);
            ServiceDocumentDescription desc = new ServiceDocumentDescription();

            IndexingParameters indexingParameters = type.getAnnotation(IndexingParameters.class);
            if (indexingParameters != null) {
                desc.serializedStateSizeLimit = indexingParameters.serializedStateSize();
                desc.versionRetentionLimit = indexingParameters.versionRetention();
                desc.versionRetentionFloor = indexingParameters.versionRetentionFloor();
            }

            desc.propertyDescriptions = root.fieldDescriptions;
            return desc;
        }

        public PropertyDescription buildPodoPropertyDescription(Class<?> type) {
            return buildPodoPropertyDescription(type, new HashSet<>(), 0);
        }

        public ServiceDocumentDescription buildDescription(
                Class<? extends ServiceDocument> type,
                EnumSet<Service.ServiceOption> serviceCaps) {
            ServiceDocumentDescription desc = buildDescription(type);
            desc.serviceCapabilities = serviceCaps;
            return desc;
        }

        public ServiceDocumentDescription buildDescription(
                Class<? extends ServiceDocument> type,
                EnumSet<Service.ServiceOption> serviceCaps,
                RequestRouter serviceRequestRouter) {
            ServiceDocumentDescription desc = buildDescription(type, serviceCaps);
            if (serviceRequestRouter != null) {
                desc.serviceRequestRoutes = serviceRequestRouter.getRoutes();
            }
            return desc;
        }

        protected PropertyDescription buildPodoPropertyDescription(
                Class<?> clazz,
                Set<String> visited,
                int depth) {
            PropertyDescription pd = new PropertyDescription();
            pd.fieldDescriptions = new HashMap<>();

            // Prevent infinite recursion if we have already seen this class type
            String typeName = clazz.getTypeName();
            if (visited.contains(typeName)) {
                return pd;
            }

            visited.add(typeName);

            for (Field f : clazz.getFields()) {
                int mods = f.getModifiers();
                if (Modifier.isStatic(mods) || Modifier.isTransient(mods)) {
                    continue;
                }
                if (ServiceDocument.isBuiltInNonIndexedDocumentField(f.getName())) {
                    continue;
                }

                PropertyDescription fd = new PropertyDescription();
                if (ServiceDocument.isBuiltInInfrastructureDocumentField(f.getName())) {
                    fd.usageOptions.add(PropertyUsageOption.INFRASTRUCTURE);
                }
                if (ServiceDocument.isAutoMergeEnabledByDefaultForField(f.getName())) {
                    fd.usageOptions.add(PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL);
                }
                if (ServiceDocument.isBuiltInSignatureExcludedDocumentField(f.getName())) {
                    fd.indexingOptions.add(PropertyIndexingOption.EXCLUDE_FROM_SIGNATURE);
                }
                if (ServiceDocument.isLink(f.getName())) {
                    fd.usageOptions.add(PropertyUsageOption.LINK);
                }
                if (ServiceDocument.isLinks(f.getName())) {
                    fd.usageOptions.add(PropertyUsageOption.LINKS);
                }
                if (f.getName().equals(ServiceDocument.FIELD_NAME_OWNER)) {
                    fd.usageOptions.add(PropertyUsageOption.ID);
                }
                if (fd.usageOptions.contains(PropertyUsageOption.LINKS)) {
                    fd.indexingOptions.add(PropertyIndexingOption.EXPAND);
                }
                buildPropertyDescription(fd, f.getType(), f.getGenericType(), f.getAnnotations(),
                        visited, depth);

                if (ServiceDocument.isBuiltInDocumentFieldWithNullExampleValue(f.getName())) {
                    fd.exampleValue = null;
                }

                fd.accessor = f;
                pd.fieldDescriptions.put(f.getName(), fd);

                if (fd.typeName == TypeName.PODO) {
                    fd.kind = Utils.buildKind(f.getType());
                }
            }

            visited.remove(typeName);

            return pd;
        }

        @SuppressWarnings("rawtypes")
        protected void buildPropertyDescription(
                PropertyDescription pd,
                Class<?> clazz,
                Type typ,
                Annotation[] annotations,
                Set<String> visited,
                int depth) {

            boolean isSimpleType = true;

            if (Boolean.class.equals(clazz) || boolean.class.equals(clazz)) {
                pd.typeName = TypeName.BOOLEAN;
                pd.exampleValue = false;
            } else if (Short.class.equals(clazz) || short.class.equals(clazz)) {
                pd.typeName = TypeName.LONG;
                pd.exampleValue = (short) 0;
            } else if (Integer.class.equals(clazz) || int.class.equals(clazz)) {
                pd.typeName = TypeName.LONG;
                pd.exampleValue = 0;
            } else if (Long.class.equals(clazz) || long.class.equals(clazz)) {
                pd.typeName = TypeName.LONG;
                pd.exampleValue = 0L;
            } else if (Byte.class.equals(clazz) || byte.class.equals(clazz)) {
                pd.typeName = TypeName.LONG;
                pd.exampleValue = (byte) 0;
            } else if (byte[].class.equals(clazz)) {
                pd.typeName = TypeName.BYTES;
            } else if (Double.class.equals(clazz) || double.class.equals(clazz)) {
                pd.typeName = TypeName.DOUBLE;
                pd.exampleValue = 0.0;
            } else if (Float.class.equals(clazz) || float.class.equals(clazz)) {
                pd.typeName = TypeName.DOUBLE;
                pd.exampleValue = 0.0F;
            } else if (Number.class.equals(clazz)) {
                // Special case for undefined Number fields.  Number subclasses will be PODOs.
                pd.typeName = TypeName.DOUBLE;
                pd.exampleValue = 0.0;
            } else if (Character.class.equals(clazz) || char.class.equals(clazz)) {
                pd.typeName = TypeName.STRING;
                pd.exampleValue = 'a';
            } else if (String.class.equals(clazz)) {
                pd.typeName = TypeName.STRING;
                pd.exampleValue = "example string";
            } else if (Date.class.equals(clazz)) {
                pd.exampleValue = new Date();
                pd.typeName = TypeName.DATE;
            } else if (URI.class.equals(clazz)) {
                pd.exampleValue = URI.create("http://localhost:1234/some/service");
                pd.typeName = TypeName.URI;
            } else {
                isSimpleType = false;
            }

            // allow annotations to modify certain attributes
            // we do this after simple types since some UsageOptions can change example values
            if (annotations != null) {
                for (Annotation a : annotations) {
                    if (Documentation.class.equals(a.annotationType())) {
                        Documentation df = (Documentation) a;
                        if (df.description() != null && !df.description().isEmpty()) {
                            pd.propertyDocumentation = df.description();
                        }
                        if (df.exampleString() != null && !df.exampleString().isEmpty()) {
                            pd.exampleValue = df.exampleString();
                        }
                    } else if (UsageOptions.class.equals(a.annotationType())) {
                        UsageOptions usageOptions = (UsageOptions) a;
                        for (UsageOption usageOption : usageOptions.value()) {
                            pd.usageOptions.add(usageOption.option());
                        }
                    } else if (UsageOption.class.equals(a.annotationType())) {
                        // Parse single @UsageOption annotation
                        UsageOption usageOption = (UsageOption) a;
                        pd.usageOptions.add(usageOption.option());
                    } else if (PropertyOptions.class.equals(a.annotationType())) {
                        PropertyOptions po = (PropertyOptions) a;
                        pd.indexingOptions.addAll(Arrays.asList(po.indexing()));
                        pd.usageOptions.addAll(Arrays.asList(po.usage()));
                    }
                }

                // Update based on UsageOptions
                if (pd.usageOptions.contains(PropertyUsageOption.ID)) {
                    pd.exampleValue = UUID.randomUUID().toString();
                    if (pd.propertyDocumentation != null && pd.propertyDocumentation.isEmpty()) {
                        pd.propertyDocumentation = "This must be unique across all instances of all services";
                    }
                }
                if (pd.usageOptions.contains(PropertyUsageOption.LINK)) {
                    pd.exampleValue = "some/service";
                }
            }

            if (!isSimpleType) {
                if (Map.class.isAssignableFrom(clazz)) {
                    pd.typeName = TypeName.MAP;
                    if (depth > 0) {
                        // Force indexing of all nested complex PODO fields. If the service author
                        // instructed expand at the root level, we will index and expand everything
                        pd.indexingOptions.add(PropertyIndexingOption.EXPAND);
                    }

                    // If the map's type is not a parameterized type, then assume the element is a string.
                    // An example where this is the case is {@link java.util.Properties}.
                    if (!(typ instanceof ParameterizedType)) {
                        PropertyDescription fd = new PropertyDescription();
                        buildPropertyDescription(fd, String.class, String.class, null, visited,
                                depth + 1);
                        pd.elementDescription = fd;
                        return;
                    }

                    // Extract the component class from type
                    ParameterizedType parameterizedType = (ParameterizedType) typ;
                    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                    Type componentType = actualTypeArguments[1];
                    Class<?> componentClass;
                    if (componentType instanceof Class<?>) {
                        componentClass = (Class<?>) componentType;
                    } else {
                        ParameterizedType parameterizedComponentType = (ParameterizedType) componentType;
                        componentClass = (Class<?>) parameterizedComponentType.getRawType();
                    }

                    // Recurse into self
                    PropertyDescription fd = new PropertyDescription();
                    buildPropertyDescription(fd, componentClass, componentType, null, visited,
                            depth + 1);
                    pd.elementDescription = fd;
                } else if (Enum.class.isAssignableFrom(clazz)) {
                    pd.typeName = TypeName.ENUM;
                    Object[] enumConstants = clazz.getEnumConstants();
                    if (enumConstants != null) {
                        pd.enumValues = Arrays
                                .stream(enumConstants)
                                .map(o -> ((Enum) o).name())
                                .collect(Collectors.toList())
                                .toArray(new String[0]);
                    }
                } else if (clazz.isArray()) {
                    pd.typeName = TypeName.COLLECTION;

                    // Extract the component class from type
                    Type componentType = clazz.getComponentType();
                    Class<?> componentClass = clazz.getComponentType();

                    // Recurse into self
                    PropertyDescription fd = new PropertyDescription();
                    buildPropertyDescription(fd, componentClass, componentType, null, visited,
                            depth + 1);
                    pd.elementDescription = fd;
                } else if (Collection.class.isAssignableFrom(clazz)) {
                    pd.typeName = TypeName.COLLECTION;
                    if (depth > 0) {
                        // Force indexing of all nested complex PODO fields. If the service author
                        // instructed expand at the root level, we will index and expand everything
                        pd.indexingOptions.add(PropertyIndexingOption.EXPAND);
                    }

                    // Extract the component class from type
                    ParameterizedType parameterizedType = (ParameterizedType) typ;
                    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                    Type componentType = actualTypeArguments[0];
                    Class<?> componentClass;
                    if (componentType instanceof Class<?>) {
                        componentClass = (Class<?>) componentType;
                    } else {
                        ParameterizedType parameterizedComponentType = (ParameterizedType) componentType;
                        componentClass = (Class<?>) parameterizedComponentType.getRawType();
                    }

                    // Recurse into self
                    PropertyDescription fd = new PropertyDescription();
                    buildPropertyDescription(fd, componentClass, componentType, null, visited,
                            depth + 1);
                    pd.elementDescription = fd;
                } else {
                    pd.typeName = TypeName.PODO;
                    pd.kind = Utils.buildKind(clazz);
                    if (depth > 0) {
                        // Force indexing of all nested complex PODO fields. If the service author
                        // instructed expand at the root level, we will index and expand everything
                        pd.indexingOptions.add(PropertyIndexingOption.EXPAND);
                    }

                    // Recurse into object
                    PropertyDescription podo = buildPodoPropertyDescription(clazz, visited,
                            depth + 1);
                    pd.fieldDescriptions = podo.fieldDescriptions;
                }
            }
        }
    }
}
