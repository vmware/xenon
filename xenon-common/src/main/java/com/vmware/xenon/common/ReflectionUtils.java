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

package com.vmware.xenon.common;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;

public final class ReflectionUtils {

    private static final ConcurrentHashMap<Class<?>, Map<String, Field>> DECLARED_FIELDS_CACHE = new ConcurrentHashMap<>();

    private ReflectionUtils() {

    }

    public static <T> T instantiate(Class<T> clazz) {
        try {
            Constructor<T> ctor = clazz.getDeclaredConstructor();
            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);
            }
            return ctor.newInstance();
        } catch (Throwable e) {
            Utils.logWarning("Reflection error: %s", Utils.toString(e));
        }
        return null;
    }

    public static Object getPropertyValue(PropertyDescription pd, Object instance) {
        try {
            return pd.accessor.get(instance);
        } catch (Throwable e) {
            Utils.logWarning("Reflection error: %s", Utils.toString(e));
        }
        return null;
    }

    public static void setPropertyValue(PropertyDescription pd, Object instance, Object value) {
        try {
            pd.accessor.set(instance, value);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    /**
     * This method sets or updates the value of a collection or map
     * If a property with this name does not exist, the object passed in is assigned
     * If a property with the same name exists, the input object is merged with the
     * existing input. For collections, the behavior of the merge will depend on the type
     * of collection - for lists, the new values are appended to the existing collection
     * and the new values replace existing entries for a set. If a property is a map
     * it will delete all key/value pairs where the value is null.
     * @param pd
     * @param instance
     * @param value
     * @return {@code true} if the property value was changed
     */
    public static boolean setOrUpdatePropertyValue(PropertyDescription pd, Object instance,
            Object value) {
        try {
            boolean hasValueChanged = false;
            Object currentObj = pd.accessor.get(instance);
            if (currentObj != null) {
                if (currentObj instanceof Collection) {
                    Collection<Object> existingCollection = (Collection<Object>) currentObj;
                    hasValueChanged = existingCollection.addAll((Collection<Object>) value);
                    pd.accessor.set(instance, existingCollection);
                } else if (currentObj instanceof Map) {
                    Map<Object, Object> existingMap = (Map<Object, Object>) currentObj;
                    hasValueChanged = mergeMapField(existingMap, (Map<Object, Object>) value);
                    pd.accessor.set(instance, existingMap);
                } else {
                    throw new RuntimeException("Merge not supported for specified data type");
                }
            } else {
                pd.accessor.set(instance, value);
                hasValueChanged = value != null;
            }
            return hasValueChanged;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A helper method which update maps in a service.
     * It add the new key-values pairs.
     * Update the value for those keys which are already set.
     * If the value in key-value pair is set to null the pair will be deleted.
     *
     * @param sourceMap The map from the service state before it is patched.
     * @param patchMap Map with values which will be patched.
     * @return whether the source map was changed or not
     */
    private static boolean mergeMapField(
            Map<Object,Object> sourceMap, Map<Object,Object> patchMap) {
        if (patchMap == null || patchMap.isEmpty()) {
            return false;
        }
        boolean hasChanged = false;
        for (Entry<Object, Object> e : patchMap.entrySet()) {
            if (e.getValue() == null) {
                hasChanged |= sourceMap.remove(e.getKey()) != null;
            } else {
                Object oldValue = sourceMap.put(e.getKey(), e.getValue());
                hasChanged = hasChanged || !e.getValue().equals(oldValue);
            }
        }
        return hasChanged;
    }

    /**
     * Sets the given collection to the given collection field.
     *
     * If the input collection is not directly assignable to the field collection type, the field is
     * instantiated and collection items added to it.
     * A default collection interface implementation is used if the field type is an interface
     * (e.g. {@link ArrayList} for {@link List}, {@link HashSet} for {@link Set}, etc.).
     *
     * @return {@code true} if the assignment has made changes to the source object
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static boolean setOrInstantiateCollectionField(Object object, Field collectionField,
            Collection<?> inputCollection) {
        if (!Collection.class.isAssignableFrom(collectionField.getType())) {
            throw new IllegalArgumentException(
                    String.format("Field %s is not a collection", collectionField.getName()));
        }

        Class<? extends Collection> fieldType = (Class<? extends Collection>) collectionField
                .getType();
        Class<? extends Collection> inputType = inputCollection.getClass();

        Collection collectionToSet;
        boolean hasChanged = false;
        if (fieldType.isAssignableFrom(inputType)) {
            collectionToSet = inputCollection;
            hasChanged = true;
        } else {
            if (fieldType.isInterface()) {
                collectionToSet = instantiateDefaultCollection(fieldType);
            } else {
                collectionToSet = ReflectionUtils.instantiate(fieldType);
            }
            hasChanged = collectionToSet.addAll(inputCollection);
        }

        try {
            collectionField.set(object, collectionToSet);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return hasChanged;
    }

    /**
     * Instantiates a default collection for the given collection interface
     * (e.g. {@link ArrayList} for {@link List}, {@link HashSet} for {@link Set}, etc.).
     */
    @SuppressWarnings("rawtypes")
    public static Collection instantiateDefaultCollection(
            Class<? extends Collection> collectionInterfaceType) {
        if (List.class.equals(collectionInterfaceType)) {
            return new ArrayList<>();
        }
        if (Set.class.equals(collectionInterfaceType)) {
            return new HashSet<>();
        }
        if (SortedSet.class.equals(collectionInterfaceType)) {
            return new TreeSet<>();
        }
        throw new IllegalArgumentException(
                "Unsupported collection interface: " + collectionInterfaceType.getCanonicalName());
    }

    /**
     * Checks if fieldName is present and accessible in specified type
     */
    public static boolean hasField(Class<?> type, String fieldName) {
        try {
            type.getField(fieldName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static Field getFieldIfExists(Class<?> type, String fieldName) {
        try {
            return type.getField(fieldName);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Retrieve field and make it accessible.
     */
    public static Field getField(Class<?> clazz, String name) {

        Map<String, Field> fieldMap = DECLARED_FIELDS_CACHE.computeIfAbsent(clazz, key ->
                Arrays.stream(key.getDeclaredFields())
                        .collect(toMap(Field::getName, identity()))
                );

        return fieldMap.computeIfPresent(name, (k, field) -> {
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            return field;
        });
    }

}
