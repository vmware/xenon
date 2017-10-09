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

package com.vmware.xenon.common.serialization;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 */
public final class SortedKeysMapViewAdapterFactory implements TypeAdapterFactory {

    private static final TypeToken<Map<String, ?>> TYPE = new TypeToken<Map<String, ?>>() {
    };

    // sort entry by the natural order of the key
    @SuppressWarnings("unchecked")
    private static final Comparator<Entry<?, ?>> COMP = (a, b) -> ((Comparable<Object>) a.getKey())
            .compareTo(b.getKey());

    static final class SortedKeysView<K, V> extends AbstractMap<K, V> {
        private final Map<K, V> map;

        SortedKeysView(Map<K, V> map) {
            this.map = map;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            try {
                SortedSet<Entry<K, V>> res = new TreeSet<>(COMP);

                // defensive copies must be done because of DMI_ENTRY_SETS_MAY_REUSE_ENTRY_OBJECTS
                // see http://findbugs.sourceforge.net/bugDescriptions.html for details
                for (Entry<K, V> entry : this.map.entrySet()) {
                    res.add(new AbstractMap.SimpleImmutableEntry<>(entry));
                }
                return res;
            } catch (ClassCastException e) {
                // in case key does not implement Comparable
                return this.map.entrySet();
            }
        }
    }

    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        if (!Map.class.isAssignableFrom(type.getRawType())) {
            return null;
        }

        TypeAdapter<Map<String, ?>> orig = gson.getDelegateAdapter(this, TYPE);

        return new TypeAdapter<T>() {
            @Override
            @SuppressWarnings({ "unchecked", "rawtypes" })
            public void write(JsonWriter out, T value) throws IOException {
                if (value == null) {
                    orig.write(out, null);
                    return;
                }

                Map<String, ?> map = (Map) value;
                if (map.size() <= 1) {
                    orig.write(out, map);
                } else {
                    orig.write(out, new SortedKeysView(map));
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public T read(JsonReader in) throws IOException {
                return (T) orig.read(in);
            }
        };
    }
}
