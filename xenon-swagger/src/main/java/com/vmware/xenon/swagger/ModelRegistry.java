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

package com.vmware.xenon.swagger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.swagger.models.Swagger;

/**
 * Aggregates and indexes ServiceDocumentDescription's by their kind.
 */
class ModelRegistry {

    private Map<String, Entry> classes;

    public ModelRegistry() {
        this.classes = new HashMap<>();
    }

    public void add(String uri, Class<?> cls, String parentUri) {
        Entry e = new Entry();
        e.parentUri = parentUri;
        e.uri = uri;
        e.type = cls;
        this.classes.put(uri, e);
    }

    public Collection<Entry> getAll() {
        return classes.values();
    }

    public static class Entry {
        private String parentUri;
        private String uri;
        private Class<?> type;

        public String getParentUri() {
            return parentUri;
        }

        public String getUri() {
            return uri;
        }

        public Class<?> getType() {
            return type;
        }
    }

    public XenonReader newReader(Swagger swagger) {
        return new XenonReader(swagger, this);
    }
}
