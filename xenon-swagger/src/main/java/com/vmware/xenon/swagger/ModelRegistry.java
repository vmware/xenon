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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.swagger.models.Model;
import io.swagger.models.ModelImpl;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.BinaryProperty;
import io.swagger.models.properties.BooleanProperty;
import io.swagger.models.properties.DateTimeProperty;
import io.swagger.models.properties.DoubleProperty;
import io.swagger.models.properties.LongProperty;
import io.swagger.models.properties.MapProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.properties.StringProperty;
import io.swagger.models.properties.StringProperty.Format;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;

/**
 * Aggregates and indexes ServiceDocumentDescription's by their kind.
 */
class ModelRegistry {
    private final HashMap<String, Model> byKind;

    public ModelRegistry() {
        this.byKind = new HashMap<>();
    }

    public ModelImpl getModel(ServiceDocument template) {
        String kind = template.documentKind;
        ModelImpl model = (ModelImpl) this.byKind.get(kind);

        if (model == null) {
            model = load(template.documentDescription.propertyDescriptions.entrySet());
            model.setName(template.documentKind);
            this.byKind.put(kind, model);
        }

        return model;
    }

    public ModelImpl getModel(PropertyDescription desc) {
        ModelImpl model = (ModelImpl) this.byKind.get(desc.kind);

        if (model == null) {
            model = load(desc.fieldDescriptions.entrySet());
            model.setName(desc.kind);
            this.byKind.put(desc.kind, model);
        }

        return model;
    }

    private ModelImpl load(Collection<Entry<String, PropertyDescription>> desc) {
        ModelImpl res = new ModelImpl();

        for (Entry<String, PropertyDescription> e : desc) {
            String name = e.getKey();
            PropertyDescription pd = e.getValue();
            if (pd.usageOptions.contains(PropertyUsageOption.INFRASTRUCTURE)) {
                continue;
            }
            Property property = makeProperty(pd);
            property.description(pd.propertyDocumentation);
            property.setExample(pd.exampleValue);
            res.addProperty(name, property);
        }

        return res;
    }

    private Property makeProperty(PropertyDescription pd) {
        switch (pd.typeName) {
        case BOOLEAN:
            return new BooleanProperty();
        case BYTES:
            return new BinaryProperty();
        case COLLECTION:
            return new ArrayProperty(makeProperty(pd.elementDescription));
        case DATE:
            return new DateTimeProperty();
        case DOUBLE:
            return new DoubleProperty();
        case ENUM:
            StringProperty prop = new StringProperty();
            if (pd.enumValues != null) {
                prop._enum(Arrays.asList(pd.enumValues));
            }
            return prop;
        case InternetAddressV4:
            return new StringProperty();
        case InternetAddressV6:
            return new StringProperty();
        case LONG:
            return new LongProperty();
        case MAP:
            return new MapProperty(new StringProperty());
        case PODO:
            return refProperty(pd);
        case STRING:
            return new StringProperty();
        case URI:
            return new StringProperty(Format.URI);
        default:
            throw new IllegalStateException("unknown type " + pd.typeName);
        }
    }

    private RefProperty refProperty(PropertyDescription pd) {
        ModelImpl model = (ModelImpl) this.byKind.get(pd.kind);
        if (model == null) {
            model = load(pd.fieldDescriptions.entrySet());
            model.setName(pd.kind);
            this.byKind.put(pd.kind, model);
        }

        return new RefProperty(pd.kind);
    }

    public Map<String, Model> getDefinitions() {
        return this.byKind;
    }
}
