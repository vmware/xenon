package com.vmware.xenon.swagger;

import static com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import static com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import io.swagger.models.Model;
import io.swagger.models.ModelImpl;
import io.swagger.models.properties.BinaryProperty;
import io.swagger.models.properties.BooleanProperty;
import io.swagger.models.properties.DateTimeProperty;
import io.swagger.models.properties.DoubleProperty;
import io.swagger.models.properties.LongProperty;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.properties.StringProperty;

/**
 */
public class ModelRegistry {
    private final HashMap<String, Model> byKind;

    public ModelRegistry() {
        this.byKind = new HashMap<>();
    }

    public ModelImpl getModel(ServiceDocument template) {
        String kind = template.documentKind;
        ModelImpl model = (ModelImpl) byKind.get(kind);

        if (model == null) {
            model = load(template.documentDescription);
            model.setName(template.documentKind);
            byKind.put(kind, model);
        }

        return model;
    }

    private ModelImpl load(ServiceDocumentDescription desc) {
        ModelImpl res = new ModelImpl();

        for (Entry<String, PropertyDescription> e : desc.propertyDescriptions.entrySet()) {
            String name = e.getKey();
            PropertyDescription pd = e.getValue();
            if (pd.usageOptions.contains(PropertyUsageOption.INFRASTRUCTURE)) {
                continue;
            }

            switch (pd.typeName) {
            case ARRAY:
                //throw new UnsupportedOperationException("implement me!!!");
                continue;
            case BOOLEAN:
                res.addProperty(name, new BooleanProperty());
            continue;
            case BYTES:
                res.addProperty(name, new BinaryProperty());
                continue;
            case COLLECTION:
                //throw new UnsupportedOperationException("implement me!!!");
                continue;
            case DATE:
                res.addProperty(name, new DateTimeProperty());
                continue;
            case DOUBLE:
                res.addProperty(name, new DoubleProperty());
                break;
            case ENUM:
                res.addProperty(name, new StringProperty());
                continue;
            case InternetAddressV4:
                res.addProperty(name, new StringProperty());
                continue;
            case InternetAddressV6:
                res.addProperty(name, new StringProperty());
                continue;
            case LONG:
                res.addProperty(name, new LongProperty());
                continue;
            case MAP:
               // throw new UnsupportedOperationException("implement me!!!");
                continue;
            case PODO:
                res.addProperty(name, refProperty(pd));
                continue;
            case STRING:
                res.addProperty(name, new StringProperty());
                continue;
            case URI:
                res.addProperty(name, new StringProperty());
                continue;
            }

            return res;
        }

        return res;
    }

    private RefProperty refProperty(PropertyDescription pd) {
        return new RefProperty(pd.kind);
    }

    public Map<String,  Model> getDefinitions() {
        return byKind;
    }
}
