package com.vmware.xenon.swagger;

import java.util.ArrayList;
import java.util.HashMap;

import io.swagger.jaxrs.Reader;
import io.swagger.models.Swagger;
import io.swagger.models.Tag;
import io.swagger.models.parameters.Parameter;

import com.vmware.xenon.swagger.ModelRegistry.Entry;

/**
 */
class XenonReader extends Reader {
    private final ModelRegistry modelRegistry;

    public XenonReader(Swagger swagger, ModelRegistry modelRegistry) {
        super(swagger);
        this.modelRegistry = modelRegistry;
    }

    public void read() {
        for (Entry e : modelRegistry.getAll()) {
            boolean isSubresource = e.getParentUri() != null;
            HashMap<String, Tag> parentTags = new HashMap<>();

            Tag tag = new Tag();
            tag.setName(e.getParentUri());
            parentTags.put(tag.getName(), tag);

            String[] parentConsumes = new String[0];
            String[] parentProduces = new String[0];
            ArrayList<Parameter> parentParams = new ArrayList<>();

            read(e.getType(), e.getParentUri(), "", isSubresource, parentConsumes, parentProduces, parentTags, parentParams);

            read(StatsUtilityDescriptor.class, e.getParentUri(), "", isSubresource, parentConsumes, parentProduces, parentTags, parentParams);
        }
    }
}
