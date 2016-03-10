/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import io.swagger.models.Info;
import io.swagger.models.Model;
import io.swagger.models.ModelImpl;
import io.swagger.models.Path;
import io.swagger.models.RefModel;
import io.swagger.models.Response;
import io.swagger.models.Scheme;
import io.swagger.models.Swagger;
import io.swagger.models.Tag;
import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.util.Json;
import io.swagger.util.Yaml;

/**
 */
public class SwaggerBuilder {
    private final Service service;
    private Info info;
    private ServiceDocumentQueryResult services;
    private Swagger swagger;
    private Operation get;
    private ModelRegistry modelRegistry;

    public SwaggerBuilder(Service requestSender) {
        this.service = requestSender;
        this.modelRegistry = new ModelRegistry();
    }

    public SwaggerBuilder info(Info info) {
        this.info = info;
        return this;
    }

    public SwaggerBuilder services(ServiceDocumentQueryResult services) {
        this.services = services;
        return this;
    }

    public void serve(Operation get) {
        this.get = get;
        swagger = new Swagger();
        prepareSwagger();

        Stream<Operation> ops = services.documentLinks
                .stream()
                .map(link -> {
                    if (service.getSelfLink().equals(link)) {
                        // skip self
                        return null;
                    } else if (link.startsWith("/core/node-selectors")) {
                        // skip node selectors
                        return null;
                    } else if (link.startsWith("/core/ui")) {
                        // skip UI
                        return null;
                    } else {
                        return Operation.createGet(service, link + "/template");
                    }
                })
                .filter(obj -> obj != null);

        OperationJoin.create(ops)
                .setCompletion(this::completion)
                .sendWith(service);
    }

    private void prepareSwagger() {
        swagger.setConsumes(Collections.singletonList(Operation.MEDIA_TYPE_APPLICATION_JSON));
        swagger.setProduces(Collections.singletonList(Operation.MEDIA_TYPE_APPLICATION_JSON));

        if (service.getHost().getSecureListener() != null) {
            swagger.setSchemes(Arrays.asList(Scheme.HTTPS));
            swagger.setHost(service.getHost().getSecureUri().toString());
        } else {
            swagger.setSchemes(Arrays.asList(Scheme.HTTP));
            swagger.setHost(service.getHost().getPublicUri().toString());
        }

        swagger.setSchemes(new ArrayList<>());

        swagger.setTags(new ArrayList<>());
        swagger.setInfo(info);
        swagger.setBasePath("/");
    }

    private void completion(Map<Long, Operation> ops, Map<Long, Throwable> errors) {
        for (Map.Entry<Long,Operation>  e : ops.entrySet()) {
            if (errors != null && errors.containsKey(e.getKey())) {
                continue;
            }

            addOperation(UriUtils.getParentPath(e.getValue().getUri().getPath()), e.getValue());
        }

        swagger.setDefinitions(modelRegistry.getDefinitions());

        ObjectWriter writer;
        String accept = get.getRequestHeader(Operation.ACCEPT_HEADER);
        if (accept != null && (accept.contains("yml") || accept.contains("yaml"))) {
            get.addResponseHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_TEXT_YAML);
            writer = Yaml.pretty();
        } else {
            get.addResponseHeader(Operation.CONTENT_TYPE_HEADER,
                    Operation.MEDIA_TYPE_APPLICATION_JSON);
            writer = Json.pretty();
        }

        try {
            get.setBody(writer.writeValueAsString(swagger));
            get.complete();
        } catch (JsonProcessingException e) {
            get.fail(e);
        }
    }

    private void addOperation(String uri, Operation op) {
        System.out.println("adding " + uri);
        Map<String, Path> paths = swagger.getPaths();

        ServiceDocumentQueryResult q = op.getBody(ServiceDocumentQueryResult.class);
        Tag tag = new Tag();
        tag.setName(uri);
        swagger.getTags().add(tag);

        if (q.documents != null) {
            addFactory(uri, Utils.fromJson(q.documents.values().iterator().next(), ServiceDocument.class));
        } else {
            addStateless(uri, q);
        }
    }

    private void addStateless(String uri, ServiceDocument doc) {
        // TODO
    }

    private void addFactory(String uri, ServiceDocument doc) {
        swagger.path(uri, path2Factory(doc));
//        swagger.path(uri + "/{id}", path2Instance(doc));
        swagger.path(uri + "/{id}/stats", path2UtilStats());
//        swagger.path(uri + "/{id}/config", path2UtilConfig());
//        swagger.path(uri + "/{id}/template", path2UtilTemplate());
//        swagger.path(uri + "/{id}/available", path2UtilAvailable());
    }

    private Path path2UtilTemplate() {
        throw new UnsupportedOperationException();
    }

    private Path path2UtilAvailable() {
        throw new UnsupportedOperationException();
    }

    private Path path2UtilConfig() {
        throw new UnsupportedOperationException();
    }

    private Path path2UtilStats() {
        Path path = new Path();
        io.swagger.models.Operation get = new io.swagger.models.Operation();
        get.setResponses(responseMap(
                200, responseOk(desc(ServiceStats.class))
        ));
        path.set(Service.Action.GET.name(), get);

        //TODO add ops for changing stats

        return path;
    }

    private Parameter paramId() {
        PathParameter res = new PathParameter();
        res.setName("id");
        res.setRequired(true);
        res.setType("string");
        return res;
    }

    private Parameter paramBody(ServiceDocument desc) {
        BodyParameter res = new BodyParameter();
        res.setName("body");
        res.setRequired(true);

        res.setSchema(refSchema(desc));

        return res;
    }

    private Response responseOk(ServiceDocument desc) {
        Response res = new Response();
        res.setDescription("OK");
        res.setSchema(refProperty(desc));
        return res;
    }

    private ServiceDocument desc(Class<? extends ServiceDocument> type) {
        ServiceDocumentDescription desc = ServiceDocumentDescription.Builder
                .create()
                .buildDescription(type);

        try {
            ServiceDocument res = type.newInstance();
            res.documentDescription = desc;
            res.documentKind = Utils.buildKind(type);
            return res;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private Model refSchema(ServiceDocument desc) {
        return new RefModel(scheme(desc).getName());
    }

    private ModelImpl scheme(ServiceDocument desc) {
        return modelRegistry.getModel(desc);
    }

    private Property refProperty(ServiceDocument desc) {
        return new RefProperty(scheme(desc).getName());
    }

    private Map<String, Response> responseMap(Object... args) {
        Map<String, Response> res = new HashMap<>();
        for (int i = 0; i < args.length - 1; i += 2) {
            Object code = args[i];
            Object resp = args[i + 1];

            res.put(code.toString(), (Response) resp);
        }
        return res;
    }
    private Path path2Instance(ServiceDocument doc) {
        throw new UnsupportedOperationException();
    }

    private Path path2Factory(ServiceDocument doc) {;
        Path path = new Path();
        path.setPost(opCreateInstance(doc));
        path.setGet(opFactoryGetInstances());
        // TODO find other handler methods
        //TODO extract documentation from annotations
        return path;
    }

    private io.swagger.models.Operation opFactoryGetInstances() {
        io.swagger.models.Operation op = new io.swagger.models.Operation();
        op.setResponses(responseMap(
                200, responseOk(desc(ServiceDocumentQueryResult.class))
        ));

        //TODO handle query params and documentation
        return op;
    }

    private io.swagger.models.Operation opCreateInstance(ServiceDocument doc) {
        io.swagger.models.Operation op = new io.swagger.models.Operation();
        op.setParameters(Collections.singletonList(paramBody(doc)));
        op.setResponses(responseMap(
                200, responseOk(doc)
        ));

        //TODO extract documentation from annotations
        return op;
    }
}
