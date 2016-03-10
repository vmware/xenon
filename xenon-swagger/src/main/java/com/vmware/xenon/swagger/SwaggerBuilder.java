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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.swagger.models.Info;
import io.swagger.models.Swagger;
import io.swagger.util.Json;
import io.swagger.util.Yaml;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;

/**
 */
public class SwaggerBuilder {

    private final Service service;
    private Info info;
    private ServiceDocumentQueryResult services;
    private Swagger swagger;
    private Operation get;

    public SwaggerBuilder(Service requestSender) {
        this.service = requestSender;
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
        swagger.setInfo(info);
        swagger.setBasePath("/");
        swagger.setHost(service.getHost().getPublicUri().toString());

        List<Operation> ops = services.documentLinks
                .stream()
                .map(link -> {
                    if (service.getSelfLink().equals(link)) {
                        // skip self
                        return null;
                    } else if (link.startsWith("/core/node-selectors")) {
                        // skip node selectors
                        return null;
                    } else {
                        return Operation.createGet(service, link + "/template");
                    }
                })
                .filter(obj -> obj != null)
                .collect(Collectors.toList());

        OperationJoin.create(ops)
                .setCompletion(this::completion)
                .sendWith(service);
    }


    private void completion(Map<Long, Operation> ops, Map<Long, Throwable> errors) {
        if (errors != null) {
            // fail fast on first error
            get.fail(errors.values().iterator().next());
            return;
            // List<URI> list = errors.keySet().stream().map(k -> ops.get(k).getUri()).collect(Collectors.toList());
            // System.out.println(list);
        }

        for (Operation op : ops.values()) {
            ServiceDocument doc = op.getBody(ServiceDocument.class);
            ServiceDocumentDescription desc = doc.documentDescription;
            addOperation(op.getUri(), desc);
        }

        ObjectWriter writer;
        String accept = get.getRequestHeader(Operation.ACCEPT_HEADER);
        if (accept != null && (accept.contains("yml") || accept.contains("yaml"))) {
            get.addResponseHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_TEXT_YAML);
            writer = Yaml.pretty();
        } else {
            get.addResponseHeader(Operation.CONTENT_TYPE_HEADER, Operation.MEDIA_TYPE_APPLICATION_JSON);
            writer = Json.pretty();
        }

        try {
            get.setBody(writer.writeValueAsString(swagger));
            get.complete();
        } catch (JsonProcessingException e) {
            get.fail(e);
        }
    }

    private void addOperation(URI uri, ServiceDocumentDescription desc) {
        //TODO
    }
}
