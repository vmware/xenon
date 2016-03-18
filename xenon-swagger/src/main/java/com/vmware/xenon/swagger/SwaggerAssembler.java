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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectWriter;
import io.swagger.models.Info;
import io.swagger.models.Scheme;
import io.swagger.models.Swagger;
import io.swagger.util.Json;
import io.swagger.util.Yaml;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 */
class SwaggerAssembler {
    public static final String CONTENT_TYPE_YML = "yml";
    public static final String CONTENT_TYPE_YAML = "yaml";

    private final Service service;
    private Info info;
    private ServiceDocumentQueryResult documentQueryResult;
    private Swagger swagger;
    private Operation get;
    private ModelRegistry modelRegistry;
    private Set<String> excludedPrefixes;
    private boolean excludeUtilities;

    private SwaggerAssembler(Service service) {
        this.service = service;
        this.modelRegistry = new ModelRegistry();
    }

    public static SwaggerAssembler create(Service service) {
        return new SwaggerAssembler(service);
    }

    public SwaggerAssembler setInfo(Info info) {
        this.info = info;
        return this;
    }

    public SwaggerAssembler setExcludedPrefixes(String... excludedPrefixes) {
        if (excludedPrefixes != null) {
            this.excludedPrefixes = new HashSet<>(Arrays.asList(excludedPrefixes));
        }

        return this;
    }

    public SwaggerAssembler setQueryResult(ServiceDocumentQueryResult documentQueryResult) {
        this.documentQueryResult = documentQueryResult;
        return this;
    }

    public void build(Operation get) {
        this.get = get;
        this.swagger = new Swagger();
        prepareSwagger();

        Stream<Operation> ops = this.documentQueryResult.documentLinks
                .stream()
                .map(link -> {
                    if (this.service.getSelfLink().equals(link)) {
                        // skip self
                        return null;
                    } else if (link.startsWith(ServiceUriPaths.NODE_SELECTOR_PREFIX)) {
                        // skip node selectors
                        return null;
                    } else if (link
                            .startsWith(ServiceUriPaths.CORE + ServiceUriPaths.UI_PATH_SUFFIX)) {
                        // skip UI
                        return null;
                    } else if (link.startsWith(ServiceUriPaths.UI_RESOURCES)) {
                        // skip UI
                        return null;
                    } else {
                        if (this.excludedPrefixes != null) {
                            for (String prefix : this.excludedPrefixes) {
                                if (link.startsWith(prefix)) {
                                    return null;
                                }
                            }
                        }

                        return Operation.createGet(this.service,
                                link + ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE);
                    }
                })
                .filter(obj -> obj != null);

        OperationJoin.create(ops)
                .setCompletion(this::completion)
                .sendWith(this.service);
    }

    private void prepareSwagger() {
        List<String> json = Collections.singletonList(Operation.MEDIA_TYPE_APPLICATION_JSON);
        this.swagger.setConsumes(json);
        this.swagger.setProduces(json);

        if (this.service.getHost().getSecureListener() != null) {
            this.swagger.setSchemes(Collections.singletonList(Scheme.HTTPS));
            URI uri = this.service.getHost().getSecureUri();
            this.swagger.setHost(uri.getHost() + ":" + uri.getPort());
        } else {
            this.swagger.setSchemes(Collections.singletonList(Scheme.HTTP));
            URI uri = this.service.getHost().getPublicUri();
            this.swagger.setHost(uri.getHost() + ":" + uri.getPort());
        }

        this.swagger.setSchemes(new ArrayList<>());

        this.swagger.setInfo(this.info);
        this.swagger.setBasePath(UriUtils.URI_PATH_CHAR);
    }

    private void completion(Map<Long, Operation> ops, Map<Long, Throwable> errors) {
        try {
            for (Map.Entry<Long, Operation> e : ops.entrySet()) {
                // ignore failed ops
                if (errors != null && errors.containsKey(e.getKey())) {
                    continue;
                }

                String uri = UriUtils.getParentPath(e.getValue().getUri().getPath());

                addService(service.getHost().findService(uri));
            }

            XenonReader reader = this.modelRegistry.newReader(this.swagger);
            reader.read();

            ObjectWriter writer;
            String accept = this.get.getRequestHeader(Operation.ACCEPT_HEADER);
            if (accept != null && (accept.contains(CONTENT_TYPE_YML) || accept.contains(
                    CONTENT_TYPE_YAML))) {
                this.get.addResponseHeader(Operation.CONTENT_TYPE_HEADER,
                        Operation.MEDIA_TYPE_TEXT_YAML);
                writer = Yaml.pretty();
            } else {
                this.get.addResponseHeader(Operation.CONTENT_TYPE_HEADER,
                        Operation.MEDIA_TYPE_APPLICATION_JSON);
                writer = Json.pretty();
            }

            this.get.setBody(writer.writeValueAsString(this.swagger));
            this.get.complete();
        } catch (Exception e) {
            this.get.fail(e);
        }
    }

    private void addService(Service s) {
        DescribedBy annot = s.getClass().getAnnotation(DescribedBy.class);

        if (s instanceof FactoryService) {
            if (annot != null) {
                this.modelRegistry.add(s.getSelfLink(), annot.value(), null);
            } else {
                System.out.println("TODO add generic factory descriptor for" + s.getSelfLink());
            }

            try {
                annot = ((FactoryService) s).createServiceInstance().getClass()
                        .getAnnotation(DescribedBy.class);
                String instanceUri = s.getSelfLink() + "/{id}";
                if (annot != null) {
                    this.modelRegistry.add(instanceUri, annot.value(), s.getSelfLink());
                } else {
                    System.out
                            .println("TODO add generic instance descriptor for " + instanceUri);
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        } else if (s instanceof StatelessService) {
            if (annot != null) {
                this.modelRegistry.add(s.getSelfLink(), annot.value(), null);
            } else {
                System.out.println(
                        "TODO add generic stateless descriptor for " + s.getSelfLink());
            }
        }
    }

    public SwaggerAssembler setExcludeUtilities(boolean excludeUtilities) {
        this.excludeUtilities = excludeUtilities;
        return this;
    }
}
