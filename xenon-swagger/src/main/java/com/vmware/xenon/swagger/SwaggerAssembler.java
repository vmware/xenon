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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectWriter;
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
import io.swagger.models.parameters.QueryParameter;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.properties.StringProperty;
import io.swagger.util.Json;
import io.swagger.util.Yaml;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.RequestRouter.Route;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceConfigUpdateRequest;
import com.vmware.xenon.common.ServiceConfiguration;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.Builder;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 */
class SwaggerAssembler {
    public static final String PARAM_NAME_BODY = "body";
    public static final String PARAM_NAME_ID = "id";
    public static final String DESCRIPTION_SUCCESS = "Success";
    public static final String PREFIX_ID = "/{id}";
    public static final String DESCRIPTION_ERROR = "Error";
    public static final String CONTENT_TYPE_YML = "yml";
    public static final String CONTENT_TYPE_YAML = "yaml";
    public static final String AS_SEPARATOR = "_as_";

    private final Service service;
    private Info info;
    private ServiceDocumentQueryResult documentQueryResult;
    private Swagger swagger;
    private Operation get;
    private ModelRegistry modelRegistry;
    private Tag currentTag;
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
                    } else if (link.startsWith(ServiceUriPaths.CORE + ServiceUriPaths.UI_PATH_SUFFIX)) {
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

                        return Operation.createGet(this.service, link + ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE);
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

                // use service base path as tag
                this.currentTag = new Tag();
                this.currentTag.setName(uri);

                addOperation(uri, e.getValue());
            }

            this.swagger.setDefinitions(this.modelRegistry.getDefinitions());

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

    private void addOperation(String uri, Operation op) {
        ServiceDocumentQueryResult q = op.getBody(ServiceDocumentQueryResult.class);

        if (q.documents != null) {
            Object firstDoc = q.documents.values().iterator().next();
            addFactory(uri, Utils.fromJson(firstDoc, ServiceDocument.class));

            this.swagger.addTag(this.currentTag);
        } else if (q.documentDescription != null
                && q.documentDescription.serviceRequestRoutes != null
                && !q.documentDescription.serviceRequestRoutes.isEmpty()) {
            this.swagger
                    .path(uri, pathByRoutes(q.documentDescription.serviceRequestRoutes.values()));

            this.swagger.addTag(this.currentTag);
        }
    }

    private void addFactory(String uri, ServiceDocument doc) {
        this.swagger.path(uri, path2Factory(doc));

        if (!this.excludeUtilities) {
            this.swagger.path(uri + ServiceHost.SERVICE_URI_SUFFIX_STATS, path2UtilStats(null));
            this.swagger.path(uri + ServiceHost.SERVICE_URI_SUFFIX_CONFIG, path2UtilConfig(null));
            this.swagger.path(uri + ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS,
                    path2UtilSubscriptions(null));
            this.swagger
                    .path(uri + ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE, path2UtilTemplate(null));
            this.swagger
                    .path(uri + ServiceHost.SERVICE_URI_SUFFIX_AVAILABLE, path2UtilAvailable(null));
        }

        Parameter idParam = paramId();
        this.swagger.path(uri + PREFIX_ID, path2Instance(doc));

        if (!this.excludeUtilities) {
            this.swagger.path(uri + PREFIX_ID + ServiceHost.SERVICE_URI_SUFFIX_STATS,
                    path2UtilStats(idParam));
            this.swagger.path(uri + PREFIX_ID + ServiceHost.SERVICE_URI_SUFFIX_CONFIG,
                    path2UtilConfig(idParam));
            this.swagger.path(uri + PREFIX_ID + ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS,
                    path2UtilSubscriptions(idParam));
            this.swagger.path(uri + PREFIX_ID + ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE,
                    path2UtilTemplate(idParam));
            this.swagger.path(uri + PREFIX_ID + ServiceHost.SERVICE_URI_SUFFIX_AVAILABLE,
                    path2UtilAvailable(idParam));
        }
    }

    private Path path2UtilSubscriptions(Parameter idParam) {
        Path path = new Path();
        if (idParam != null) {
            path.setParameters(Collections.singletonList(paramId()));
        }

        ServiceDocument subscriptionState = template(ServiceSubscriptionState.class);
        path.setGet(opDefault(subscriptionState));

        io.swagger.models.Operation deleteOrPost = new io.swagger.models.Operation();
        deleteOrPost.addParameter(paramBody(template(ServiceSubscriber.class)));
        deleteOrPost.addTag(this.currentTag.getName());
        deleteOrPost.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(subscriptionState)
        ));
        path.setDelete(deleteOrPost);
        path.setPost(deleteOrPost);
        return path;

    }

    private Path path2UtilTemplate(Parameter idParam) {
        Path path = new Path();
        if (idParam != null) {
            path.setParameters(Collections.singletonList(paramId()));
            path.setGet(opDefault(template(ServiceDocument.class)));
        } else {
            // idParam == null -> it is a factory
            path.setGet(opDefault(template(ServiceDocumentQueryResult.class)));
        }

        return path;
    }

    private Path path2UtilAvailable(Parameter idParam) {
        Path path = new Path();
        if (idParam != null) {
            path.setParameters(Collections.singletonList(paramId()));
        }

        io.swagger.models.Operation get = new io.swagger.models.Operation();
        get.addTag(this.currentTag.getName());

        get.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(),
                Operation.STATUS_CODE_UNAVAILABLE, responseNoContent(),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));

        path.setGet(get);


        io.swagger.models.Operation patchOrPut = new io.swagger.models.Operation();
        patchOrPut.addTag(this.currentTag.getName());
        patchOrPut.setParameters(Collections.singletonList(paramBody(ServiceStat.class)));

        patchOrPut.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(template(ServiceStats.class)),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));

        path.put(patchOrPut);
        path.patch(patchOrPut);
        return path;
    }

    private Response responseOk() {
        Response res = new Response();
        res.setDescription(DESCRIPTION_SUCCESS);
        return res;
    }

    private Path path2UtilConfig(Parameter idParam) {
        Path path = new Path();
        if (idParam != null) {
            path.setParameters(Collections.singletonList(paramId()));
        }
        io.swagger.models.Operation op = new io.swagger.models.Operation();
        op.addTag(this.currentTag.getName());

        op.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(template(ServiceConfiguration.class)),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));
        path.setGet(op);

        op = new io.swagger.models.Operation();
        op.addTag(this.currentTag.getName());
        op.setParameters(
                Collections.singletonList(paramBody(ServiceConfigUpdateRequest.class)));
        op.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(template(ServiceConfiguration.class)),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));
        path.setPatch(op);

        return path;
    }

    private Path path2UtilStats(Parameter idParam) {
        Path path = new Path();

        if (idParam != null) {
            path.setParameters(Collections.singletonList(paramId()));
        }

        io.swagger.models.Operation get = new io.swagger.models.Operation();
        get.addTag(this.currentTag.getName());
        get.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(template(ServiceStats.class)),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));
        path.set(Service.Action.GET.name().toLowerCase(), get);


        io.swagger.models.Operation put = new io.swagger.models.Operation();
        put.addTag(this.currentTag.getName());
        put.setParameters(Arrays.asList(
                paramNamedBody(template(ServiceStats.class)),
                paramNamedBody(ServiceStats.ServiceStat.class)
        ));
        put.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(template(ServiceStats.class)),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));
        path.put(put);


        io.swagger.models.Operation post = new io.swagger.models.Operation();
        post.addTag(this.currentTag.getName());
        post.setParameters(Collections.singletonList(paramBody(ServiceStat.class)));

        post.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(template(ServiceStats.class)),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));

        path.post(post);
        path.patch(post);

        return path;
    }

    private Parameter paramNamedBody(ServiceDocument template) {
        BodyParameter res = paramBody(template);
        res.setName(PARAM_NAME_BODY + AS_SEPARATOR +  shortenKind(template.documentKind));
        return res;
    }

    private Parameter paramNamedBody(Class<?> type) {
        BodyParameter res = paramBody(type);
        res.setName(PARAM_NAME_BODY + AS_SEPARATOR + shortenKind(Utils.buildKind(type)));
        return res;
    }

    private String shortenKind(String kind) {
        return kind.substring(kind.lastIndexOf(':') + 1);
    }

    private Parameter paramId() {
        PathParameter res = new PathParameter();
        res.setName(PARAM_NAME_ID);
        res.setRequired(true);
        res.setType(StringProperty.TYPE);
        return res;
    }

    private BodyParameter paramBody(ServiceDocument desc) {
        BodyParameter res = new BodyParameter();
        res.setName(PARAM_NAME_BODY);
        res.setRequired(false);

        res.setSchema(refModel(desc));

        return res;
    }

    private BodyParameter paramBody(Class<?> type) {
        BodyParameter res = new BodyParameter();
        res.setName(PARAM_NAME_BODY);
        res.setRequired(false);
        ModelImpl model = modelForPodo(type);
        res.setSchema(new RefModel(model.getName()));
        return res;
    }

    private ModelImpl modelForPodo(Class<?> type) {
        PropertyDescription pd = Builder.create()
                .buildPodoPropertyDescription(type);
        pd.kind = Utils.buildKind(type);
        return this.modelRegistry.getModel(pd);
    }

    private Response responseOk(ServiceDocument template) {
        Response res = new Response();
        res.setDescription(DESCRIPTION_SUCCESS);
        res.setSchema(refProperty(modelForServiceDocument(template)));
        return res;
    }

    private Response responseOk(Class<?> type) {
        Response res = new Response();
        res.setDescription(DESCRIPTION_SUCCESS);

        if (type == null) {
            return res;
        }

        res.setSchema(refProperty(modelForPodo(type)));
        return res;
    }

    private ServiceDocument template(Class<? extends ServiceDocument> type) {
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

    private Model refModel(ServiceDocument desc) {
        return new RefModel(modelForServiceDocument(desc).getName());
    }

    private ModelImpl modelForServiceDocument(ServiceDocument template) {
        return this.modelRegistry.getModel(template);
    }

    private Property refProperty(ModelImpl model) {
        return new RefProperty(model.getName());
    }

    /**
     * Builds a map with a fluent syntax. args must be of even length.
     * Every even argument must be of type {@link Response}. Every odd arg
     * is coerced to string.
     *
     * @param args
     * @return non-null map
     */
    private Map<String, Response> responseMap(Object... args) {
        Map<String, Response> res = new HashMap<>();
        for (int i = 0; i < args.length - 1; i += 2) {
            Object code = args[i];
            Object resp = args[i + 1];

            res.put(code.toString(), (Response) resp);
        }
        return res;
    }

    private io.swagger.models.Operation opDefault(ServiceDocument doc) {
        io.swagger.models.Operation op = new io.swagger.models.Operation();
        op.addTag(this.currentTag.getName());

        op.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(doc),
                Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
        ));
        return op;
    }

    private Response responseNoContent() {
        Response res = new Response();
        res.setDescription(DESCRIPTION_ERROR);
        return res;
    }

    private Response responseGenericError() {
        Response res = new Response();
        res.setDescription(DESCRIPTION_ERROR);
        res.setSchema(refProperty(modelForPodo(ServiceErrorResponse.class)));
        return res;
    }

    private Path path2Instance(ServiceDocument doc) {
        Path path = new Path();
        path.setParameters(Collections.singletonList(paramId()));
        path.setGet(opDefault(doc));

        if (doc.documentDescription != null
                && doc.documentDescription.serviceRequestRoutes != null
                && !doc.documentDescription.serviceRequestRoutes.isEmpty()) {
            Path pathByRoutes = pathByRoutes(doc.documentDescription.serviceRequestRoutes.values());
            path.setPost(pathByRoutes.getPost());
            path.setPut(pathByRoutes.getPut());
            path.setPatch(pathByRoutes.getPatch());
        } else {
            io.swagger.models.Operation op = new io.swagger.models.Operation();
            op.addTag(this.currentTag.getName());
            op.setParameters(Collections.singletonList(paramBody(ServiceDocument.class)));
            op.setResponses(responseMap(
                    Operation.STATUS_CODE_OK, responseOk(doc),
                    Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
            ));

            // service definition should be introspected to better
            // describe which actions are supported
            path.setPost(op);
            path.setPut(op);
            path.setPatch(op);
        }

        return path;
    }

    private Path pathByRoutes(Collection<List<Route>> serviceRoutes) {
        Path path = new Path();

        for (List<Route> routes : serviceRoutes) {
            for (Route route : routes) {
                // Although Xenon allows us to route same action (POST, PATCH, etc.) to different
                // implementation based on request type Swagger doesn't handle same operation with
                // different data types. For example: PATCH /user with UserProfile PODO and
                // PATCH /user with MemberProfile.
                // Reference: https://github.com/OAI/OpenAPI-Specification/issues/146
                io.swagger.models.Operation op = new io.swagger.models.Operation();
                op.addTag(this.currentTag.getName());
                op.setDescription(route.description);
                if (route.requestType != null) {
                    op.setParameters(Collections.singletonList(paramBody(route.requestType)));
                }

                // Add default response codes
                op.setResponses(responseMap(
                        Operation.STATUS_CODE_OK, responseOk(route.responseType),
                        Operation.STATUS_CODE_NOT_FOUND, responseGenericError()
                ));

                switch (route.action) {
                case POST:
                    path.post(op);
                    break;
                case PUT:
                    path.put(op);
                    break;
                case PATCH:
                    path.patch(op);
                    break;
                case GET:
                    path.get(op);
                    break;
                case DELETE:
                    path.delete(op);
                    break;
                case OPTIONS:
                    path.options(op);
                    break;
                default:
                    throw new IllegalStateException("Unknown route action encounter: " + route.action);
                }
            }
        }
        return path;
    }

    private Path path2Factory(ServiceDocument doc) {
        Path path = new Path();
        path.setPost(opCreateInstance(doc));
        path.setGet(opFactoryGetInstances());
        return path;
    }

    private io.swagger.models.Operation opFactoryGetInstances() {
        io.swagger.models.Operation op = new io.swagger.models.Operation();
        op.addTag(this.currentTag.getName());
        op.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(template(ServiceDocumentQueryResult.class))
        ));

        QueryParameter p = new QueryParameter();
        p.setName(UriUtils.URI_PARAM_ODATA_FILTER);
        p.setType(StringProperty.TYPE);
        op.addParameter(p);

        p = new QueryParameter();
        p.setName(UriUtils.URI_PARAM_ODATA_EXPAND_NO_DOLLAR_SIGN);
        p.setType(StringProperty.TYPE);
        op.addParameter(p);

        return op;
    }

    private io.swagger.models.Operation opCreateInstance(ServiceDocument doc) {
        io.swagger.models.Operation op = new io.swagger.models.Operation();
        op.addTag(this.currentTag.getName());
        op.setParameters(Collections.singletonList(paramBody(doc)));
        op.setResponses(responseMap(
                Operation.STATUS_CODE_OK, responseOk(doc)
        ));

        return op;
    }

    public SwaggerAssembler setExcludeUtilities(boolean excludeUtilities) {
        this.excludeUtilities = excludeUtilities;
        return this;
    }
}
