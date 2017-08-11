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

package com.vmware.xenon.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.xenon.common.RequestRouter.ParamDef;
import com.vmware.xenon.common.RequestRouter.Route.RouteDocumentation;
import com.vmware.xenon.common.RequestRouter.Route.SupportLevel;

/**
 * Helper methods for constructing ServiceDocumentDescriptions
 *
 * Package private - Infrastructure use only
 */
class ServiceDocumentDescriptionHelper {

    private static final Logger logger = Logger.getLogger(ServiceDocumentDescriptionHelper.class.getName());

    /** WeakHashMap &gt;Service/Doc class, Map&gt;Description Key,Description Text&lt;&lt; */
    private static final Map<Class<?>, Map<String,String>> documentationDescriptionCache =
            new WeakHashMap<>();

    /**
     * Lookup table for whether or not a given action should
     * have a default request type (if true) or both request and response type (false) or neither (null)
     */
    private static final Map<String, Boolean> actionDefaultTypeMap;

    static {
        actionDefaultTypeMap = new LinkedHashMap<>();
        actionDefaultTypeMap.put("Get", false);
        actionDefaultTypeMap.put("Post", true);
        actionDefaultTypeMap.put("Put", true);
        actionDefaultTypeMap.put("Patch", true);
        actionDefaultTypeMap.put("Delete", null);
    }

    private ServiceDocumentDescriptionHelper() {
        // do nothing
    }

    /**
     * Look up or create and enrich the RequestRouter for the service with
     * documentation about each route
     */
    public static RequestRouter findAndDocumentRequestRouter(Service s) {
        RequestRouter requestRouter = RequestRouter.findRequestRouter(s.getOperationProcessingChain());
        if (requestRouter == null) {
            requestRouter = new RequestRouter();
        }
        for (Map.Entry<String, Boolean> entry : actionDefaultTypeMap.entrySet()) {
            try {
                String action = entry.getKey();
                String methodName = "handle" + action;
                String actionName = action.toUpperCase(Locale.ENGLISH);

                Method method = s.getClass().getMethod(methodName, Operation.class);

                // look up the route's documentation annotation if present
                RouteDocumentation[] docs = method.getAnnotationsByType(RouteDocumentation.class);

                if (docs.length == 0) {
                    // not annotated - still add  default handler
                    RequestRouter.Route route = new RequestRouter.Route();
                    route.path = "";
                    route.action = Service.Action.valueOf(actionName);
                    route.matcher = new RequestRouter.RequestDefaultMatcher();
                    requestRouter.register(route);
                } else {
                    for (RouteDocumentation doc : docs) {
                        // do not include unsupported routes at all
                        if (SupportLevel.NOT_SUPPORTED == doc.supportLevel()) {
                            continue;
                        }

                        RequestRouter.Route route = new RequestRouter.Route();
                        route.path = doc.path();
                        // if the request / response is not the default (used by stateless) ServiceDocument then enrich
                        if (entry.getValue() != null && !s.getStateType().equals(ServiceDocument.class)) {
                            // Get, Post, Put all generate a document as response
                            route.responseType = s.getStateType();
                            if (entry.getValue().equals(Boolean.TRUE)) {
                                // Post and Put also accept a document as a request parameter
                                route.requestType = s.getStateType();
                            }
                        }

                        // @Deprecated annotation on method overrides support levels higher than DEPRECATED
                        SupportLevel supportLevel = doc.supportLevel();
                        if (SupportLevel.DEPRECATED.compareTo(supportLevel) < 0 &&
                                method.getAnnotation(Deprecated.class) != null) {
                            supportLevel = SupportLevel.DEPRECATED;
                        }
                        route.supportLevel = supportLevel;
                        route.description = lookupDocumentationDescription(s.getClass(), doc.description());
                        route.parameters = new ArrayList<>();

                        for (RouteDocumentation.QueryParam qp : doc.queryParams()) {
                            RequestRouter.Parameter p =
                                    new RequestRouter.Parameter(
                                            qp.name(),
                                            lookupDocumentationDescription(s.getClass(), qp.description()),
                                            qp.type(),
                                            qp.required(),
                                            qp.example().isEmpty() ? null : qp.example(),
                                            ParamDef.QUERY);
                            route.parameters.add(p);
                        }

                        for (RouteDocumentation.PathParam pp : doc.pathParams()) {
                            RequestRouter.Parameter p =
                                    new RequestRouter.Parameter(
                                            pp.name(),
                                            lookupDocumentationDescription(s.getClass(), pp.description()),
                                            pp.type(),
                                            pp.required(),
                                            pp.example().isEmpty() ? null : pp.example(),
                                            ParamDef.PATH);
                            route.parameters.add(p);
                        }

                        for (RouteDocumentation.ApiResponse response : doc.responses()) {
                            RequestRouter.Parameter p =
                                    new RequestRouter.Parameter(
                                            Integer.toString(response.statusCode()),
                                            lookupDocumentationDescription(s.getClass(), response.description()),
                                            response.response().getName(),
                                            false,
                                            null,
                                            ParamDef.RESPONSE);
                            route.parameters.add(p);
                        }

                        for (String mediaType : doc.consumes()) {
                            RequestRouter.Parameter p =
                                    new RequestRouter.Parameter(
                                            mediaType,
                                            null,
                                            null,
                                            false,
                                            null,
                                            ParamDef.CONSUMES);
                            route.parameters.add(p);
                        }

                        for (String mediaType : doc.produces()) {
                            RequestRouter.Parameter p =
                                    new RequestRouter.Parameter(
                                            mediaType,
                                            null,
                                            null,
                                            false,
                                            null,
                                            ParamDef.PRODUCES);
                            route.parameters.add(p);
                        }

                        route.action = Service.Action.valueOf(actionName);
                        route.matcher = new RequestRouter.RequestDefaultMatcher();
                        requestRouter.register(route);
                    }
                }
            } catch (NoSuchMethodException | SecurityException ex) {
                logger.log(Level.WARNING, "Failure looking up handler method for %s: %s",
                                        new Object[] { entry.getKey(), Utils.toString(ex) });
            }
        }
        return requestRouter;
    }

    /**
     * The description field can optionally be used as a key into an HTML document containing more complete documentation
     * so as to avoid including massive documentation inside the Java sources, and to permit tech-pubs
     * authors to edit an HTML file instead of modifying in-line descriptions inside Java files.
     * <p>
     * If there is no resource file, or if the file does not contain the description, the description is used as-is.
     */
    public static String lookupDocumentationDescription(Class<?> clazz, String description) {

        if (description == null) {
            return null;
        }

        if (!documentationDescriptionCache.containsKey(clazz)) {
            // this document type has not yet been cached
            String resourceName = "/" + clazz.getName().replaceAll("\\.", "/") + ".html";
            InputStream is = clazz.getResourceAsStream(resourceName);
            if (is == null) {
                documentationDescriptionCache.put(clazz, null);
                return description;
            }
            Map<String,String> cache = new HashMap<>();
            // very simple parser - each new description mapping starts on a new line with '<h1>' and the description key,
            // which is the contents between an '<h1>' and an '</h1>' termination.
            // The description body must follow on subsequent lines (anything on the same line as the key is ignored).

            String key = null;
            StringBuilder body = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
                String line;
                int lineNo = 1;

                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("<h1>")) {
                        // look for end
                        int index = line.indexOf("</h1>");
                        if (index < 0) {
                            logger.log(Level.WARNING,
                                    "Unexpected format in document description file: %s at line %d",
                                    new Object[]{resourceName, lineNo});
                        } else {
                            if (key != null) {
                                cache.put(key, body.toString().trim());
                            }
                            key = line.substring(4, index).trim();
                            body = new StringBuilder();
                        }
                    } else {
                        body.append(line).append(" ");
                    }
                    lineNo++;
                }
            } catch (IOException ex) {
                Logger.getLogger(ServiceHost.class.getName()).log(Level.SEVERE, null, ex);
            }
            // and add last key/value pair if there is one
            if (body.length() > 0) {
                cache.put(key, body.toString());
            }

            // now store this in cache
            documentationDescriptionCache.put(clazz, cache);
        }

        Map<String,String> cache = documentationDescriptionCache.get(clazz);
        if (cache == null) {
            // no description file, return as-is
            return description;
        }
        String mappedDesc = cache.get(description);
        if (mappedDesc == null) {
            // no mapping for this description, return previous description
            return description;
        }
        return mappedDesc;
    }
}
