/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.vmware.xenon.common.Service.Action;

/**
 * A RequestRouter routes operations to handlers by evaluating an incoming operation against a list of matchers, and invoking the handler of the first match.
 *
 * The primary benefit of using a RequestRouter to map incoming requests to handlers is that the API is being modeled in a way that can later be parsed, for example
 * API documentation can be auto-generated. In addition, it encourages separating logic of each logical operation into its own method/code block.
 */
public class RequestRouter implements Predicate<Operation> {

    public static class Parameter {
        public String name;
        public String description;
        public String type;
        public boolean required;
        public String value;
        public ParamDef paramDef;

        public Parameter(String name, String description, String type, boolean required,
                String value, ParamDef paramDef) {
            this.name = name;
            this.description = description;
            this.type = type;
            this.required = required;
            this.value = value;
            this.paramDef = paramDef;
        }
    }

    // Defines where the parameter appears in the given request.
    public enum ParamDef {
        /** Used to document the query parameters understood by this route */
        QUERY("query"),
        /** Used to specify the body type used for this route */
        BODY("body"),
        /** Used to document the media types accepted by this route */
        CONSUMES("consumes"),
        /** Used to document the media types this route can produce */
        PRODUCES("produces"),
        /** Used to document the possible response codes from this route */
        RESPONSE("response"),
        /** Use to document path variables in case of a NAMESPACE_OWNER enabled service. */
        PATH("path");

        String value;

        ParamDef(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }
    }

    public static class Route {
        /**
         * Support level of this route, for documentation purposes.
         * Note that this does not imply the support level of
         * the Service itself - if a Service is visible to the client then it is considered
         * to be supported, but a Service or Routes may be hidden from the client through access controls.
         */
        public enum SupportLevel {
            /** Route is not supported and requests will fail */
            NOT_SUPPORTED,
            /** Route is for internal use only */
            INTERNAL,
            /** Route is exposed to the public but is deprecated and alternatives should be used */
            DEPRECATED,
            /** Route is public */
            PUBLIC
        }

        public Action action;
        public String path;
        public Predicate<Operation> matcher;
        public Consumer<Operation> handler;
        public String description;
        public Class<?> requestType;
        public Class<?> responseType;
        public List<Parameter> parameters;
        public SupportLevel supportLevel;

        public Route(Action action, Predicate<Operation> matcher, Consumer<Operation> handler,
                String description) {
            this.action = action;
            this.matcher = matcher;
            this.handler = handler;
            this.description = description;
        }

        public Route() {
        }

        @Retention(RetentionPolicy.RUNTIME)
        @Target({ElementType.METHOD})
        public @interface RouteDocumentations {
            RouteDocumentation[] value();
        }

        /**
         * Documentation annotations for Route handler methods (doGet, doPost, ...)
         * This annotation is placed on handler methods to document the
         * behavior of the http verb.
         * This annotation can be repeated though this makes sense only in the case
         * of {@link com.vmware.xenon.common.Service.ServiceOption#URI_NAMESPACE_OWNER} enabled
         * services.
         * <p>
         * Note that the 'description' fields can either directly contain a description,
         * or can be used as a key to look up a more complete description in an HTML
         * resource file with the same path as the java class, but with the
         * file terminating in '.html'.
         * <p>
         * The format of the HTML file is that each key must appear on a line on its own
         * surrounded by &gt;h1&lt; and &gt;/h1&lt; tags, and the lines between that key
         * and the next key will be inserted as the description.
         *
         * Example:
         * <pre>
         * {@code
         *
         * @Documentation(description = "@CAR",
         *       queryParams = {
         *          @QueryParam(description = "@TEAPOT",
         *                  example = "false", name = "teapot", required = false, type = "boolean")
         *      },
         *      consumes = { "application/json", "app/json" },
         *      produces = { "application/json", "app/json" },
         *      responses = {
         *          @ApiResponse(statusCode = 200, description = "OK"),
         *          @ApiResponse(statusCode = 404, description = "Not Found"),
         *          @ApiResponse(statusCode = 418, description = "I'm a teapot!")
         *      })
         * @Override
         * public void handlePut(Operation put) {
         * ...
         *
         * Car.html:
         * <h1>@TEAPOT</h1>
         *
         * Test param - if true then do not modify state, and return http status
         * \"I'm a teapot\"
         * <h1>@CAR</h1>
         *
         * Description of a car
         *
         * }
         * </pre>
         */
        @Retention(RetentionPolicy.RUNTIME)
        @Target({ElementType.METHOD})
        @Repeatable(RouteDocumentations.class)
        public @interface RouteDocumentation {

            /** defines API support level - default is APIs are public unless stated otherwise */
            SupportLevel supportLevel() default SupportLevel.PUBLIC;

            String description() default "";

            /** Defines the path segment relative to the selfLink of a namespace
             * owner service.*/
            String path() default "";

            /** defines HTTP status statusCode responses */
            ApiResponse[] responses() default {};

            /** defines optional query parameters */
            QueryParam[] queryParams() default {};

            /** defines optional path parameters */
            PathParam[] pathParams() default {};

            /** List of supported media types, defaults to application/json */
            String[] consumes() default {};

            /** List of supported media types, defaults to application/json */
            String[] produces() default {};

            /**
             * Documentation of HTTP response codes for Route handler methods.
             * This annotation is used as an embedded annotation inside the @Documentation
             * annotation.
             */
            @Target(value = {ElementType.METHOD})
            @Retention(value = RetentionPolicy.RUNTIME)
            @interface ApiResponse {
                int statusCode();

                String description();

                Class<?> response() default Void.class;
            }

            /**
             * Documentation of query parameter support for Route handler methods.
             * This annotation is used as an embedded annotation inside the @Documentation
             * annotation.
             */
            @Target(value = {ElementType.METHOD})
            @Retention(value = RetentionPolicy.RUNTIME)
            @interface QueryParam {
                String name();

                String description() default "";

                String example() default "";

                String type() default "string";

                boolean required() default false;
            }

            /**
             * Documentation of query parameter support for Route handler methods.
             * This annotation is used as an embedded annotation inside the @Documentation
             * annotation.
             */
            @Target(value = {ElementType.METHOD})
            @Retention(value = RetentionPolicy.RUNTIME)
            @interface PathParam {
                String name();

                String description() default "";

                String example() default "";

                String type() default "string";

                boolean required() default true;
            }
        }
    }

    public static class RequestDefaultMatcher implements Predicate<Operation> {

        @Override
        public boolean test(Operation op) {
            return true;
        }

        @Override
        public String toString() {
            return "#";
        }
    }

    public static class RequestUriMatcher implements Predicate<Operation> {

        private Pattern pattern;

        public RequestUriMatcher(String regexp) {
            this.pattern = Pattern.compile(regexp);
        }

        @Override
        public boolean test(Operation op) {
            return op.getUri() != null && op.getUri().getQuery() != null
                    && this.pattern.matcher(op.getUri().getQuery()).matches();
        }

        @Override
        public String toString() {
            return String.format("?%s", this.pattern.pattern());
        }
    }

    public static class RequestBodyMatcher<T> implements Predicate<Operation> {

        private final Class<T> typeParameterClass;
        private final Object fieldValue;
        private final Field field;

        public RequestBodyMatcher(Class<T> typeParameterClass, String fieldName, Object fieldValue) {
            this.typeParameterClass = typeParameterClass;

            this.field = ReflectionUtils.getField(typeParameterClass, fieldName);
            this.fieldValue = fieldValue;
        }

        @Override
        public boolean test(Operation op) {
            if (this.field == null) {
                return false;
            }

            try {
                T body = op.getBody(this.typeParameterClass);
                return body != null && Objects.equals(this.field.get(body), this.fieldValue);
            } catch (IllegalAccessException ex) {
                return false;
            }
        }

        @Override
        public String toString() {
            return String.format("%s#%s=%s", this.typeParameterClass.getName(),
                    this.field != null ? this.field.getName() : "<<bad field>>", this.fieldValue);
        }
    }

    private Map<Action, List<Route>> routes;

    public RequestRouter() {
        this.routes = new LinkedHashMap<>();
    }

    public void register(Route route) {
        Action action = route.action;
        List<Route> actionRoutes = this.routes.get(action);
        if (actionRoutes == null) {
            actionRoutes = new ArrayList<>();
            this.routes.put(action, actionRoutes);
        }
        actionRoutes.add(route);
    }

    public void register(Action action, Predicate<Operation> matcher, Consumer<Operation> handler,
            String description) {
        List<Route> actionRoutes = this.routes.get(action);
        if (actionRoutes == null) {
            actionRoutes = new ArrayList<>();
        }
        actionRoutes.add(new Route(action, matcher, handler, description));
        this.routes.put(action, actionRoutes);
    }

    public boolean test(Operation op) {
        List<Route> actionRoutes = this.routes.get(op.getAction());
        if (actionRoutes != null) {
            for (Route route : actionRoutes) {
                if (route.matcher.test(op)) {
                    route.handler.accept(op);
                    return false;
                }
            }
        }

        // no match found - processing of the request should continue
        return true;
    }

    public Map<Action, List<Route>> getRoutes() {
        return this.routes;
    }

    public static RequestRouter findRequestRouter(OperationProcessingChain opProcessingChain) {
        if (opProcessingChain == null) {
            return null;
        }

        List<Predicate<Operation>> filters = opProcessingChain.getFilters();
        if (filters.isEmpty()) {
            return null;
        }

        // we are assuming as convention that if a RequestRouter exists in the chain, it is
        // the last element as it invokes the service handler by itself and then drops the request
        Predicate<Operation> lastElement = filters.get(filters.size() - 1);
        if (lastElement instanceof RequestRouter) {
            return (RequestRouter) lastElement;
        }

        return null;
    }
}
