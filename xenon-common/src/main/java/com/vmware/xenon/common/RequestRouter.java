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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
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

    public static class Route {
        public Action action;
        public Predicate<Operation> matcher;
        public Consumer<Operation> handler;
        public String description;
        public Class<?> requestType;
        public Class<?> responseType;

        public Route(Action action, Predicate<Operation> matcher, Consumer<Operation> handler,
                String description) {
            this.action = action;
            this.matcher = matcher;
            this.handler = handler;
            this.description = description;
        }

        public Route() {
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

            Field f;
            try {
                f = typeParameterClass.getField(fieldName);
                //PODO's fields are public, just in case
                f.setAccessible(true);
            } catch (ReflectiveOperationException e) {
                f = null;
            }
            this.field = f;

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
            return String.format("body.%s=%s", this.field != null ? this.field.getName() : "<<bad field>>",
                    this.fieldValue);
        }
    }

    private HashMap<Action, List<Route>> routes;

    public RequestRouter() {
        this.routes = new HashMap<>();
    }

    public void register(Action action, Predicate<Operation> matcher, Consumer<Operation> handler,
            String description) {
        List<Route> actionRoutes = this.routes.get(action);
        if (actionRoutes == null) {
            actionRoutes = new ArrayList<Route>();
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
