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

package com.vmware.xenon.common.serialization;

import static com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import static com.vmware.xenon.common.UriUtils.URI_QUERY_PARAM_KV_CHAR_CONST;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import io.netty.util.internal.StringUtil;

import com.vmware.xenon.common.RequestRouter;
import com.vmware.xenon.common.RequestRouter.Route;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.UriUtils;

/**
 * GSON {@link JsonSerializer} for representing a {@link Route}s as a JSON string.
 */
public enum RequestRouteConverter implements JsonSerializer<Route>, JsonDeserializer<Route> {
    INSTANCE;

    public static final Type TYPE = new TypeToken<Route>() {}.getType();
    private static final String BODY_MATCHER_CLASS_NAME_SPLITTER = "#";

    /**
     * Serialize the Route instance. For matcher field, toString() is used for serialization
     * which is stored against the field named 'condition'.
     *
     * For RequestUriMatcher ex: {"action":"PATCH","condition":"?action=doX",
     * "description":"performX"}
     * For RequestBodyMatcher ex: {"action":"PATCH","condition":"com.vmware.xenon.common.RequestBody#kind=X",
     * "description":"perform X"}
     */
    @Override
    public JsonElement serialize(Route src, Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject ob = new JsonObject();
        ob.addProperty("action", src.action.toString());
        if (src.matcher != null) {
            ob.addProperty("condition", src.matcher.toString());
        }
        ob.addProperty("description", src.description);
        if (src.requestType != null) {
            ob.addProperty("requestType", src.requestType.getName());
        }

        if (src.responseType != null) {
            ob.addProperty("responseType", src.responseType.getName());
        }

        return ob;
    }

    /**
     * Deserialize the Route instance. During deserialization, the condition field value is
     * decomposed into Parameter of the Route.
     *
     * For RequestUriMatcher ex: Parameter name is 'action' & ParamDef is Query
     * For RequestBodyMatcher ex: Parameter name is 'kind' & ParamDef is Body
     */
    @Override
    public Route deserialize(JsonElement json, Type type,
            JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        if (!json.isJsonObject()) {
            throw new JsonParseException("The json element is not valid");
        }

        JsonObject jsonObject = json.getAsJsonObject();
        Route route = new Route();

        String action = checkAndGetFromJson(jsonObject, "action");
        route.action = action == null ? null : Action.valueOf(action);
        route.description = checkAndGetFromJson(jsonObject, "description");
        try {
            String requestType = checkAndGetFromJson(jsonObject, "requestType");
            route.requestType = requestType == null ? null : Class.forName(requestType);

            // If null, try to fill in if the condition field is filled in.
            String condition = checkAndGetFromJson(jsonObject, "condition");
            if (!StringUtil.isNullOrEmpty(condition)) {
                // If condition starts with ? then it is RequestUriMatcher, so construct
                // QueryParam, else it will be RequestBodyMatcher, so fill in the
                if (condition.startsWith("?")) {
                    Map<String, String> queryParams = UriUtils.parseUriQueryParams(UriUtils
                            .buildUri(condition));
                    route.parameters = new ArrayList<>(queryParams.size());
                    for (Map.Entry<String, String> paramPair: queryParams.entrySet()) {
                        route.parameters.add(new RequestRouter.Parameter(paramPair.getKey(),
                                StringUtil.EMPTY_STRING, TypeName.STRING.name(), true,
                                paramPair.getValue(), RequestRouter.ParamDef.QUERY));
                    }
                } else {
                    String[] conditionParts = condition.split(BODY_MATCHER_CLASS_NAME_SPLITTER);
                    if (route.requestType == null) {
                        route.requestType = Class.forName(conditionParts[0]);
                    }
                    String[] bodyParts = conditionParts[1].split(String.valueOf(URI_QUERY_PARAM_KV_CHAR_CONST));
                    RequestRouter.Parameter parameter = new RequestRouter.Parameter(bodyParts[0],
                            StringUtil.EMPTY_STRING, TypeName.STRING.name(), true, bodyParts[1],
                            RequestRouter.ParamDef.BODY);
                    route.parameters = Arrays.asList(parameter);
                }
            }

            String responseType = checkAndGetFromJson(jsonObject, "responseType");
            route.responseType = responseType == null ? null : Class.forName(responseType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

        return route;
    }

    private String checkAndGetFromJson(JsonObject jsonObject, String memberName) {
        if (jsonObject.has(memberName)) {
            return jsonObject.get(memberName).getAsString();
        }

        return null;
    }
}
