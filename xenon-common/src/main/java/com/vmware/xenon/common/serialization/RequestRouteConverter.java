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

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import com.vmware.xenon.common.RequestRouter.Route;
import com.vmware.xenon.common.Service.Action;

/**
 * GSON {@link JsonSerializer} for representing a {@link Route}s as a JSON string.
 */
public enum RequestRouteConverter implements JsonSerializer<Route>, JsonDeserializer<Route> {
    INSTANCE;

    public static final Type TYPE = new TypeToken<Route>() {}.getType();

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
