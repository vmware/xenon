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

package com.vmware.xenon.common.serialization;

import java.lang.reflect.Type;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;

public final class UtcDateTypeAdapter implements JsonSerializer<Date>, JsonDeserializer<Date> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_INSTANT
            .withZone(ZoneId.of("UTC"));

    public static final UtcDateTypeAdapter INSTANCE = new UtcDateTypeAdapter();

    private UtcDateTypeAdapter() {
    }

    @Override
    public synchronized JsonElement serialize(Date date, Type type,
            JsonSerializationContext jsonSerializationContext) {
        String dateFormatAsString = DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
        return new JsonPrimitive(dateFormatAsString);
    }

    @Override
    public synchronized Date deserialize(JsonElement jsonElement, Type type,
            JsonDeserializationContext jsonDeserializationContext) {
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(jsonElement.getAsString(), DATE_FORMAT);
            return Date.from(zdt.toInstant());
        } catch (DateTimeParseException e) {
            throw new JsonSyntaxException(jsonElement.getAsString(), e);
        }
    }
}