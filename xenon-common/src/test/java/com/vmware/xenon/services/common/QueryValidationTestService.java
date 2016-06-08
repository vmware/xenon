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

package com.vmware.xenon.services.common;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyIndexingOption;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

/**
 * Test service used to validate document queries
 */
public class QueryValidationTestService extends StatefulService {

    public static class NestedType {
        public String id;
        public long longValue;
        @ServiceDocument.PropertyOptions(usage = PropertyUsageOption.LINK)
        public String link;
    }

    public static class QueryValidationServiceState extends ServiceDocument {
        public static final String FIELD_NAME_IGNORED_STRING_VALUE = "ignoredStringValue";
        public static final String FIELD_NAME_SERVICE_LINK = "serviceLink";
        public static final String FIELD_NAME_SERVICE_LINKS = "serviceLinks";
        public String id;
        @Documentation(description = "a Long value")
        @PropertyOptions(usage = PropertyUsageOption.OPTIONAL)
        public Long longValue;
        public Double doubleValue;
        public Float floatValue;
        public long longPrimitive;
        public double doublePrimitive;
        public float floatPrimitive;
        public short shortPrimitive;
        public byte bytePrimitive;
        public Date dateValue;
        public String stringValue;
        @PropertyOptions(usage = { PropertyUsageOption.OPTIONAL, PropertyUsageOption.LINK })
        public String serviceLink;
        @PropertyOptions(usage = { PropertyUsageOption.OPTIONAL, PropertyUsageOption.LINKS })
        public List<String> serviceLinks;
        public URI referenceValue;
        public Boolean booleanValue;
        public TaskState taskInfo;
        public ExampleServiceState exampleValue;
        public NestedType nestedComplexValue;
        public List<String> listOfStrings;
        public List<NestedType> listOfNestedValues;
        @PropertyOptions(usage = PropertyUsageOption.OPTIONAL)
        public List<ExampleServiceState> listOfExampleValues;
        public String[] arrayOfStrings;
        public String[] ignoredArrayOfStrings;
        public ExampleServiceState[] arrayOfExampleValues;
        public Map<String, String> mapOfStrings;
        public Map<String, Long> mapOfLongs;
        public Map<String, Double> mapOfDoubles;
        public Map<String, Boolean> mapOfBooleans;
        public Map<String, URI> mapOfUris;
        public Map<String, Service.Action> mapOfEnums;
        public Map<String, NestedType> mapOfNestedTypes;
        public Map<String, byte[]> mapOfBytesArrays;
        public List<Map<String, List<NestedType>>> compositeTypeValue;
        public String ignoredStringValue;
        public byte[] binaryContent;
    }

    public QueryValidationTestService() {
        super(QueryValidationServiceState.class);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
    }

    @Override
    public void handlePut(Operation put) {
        QueryValidationServiceState replacementState = put
                .getBody(QueryValidationServiceState.class);

        // PUT replaces entire state, so update the linked state
        setState(put, replacementState);
        put.setBody(null).complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        QueryValidationServiceState body = patch
                .getBody(QueryValidationServiceState.class);
        QueryValidationServiceState currentState = getState(patch);
        currentState.documentExpirationTimeMicros = body.documentExpirationTimeMicros;
        currentState.serviceLink = body.serviceLink;
        patch.setBody(null).complete();
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument d = super.getDocumentTemplate();
        PropertyDescription pdStringValue = d.documentDescription.propertyDescriptions
                .get("stringValue");
        pdStringValue.indexingOptions.add(PropertyIndexingOption.TEXT);

        PropertyDescription pdExample = d.documentDescription.propertyDescriptions
                .get("exampleValue");
        pdExample.indexingOptions.add(PropertyIndexingOption.EXPAND);
        PropertyDescription pdExampleList = d.documentDescription.propertyDescriptions
                .get("listOfExampleValues");
        pdExampleList.indexingOptions.add(PropertyIndexingOption.EXPAND);
        PropertyDescription pdStringList = d.documentDescription.propertyDescriptions
                .get("listOfStrings");
        pdStringList.indexingOptions.add(PropertyIndexingOption.EXPAND);
        PropertyDescription pdStringArray = d.documentDescription.propertyDescriptions
                .get("arrayOfStrings");
        pdStringArray.indexingOptions.add(PropertyIndexingOption.EXPAND);
        final String[] mapFields = { "mapOfStrings", "mapOfLongs", "mapOfDoubles", "mapOfBooleans",
                "mapOfUris",
                "mapOfEnums", "mapOfNestedTypes", "mapOfBytesArrays" };
        for (String mapField : mapFields) {
            PropertyDescription pdMap = d.documentDescription.propertyDescriptions.get(mapField);
            pdMap.indexingOptions.add(PropertyIndexingOption.EXPAND);
        }
        PropertyDescription pdExampleArray = d.documentDescription.propertyDescriptions
                .get("arrayOfExampleValues");
        pdExampleArray.indexingOptions.add(PropertyIndexingOption.EXPAND);
        PropertyDescription pdNestedType = d.documentDescription.propertyDescriptions
                .get("nestedComplexValue");
        pdNestedType.indexingOptions.add(PropertyIndexingOption.EXPAND);
        PropertyDescription pdIgnored = d.documentDescription.propertyDescriptions
                .get("ignoredStringValue");
        pdIgnored.indexingOptions.add(PropertyIndexingOption.STORE_ONLY);

        return d;
    }
}
