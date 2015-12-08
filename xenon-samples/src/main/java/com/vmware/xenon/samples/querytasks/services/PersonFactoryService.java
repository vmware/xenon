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

package com.vmware.xenon.samples.querytasks.services;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.reflect.TypeToken;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Service;

public class PersonFactoryService extends FactoryService{

    public static final Type COLLECTION_TYPE = new TypeToken<List<PersonService.PersonState>>(){}.getType();
    public static final String SEED_FILE = "/people.json";

    public static final String SELF_LINK = "/people";

    public PersonFactoryService() {
        super(PersonService.PersonState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new PersonService();
    }



}
