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

package com.vmware.xenon.examples;

import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;

/**
 * An example factory service dynamically loaded by the DCP LoaderService. A dynamically loaded
 * factory service must have a class name ending with Factory or Service and define a public static
 * String FACTORY_LINK field.
 */
public class ExampleWithFactoryLinkService extends StatefulService {
    public static final String FACTORY_LINK = UriUtils.buildUriPath("loader", "examples", "bar",
            "factory");

    public static class ExampleSingletonServiceState extends ServiceDocument {
        public String name;
    }

    public ExampleWithFactoryLinkService() {
        super(ExampleSingletonServiceState.class);
    }
}
