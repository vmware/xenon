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

public class DefaultFactoryService extends FactoryService {

    public static Service create(Class<? extends Service> childServiceType,
            Class<? extends ServiceDocument> childServiceDocumentType) {
        DefaultFactoryService fs = new DefaultFactoryService(childServiceDocumentType) {
            @Override
            public Service createServiceInstance() throws Throwable {
                return childServiceType.newInstance();
            }
        };
        return fs;
    }

    public DefaultFactoryService(Class<? extends ServiceDocument> childServiceDocumentType) {
        super(childServiceDocumentType);
    }

    /**
     * This method is not used
     */
    @Override
    public Service createServiceInstance() throws Throwable {
        return null;
    }
}