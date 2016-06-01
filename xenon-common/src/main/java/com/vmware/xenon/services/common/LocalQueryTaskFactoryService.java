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

import com.vmware.xenon.common.Service;

public class LocalQueryTaskFactoryService extends QueryTaskFactoryService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_LOCAL_QUERY_TASKS;

    public LocalQueryTaskFactoryService() {
        super();
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        QueryTaskService service = new QueryTaskService();
        service.toggleOption(ServiceOption.REPLICATION, false);
        service.toggleOption(ServiceOption.OWNER_SELECTION, false);
        super.toggleOption(ServiceOption.REPLICATION, false);
        super.toggleOption(ServiceOption.OWNER_SELECTION, false);
        return service;
    }
}