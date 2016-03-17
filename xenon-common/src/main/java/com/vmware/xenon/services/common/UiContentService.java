/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;

/**
 * Use this class to to host your custom ui at any uri. Subclasses should define
 * a SELF_LINK static field for the prefered uri. This service behavea like a web server
 * serving static files from a directory.
 */
public abstract class UiContentService extends StatelessService {
    public UiContentService() {
        toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
        toggleOption(ServiceOption.HTML_USER_INTERFACE, true);
    }

    @Override
    public void handleGet(Operation get) {
        super.handleUiGet(get);
    }
}
